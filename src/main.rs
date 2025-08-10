use std::path::PathBuf;
use std::sync::Arc;
use std::os::unix::fs::PermissionsExt;
use clap::{Parser, ValueEnum};
use rhss::fs::FileSystem;
use rhss::storage::{LocalStorage, HybridStorage, PosixStorage};
use rhss::fuse::{FuseAdapter, FuseConfig};
use rhss::lock::StorageLock;
use tracing_subscriber::{fmt, EnvFilter};
use tracing::{info, error, warn};
use tokio::signal;
use std::os::unix::fs::MetadataExt;
use rustix::process::{getuid, getgid};
use rustix::fs::Mode;

#[derive(ValueEnum, Clone, Debug, PartialEq)]
enum StorageMode {
    Tokio,
    Rustix,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
#[command(disable_help_flag = true)]
struct Args {
    /// 挂载点路径 (必需)
    #[arg(short, long, required = true)]
    mount: PathBuf,

    /// 热存储路径
    #[arg(short = 'H', long, required = true)]
    hot: PathBuf,

    /// 冷存储路径
    #[arg(short = 'C', long, required = true)]
    cold: PathBuf,

    /// 阈值（字节）
    #[arg(short, long, default_value = "1048576")]
    threshold: u64,

    /// 存储后端模式
    #[arg(long, value_enum, default_value_t = StorageMode::Tokio)]
    mode: StorageMode,

    /// 强制启动（忽略现有锁）
    #[arg(long, default_value = "false")]
    force: bool,

    /// 使用隐藏存储模式（将存储移到系统临时目录）
    #[arg(long, default_value = "false")]
    hidden_storage: bool,

    /// 显示帮助信息
    #[arg(long, action = clap::ArgAction::Help)]
    help: Option<bool>,
}

#[tokio::main]
async fn main() {
    // 初始化日志系统
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_ansi(false)
        .init();

    let mut args = Args::parse();

    let mount_point = Arc::new(args.mount.clone());
    
    // 如果启用隐藏存储模式，重新设置存储路径
    let (actual_hot_path, actual_cold_path, original_paths) = if args.hidden_storage {
        // 保存原始路径用于显示
        let original_hot = args.hot.clone();
        let original_cold = args.cold.clone();
        
        // 创建隐藏的存储目录
        let hidden_base = std::env::temp_dir().join(format!(".rhss_{}", std::process::id()));
        let hidden_hot = hidden_base.join("hot");
        let hidden_cold = hidden_base.join("cold");
        
        // 创建隐藏目录
        std::fs::create_dir_all(&hidden_hot).expect("无法创建隐藏热存储目录");
        std::fs::create_dir_all(&hidden_cold).expect("无法创建隐藏冷存储目录");
        
        // 如果原始目录存在且有内容，复制到隐藏目录
        if original_hot.exists() {
            info!("迁移热存储内容到隐藏位置: {:?} -> {:?}", original_hot, hidden_hot);
            copy_dir_contents(&original_hot, &hidden_hot).expect("无法迁移热存储内容");
        }
        if original_cold.exists() {
            info!("迁移冷存储内容到隐藏位置: {:?} -> {:?}", original_cold, hidden_cold);
            copy_dir_contents(&original_cold, &hidden_cold).expect("无法迁移冷存储内容");
        }
        
        // 更新 args 中的路径
        args.hot = hidden_hot.clone();
        args.cold = hidden_cold.clone();
        
        info!("隐藏存储模式已启用");
        info!("  原始热存储: {:?}", original_hot);
        info!("  原始冷存储: {:?}", original_cold);
        info!("  隐藏热存储: {:?}", hidden_hot);
        info!("  隐藏冷存储: {:?}", hidden_cold);
        
        // 设置原始目录为只读，防止直接访问
        if let Ok(metadata) = std::fs::metadata(&original_hot) {
            let mut perms = metadata.permissions();
            perms.set_mode(0o500); // 只读
            let _ = std::fs::set_permissions(&original_hot, perms);
            info!("已将原始热存储设为只读");
        }
        if let Ok(metadata) = std::fs::metadata(&original_cold) {
            let mut perms = metadata.permissions();
            perms.set_mode(0o500); // 只读
            let _ = std::fs::set_permissions(&original_cold, perms);
            info!("已将原始冷存储设为只读");
        }
        
        (hidden_hot, hidden_cold, Some((original_hot, original_cold)))
    } else {
        (args.hot.clone(), args.cold.clone(), None)
    };

    info!("初始化文件系统，模式={:?}, 热存储={:?}, 冷存储={:?}, 阈值={}, 挂载点={:?}",
          args.mode, actual_hot_path, actual_cold_path, args.threshold, mount_point);

    // 创建存储锁（使用 Arc 以便在多个地方使用）
    let storage_lock = Arc::new(std::sync::Mutex::new(StorageLock::new(&actual_hot_path, &actual_cold_path)));
    
    // 尝试获取锁
    {
        let mut lock = storage_lock.lock().unwrap();
        if args.force {
            info!("强制模式：清理现有锁并获取新锁");
            if let Err(e) = lock.force_lock() {
                error!("无法获取存储锁: {}", e);
                std::process::exit(1);
            }
        } else {
            if let Err(e) = lock.try_lock() {
                error!("{}", e);
                error!("提示：如果确定没有其他实例在运行，可以使用 --force 参数强制启动");
                std::process::exit(1);
            }
        }
    }
    
    info!("已成功获取存储锁");

    // --- 根据模式创建存储实例 ---
    let hot_storage: Box<dyn FileSystem>;
    let cold_storage: Box<dyn FileSystem>;

    match args.mode {
        StorageMode::Tokio => {
            info!("使用 LocalStorage (tokio::fs) 作为后端");
            hot_storage = Box::new(LocalStorage::new(args.hot.clone()));
            cold_storage = Box::new(LocalStorage::new(args.cold.clone()));
        }
        StorageMode::Rustix => {
            let uid = getuid();
            let gid = getgid();
            let mode = Mode::from(0o644);
            info!("使用 PosixStorage (rustix) 作为后端，uid={}, gid={}, 默认内部模式={:o}", uid.as_raw(), gid.as_raw(), mode.bits());
            hot_storage = Box::new(PosixStorage::new(args.hot.clone(), uid, gid, mode));
            cold_storage = Box::new(PosixStorage::new(args.cold.clone(), uid, gid, mode));
        }
    }
    // -----------------------------------------

    let fs = Box::new(HybridStorage::new(
        hot_storage,
        cold_storage,
        args.threshold,
    ));

    // --- FUSE 逻辑现在对所有模式都执行 ---
    info!("准备通过 FUSE 挂载到 {:?}", mount_point);

    // 创建 FUSE 配置
    let config = FuseConfig::new()
        .with_ignore_paths(vec![
            ".DS_Store".to_string(),
            ".hidden".to_string(),
            ".git".to_string(),
            "@executable_path".to_string(),
        ])
        .with_ignore_patterns(vec![
            "._*".to_string(),  // macOS 元数据文件
        ]);

    // 创建并挂载 FUSE 文件系统
    let runtime_handle = tokio::runtime::Handle::current();
    let adapter = Arc::new(FuseAdapter::new(fs, config, runtime_handle));
    let mount_point_for_mount = mount_point.clone();
    let adapter_for_signal = adapter.clone();

    // 确保挂载点目录存在
    if !mount_point_for_mount.as_path().exists() {
        info!("创建挂载点目录: {:?}", mount_point_for_mount);
        if let Err(e) = std::fs::create_dir_all(mount_point_for_mount.as_path()) {
            error!("创建挂载点目录失败: {}", e);
            std::process::exit(1);
        }
    }

    // 检查挂载点权限
    let metadata = match std::fs::metadata(mount_point_for_mount.as_path()) {
        Ok(m) => m,
        Err(e) => {
            error!("获取挂载点元数据失败: {}", e);
            std::process::exit(1);
        }
    };

    let mode = metadata.mode();
    if (mode & 0o400) == 0 || (mode & 0o200) == 0 {
        warn!("挂载点目录权限可能不足 (需要读写权限): {:?}, mode={:o}", mount_point_for_mount, mode);
    }

    // 在单独的线程中运行挂载
    let mount_handle = std::thread::spawn(move || {
        info!("正在挂载文件系统到 {:?}", mount_point_for_mount);
        match adapter.as_ref().mount(mount_point_for_mount.as_path()) {
            Ok(_) => {
                info!("文件系统挂载成功");
                // mount 函数会阻塞直到文件系统被卸载
                info!("文件系统已卸载，挂载线程退出");
            }
            Err(e) => {
                error!("挂载文件系统失败: {}", e);
                std::process::exit(1);
            }
        }
    });

    // 克隆 storage_lock 用于信号处理
    let storage_lock_for_signal = Arc::clone(&storage_lock);
    
    // 设置信号处理
    info!("文件系统已就绪，按 Ctrl+C 安全退出...");
    
    // 同时监听多个信号
    #[cfg(unix)]
    {
        let ctrl_c = signal::ctrl_c();
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("无法监听 SIGTERM 信号");
        let mut sighup = signal::unix::signal(signal::unix::SignalKind::hangup())
            .expect("无法监听 SIGHUP 信号");
        
        // 等待任意一个信号
        tokio::select! {
            _ = ctrl_c => {
                info!("接收到 Ctrl+C 信号，准备安全退出...");
            }
            _ = sigterm.recv() => {
                info!("接收到 SIGTERM 信号，准备安全退出...");
            }
            _ = sighup.recv() => {
                info!("接收到 SIGHUP 信号，准备安全退出...");
            }
        }
    }
    
    #[cfg(not(unix))]
    {
        signal::ctrl_c().await.expect("无法监听 Ctrl+C 信号");
        info!("接收到 Ctrl+C 信号，准备安全退出...");
    }

    // 开始安全退出流程
    info!("开始安全退出流程...");
    
    // 1. 先释放存储锁（防止其他实例认为锁还在）
    {
        let mut lock = storage_lock_for_signal.lock().unwrap();
        if let Err(e) = lock.unlock() {
            warn!("提前释放存储锁失败: {}", e);
        } else {
            info!("已提前释放存储锁");
        }
    }
    
    // 2. 停止接受新的 FUSE 请求
    adapter_for_signal.stop();
    
    // 2. 先尝试正常卸载，这会让 mount 函数返回
    info!("尝试卸载文件系统以触发挂载线程退出...");
    let unmount_result = unmount_fuse(&mount_point);
    if unmount_result.is_ok() {
        info!("卸载命令执行成功，等待挂载线程退出...");
    } else {
        warn!("初次卸载尝试失败，稍后会重试");
    }
    
    // 3. 等待挂载线程退出（设置超时避免无限等待）
    let mount_thread_timeout = std::time::Duration::from_secs(5);
    let mount_thread_start = std::time::Instant::now();
    
    loop {
        if mount_handle.is_finished() {
            mount_handle.join().expect("挂载线程 panic");
            info!("挂载线程已正常结束");
            break;
        }
        
        if mount_thread_start.elapsed() > mount_thread_timeout {
            warn!("等待挂载线程退出超时（{}秒）", mount_thread_timeout.as_secs());
            // 尝试强制卸载
            info!("尝试强制卸载...");
            let _ = force_unmount_fuse(&mount_point);
            std::thread::sleep(std::time::Duration::from_millis(500));
            
            // 再等待一下
            if mount_handle.is_finished() {
                mount_handle.join().expect("挂载线程 panic");
                info!("挂载线程在强制卸载后结束");
            } else {
                error!("挂载线程仍未退出，继续清理流程");
            }
            break;
        }
        
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // 4. 检查最终的挂载状态
    let mut unmounted = false;
    
    // 等待一下让卸载完全生效
    std::thread::sleep(std::time::Duration::from_millis(500));
    
    // 检查挂载点是否还存在
    info!("检查文件系统挂载状态...");
    for retry in 0..3 {
        let check_mount = std::process::Command::new("mount")
            .output();
        
        if let Ok(output) = check_mount {
            let mount_output = String::from_utf8_lossy(&output.stdout);
            if !mount_output.contains(mount_point.to_str().unwrap_or("")) {
                info!("文件系统已成功卸载");
                unmounted = true;
                break;
            } else if retry < 2 {
                // 还有重试机会
                info!("文件系统仍然挂载，等待 {} 秒后重新检查...", retry + 1);
                std::thread::sleep(std::time::Duration::from_secs(1));
                
                // 再尝试一次卸载
                let _ = force_unmount_fuse(&mount_point);
            }
        } else {
            // 无法检查挂载状态，假设已卸载
            warn!("无法检查挂载状态，假设已卸载");
            unmounted = true;
            break;
        }
    }
    
    if !unmounted {
        // 最后的最后，再检查一次
        std::thread::sleep(std::time::Duration::from_millis(500));
        if let Ok(output) = std::process::Command::new("mount").output() {
            let mount_output = String::from_utf8_lossy(&output.stdout);
            if !mount_output.contains(mount_point.to_str().unwrap_or("")) {
                info!("文件系统最终已成功卸载");
                unmounted = true;
            }
        }
    }

    // 3. 最终清理和退出
    let exit_code = if !unmounted {
        error!("无法自动卸载文件系统，请手动卸载: {:?}", mount_point);
        error!("您可能需要手动运行以下命令：");
        #[cfg(target_os = "macos")]
        error!("  sudo diskutil unmount force {:?}", mount_point);
        #[cfg(target_os = "linux")]
        error!("  sudo fusermount -uz {:?}", mount_point);
        
        // 退出码 1 表示卸载失败
        1
    } else {
        info!("文件系统已成功卸载");
        info!("=== 安全退出完成 ===");
        
        // 退出码 0 表示正常退出
        0
    };
    
    // 再次尝试释放存储锁（如果之前失败了）
    {
        let mut lock = storage_lock.lock().unwrap();
        if lock.is_locked() {
            info!("最终释放存储锁...");
            if let Err(e) = lock.unlock() {
                error!("最终释放存储锁失败: {}", e);
            }
        }
    }
    
    // 如果使用隐藏存储模式，同步内容并清理
    if let Some((original_hot, original_cold)) = original_paths {
        info!("隐藏存储模式：准备同步和清理...");
        
        // 先恢复原始目录权限
        if let Ok(metadata) = std::fs::metadata(&original_hot) {
            let mut perms = metadata.permissions();
            perms.set_mode(0o755); // 恢复为正常权限
            let _ = std::fs::set_permissions(&original_hot, perms);
            info!("已恢复原始热存储权限");
        }
        if let Ok(metadata) = std::fs::metadata(&original_cold) {
            let mut perms = metadata.permissions();
            perms.set_mode(0o755); // 恢复为正常权限
            let _ = std::fs::set_permissions(&original_cold, perms);
            info!("已恢复原始冷存储权限");
        }
        
        // 同步内容回原始位置
        if let Err(e) = sync_hidden_storage_back(&actual_hot_path, &actual_cold_path, 
                                                 &original_hot, &original_cold) {
            error!("同步隐藏存储失败: {}", e);
        }
        
        // 清理隐藏目录
        if let Some(parent) = actual_hot_path.parent() {
            info!("清理隐藏存储目录: {:?}", parent);
            if let Err(e) = std::fs::remove_dir_all(parent) {
                warn!("无法清理隐藏存储目录: {}", e);
            }
        }
    }
    
    // 确保所有日志都被刷新
    std::thread::sleep(std::time::Duration::from_millis(100));
    
    std::process::exit(exit_code);
}

fn unmount_fuse(mount_point: &Arc<PathBuf>) -> std::io::Result<()> {
    use std::process::Command;

    #[cfg(target_os = "macos")]
    {
        // 在 macOS 上，先尝试 diskutil unmount（更可靠）
        info!("执行 diskutil unmount {:?}", mount_point);
        let output = Command::new("diskutil")
            .arg("unmount")
            .arg(mount_point.as_os_str())
            .output()?;

        if !output.status.success() {
            // 如果 diskutil 失败，尝试 umount
            info!("diskutil 失败，尝试 umount {:?}", mount_point);
            let output = Command::new("umount")
                .arg(mount_point.as_os_str())
                .output()?;
            
            if !output.status.success() {
                let error_msg = String::from_utf8_lossy(&output.stderr);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("umount failed: {}", error_msg)
                ));
            }
        }
    }

    #[cfg(target_os = "linux")]
    {
        info!("执行 fusermount -u {:?}", mount_point);
        let output = Command::new("fusermount")
            .arg("-u")
            .arg(mount_point.as_os_str())
            .output()?;

        if !output.status.success() {
            let error_msg = String::from_utf8_lossy(&output.stderr);
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("fusermount failed: {}", error_msg)
            ));
        }
    }

    Ok(())
}

/// 递归复制目录内容
fn copy_dir_contents(src: &PathBuf, dst: &PathBuf) -> std::io::Result<()> {
    use std::fs;
    
    // 确保目标目录存在
    fs::create_dir_all(dst)?;
    
    // 遍历源目录
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let file_name = entry.file_name();
        let dst_path = dst.join(&file_name);
        
        if src_path.is_dir() {
            // 递归复制子目录
            copy_dir_contents(&src_path, &dst_path)?;
        } else {
            // 复制文件
            fs::copy(&src_path, &dst_path)?;
        }
    }
    
    Ok(())
}

/// 同步隐藏存储内容回原始位置
fn sync_hidden_storage_back(hidden_hot: &PathBuf, hidden_cold: &PathBuf, 
                           original_hot: &PathBuf, original_cold: &PathBuf) -> std::io::Result<()> {
    use std::fs;
    
    info!("同步隐藏存储内容回原始位置...");
    
    // 清空原始目录
    if original_hot.exists() {
        fs::remove_dir_all(original_hot)?;
    }
    if original_cold.exists() {
        fs::remove_dir_all(original_cold)?;
    }
    
    // 复制内容回原始位置
    copy_dir_contents(hidden_hot, original_hot)?;
    copy_dir_contents(hidden_cold, original_cold)?;
    
    info!("已同步热存储: {:?} -> {:?}", hidden_hot, original_hot);
    info!("已同步冷存储: {:?} -> {:?}", hidden_cold, original_cold);
    
    Ok(())
}

fn force_unmount_fuse(mount_point: &Arc<PathBuf>) -> std::io::Result<()> {
    use std::process::Command;

    #[cfg(target_os = "macos")]
    {
        info!("执行 diskutil unmount force {:?}", mount_point);
        let output = Command::new("diskutil")
            .arg("unmount")
            .arg("force")
            .arg(mount_point.as_os_str())
            .output()?;

        if !output.status.success() {
            let error_msg = String::from_utf8_lossy(&output.stderr);
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("diskutil unmount failed: {}", error_msg)
            ));
        }
    }

    #[cfg(target_os = "linux")]
    {
        info!("执行 fusermount -uz {:?}", mount_point);
        let output = Command::new("fusermount")
            .arg("-uz")
            .arg(mount_point.as_os_str())
            .output()?;

        if !output.status.success() {
            let error_msg = String::from_utf8_lossy(&output.stderr);
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("fusermount force unmount failed: {}", error_msg)
            ));
        }
    }

    Ok(())
} 