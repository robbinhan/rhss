use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use clap::Parser;
use rhss::fs::FileSystem;
use rhss::storage::{LocalStorage, HybridStorage};
use rhss::fuse::{FuseAdapter, FuseConfig};
use tracing_subscriber::{fmt, EnvFilter};
use tracing::{info, error};
use tokio::signal;
use std::os::unix::fs::MetadataExt;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
#[command(disable_help_flag = true)]
struct Args {
    /// 挂载点路径
    #[arg(short, long)]
    mount: PathBuf,

    /// 热存储路径
    #[arg(short = 'H', long)]
    hot: PathBuf,

    /// 冷存储路径
    #[arg(short = 'C', long)]
    cold: PathBuf,

    /// 阈值（字节）
    #[arg(short, long, default_value = "1048576")]
    threshold: u64,

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

    let args = Args::parse();
    let mount_point = Arc::new(args.mount.clone());
    info!("初始化文件系统，挂载点={:?}, 热存储={:?}, 冷存储={:?}, 阈值={}", 
          args.mount, args.hot, args.cold, args.threshold);

    // 创建存储系统
    let hot_storage = Box::new(LocalStorage::new(args.hot.clone()));
    let cold_storage = Box::new(LocalStorage::new(args.cold.clone()));
    let fs = Box::new(HybridStorage::new(
        hot_storage,
        cold_storage,
        args.threshold,
    ));

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
    let adapter = Arc::new(FuseAdapter::new(fs, config));
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
    
    // 检查挂载点权限
    let mode = metadata.mode();
    if (mode & 0o400) == 0 || (mode & 0o200) == 0 {
        error!("挂载点目录权限不足: {:?}", mount_point_for_mount);
        std::process::exit(1);
    }
    
    // 在单独的线程中运行挂载
    let mount_handle = std::thread::spawn(move || {
        info!("正在挂载文件系统到 {:?}", mount_point_for_mount);
        match adapter.as_ref().mount(mount_point_for_mount.as_path()) {
            Ok(_) => {
                info!("文件系统挂载成功");
                // 保持线程运行
                loop {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
            Err(e) => {
                error!("挂载文件系统失败: {}", e);
                std::process::exit(1);
            }
        }
    });

    // 等待中断信号
    info!("等待中断信号...");
    signal::ctrl_c().await.expect("无法监听 Ctrl+C 信号");
    info!("接收到中断信号，准备卸载文件系统...");

    // 停止 FUSE 文件系统
    adapter_for_signal.stop();

    // 等待一小段时间，让文件系统完全停止
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // 尝试卸载
    let mut unmounted = false;
    let mut retry_count = 0;
    const MAX_RETRIES: u32 = 5;

    while !unmounted && retry_count < MAX_RETRIES {
        info!("尝试卸载文件系统...");
        if let Err(e) = unmount_fuse(&mount_point) {
            error!("普通卸载失败，尝试强制卸载: {}", e);
            if let Err(e) = force_unmount_fuse(&mount_point) {
                error!("强制卸载也失败了: {}", e);
                retry_count += 1;
                if retry_count < MAX_RETRIES {
                    info!("等待 1 秒后重试...");
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            } else {
                info!("文件系统已强制卸载");
                unmounted = true;
            }
        } else {
            info!("文件系统已正常卸载");
            unmounted = true;
        }
    }

    if !unmounted {
        error!("无法卸载文件系统，请手动卸载");
        std::process::exit(1);
    }

    // 等待挂载线程结束
    mount_handle.join().unwrap();
}

fn unmount_fuse(mount_point: &PathBuf) -> std::io::Result<()> {
    use std::process::Command;
    
    #[cfg(target_os = "macos")]
    {
        // 获取当前工作目录
        let current_dir = std::env::current_dir()?;
        let full_path = current_dir.join(mount_point);
        let output = Command::new("umount")
            .arg(full_path.to_str().unwrap())
            .output()?;
        
        if !output.status.success() {
            let error_msg = String::from_utf8_lossy(&output.stderr);
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("umount failed: {}", error_msg)
            ));
        }
    }
    
    #[cfg(target_os = "linux")]
    {
        let output = Command::new("fusermount")
            .arg("-u")
            .arg(mount_point)
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

fn force_unmount_fuse(mount_point: &PathBuf) -> std::io::Result<()> {
    use std::process::Command;
    
    #[cfg(target_os = "macos")]
    {
        // 获取当前工作目录
        let current_dir = std::env::current_dir()?;
        let full_path = current_dir.join(mount_point);
        let output = Command::new("diskutil")
            .arg("unmount")
            .arg("force")
            .arg(full_path.to_str().unwrap())
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
        let output = Command::new("fusermount")
            .arg("-uz")
            .arg(mount_point)
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