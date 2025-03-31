use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use clap::Parser;
use rhss::fs::FileSystem;
use rhss::storage::{LocalStorage, HybridStorage};
use rhss::fuse::{FuseAdapter, FuseConfig};
use tracing_subscriber::{fmt, EnvFilter};
use tracing::{info, error};

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

fn main() {
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

    // 创建 Tokio runtime
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();

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

    // 设置中断信号处理
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let mount_point_for_handler = mount_point.clone();
    ctrlc::set_handler(move || {
        info!("接收到中断信号，准备卸载文件系统...");
        if let Err(e) = unmount_fuse(&mount_point_for_handler) {
            error!("卸载文件系统失败: {}", e);
        }
        r.store(false, Ordering::SeqCst);
    }).expect("无法设置中断处理器");

    // 创建并挂载 FUSE 文件系统
    let adapter = FuseAdapter::new(fs, config);
    
    // 在单独的线程中运行挂载
    let mount_point_for_mount = mount_point.clone();
    std::thread::spawn(move || {
        info!("正在挂载文件系统到 {:?}", mount_point_for_mount);
        if let Err(e) = adapter.mount(&mount_point_for_mount) {
            error!("挂载文件系统失败: {}", e);
        }
    });

    // 等待中断信号
    while running.load(Ordering::SeqCst) {
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // 卸载文件系统
    info!("正在卸载文件系统...");
    if let Err(e) = unmount_fuse(&mount_point) {
        error!("卸载文件系统失败: {}", e);
    }
    info!("文件系统已卸载");
}

fn unmount_fuse(mount_point: &PathBuf) -> std::io::Result<()> {
    use std::process::Command;
    
    #[cfg(target_os = "macos")]
    {
        Command::new("umount")
            .arg(mount_point)
            .status()?;
    }
    
    #[cfg(target_os = "linux")]
    {
        Command::new("fusermount")
            .arg("-u")
            .arg(mount_point)
            .status()?;
    }
    
    Ok(())
} 