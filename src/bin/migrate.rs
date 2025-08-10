use clap::Parser;
use rhss::storage::{HybridStorage, LocalStorage};
use std::path::{Path, PathBuf};
use tracing::{info, error};
use tracing_subscriber;

#[derive(Parser, Debug)]
#[command(author, version, about = "文件迁移工具", long_about = None)]
struct Args {
    /// 热存储路径
    #[arg(short = 'H', long, default_value = "test/hot")]
    hot: PathBuf,

    /// 冷存储路径
    #[arg(short = 'C', long, default_value = "test/cold")]
    cold: PathBuf,

    /// 文件大小阈值（字节）
    #[arg(short = 't', long, default_value = "1048576")]
    threshold: u64,

    /// 要迁移的文件或目录路径（相对于存储根目录）
    #[arg(short = 'p', long)]
    path: Option<PathBuf>,

    /// 是否检查整个存储
    #[arg(short = 'a', long, default_value = "false")]
    all: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into())
        )
        .init();

    let args = Args::parse();

    info!("初始化迁移工具...");
    info!("热存储: {:?}", args.hot);
    info!("冷存储: {:?}", args.cold);
    info!("阈值: {} bytes", args.threshold);

    // 创建存储实例
    let hot_storage = Box::new(LocalStorage::new(args.hot.clone()));
    let cold_storage = Box::new(LocalStorage::new(args.cold.clone()));
    let hybrid_storage = HybridStorage::new(hot_storage, cold_storage, args.threshold);

    if args.all {
        // 迁移整个存储
        info!("开始检查整个存储...");
        
        let (checked, migrated) = hybrid_storage.migrate_directory(Path::new("")).await?;
        
        info!("迁移完成！");
        info!("检查文件数: {}", checked);
        info!("迁移文件数: {}", migrated);
        
        if migrated == 0 {
            info!("所有文件都已在正确的存储层");
        }
    } else if let Some(path) = args.path {
        // 迁移指定路径
        info!("检查路径: {:?}", path);
        
        // 判断是文件还是目录
        let is_dir = hybrid_storage.get_file_metadata(&path).await
            .map(|m| m.is_dir)
            .unwrap_or(false);
        
        if is_dir {
            info!("检查目录: {:?}", path);
            let (checked, migrated) = hybrid_storage.migrate_directory(&path).await?;
            info!("检查了 {} 个文件，迁移了 {} 个", checked, migrated);
        } else {
            info!("检查文件: {:?}", path);
            if hybrid_storage.migrate_file(&path).await? {
                info!("文件已迁移");
            } else {
                info!("文件已在正确的存储层");
            }
        }
    } else {
        error!("请指定要迁移的路径 (-p) 或使用 -a 检查整个存储");
        std::process::exit(1);
    }

    // 显示缓存统计
    info!("缓存统计: {}", hybrid_storage.cache_stats());

    Ok(())
}
