use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use std::fs;
use std::io::Write;
use clap::{Parser, ValueEnum};
use rhss::fs::FileSystem;
use rhss::storage::{LocalStorage, HybridStorage, PosixStorage};
use rustix::process::{getuid, getgid};
use rustix::fs::Mode;
use tokio::runtime::Runtime;

#[derive(ValueEnum, Clone, Debug, PartialEq)]
enum StorageMode {
    Tokio,
    Rustix,
    Both,
}

#[derive(Parser, Debug)]
#[command(author, version, about = "RHSS Benchmark Tool", long_about = None)]
struct Args {
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
    #[arg(long, value_enum, default_value_t = StorageMode::Both)]
    mode: StorageMode,

    /// 测试文件数量
    #[arg(short, long, default_value = "100")]
    num_files: usize,

    /// 小文件大小（字节）
    #[arg(long, default_value = "1024")]
    small_size: usize,

    /// 大文件大小（字节）
    #[arg(long, default_value = "10485760")]
    large_size: usize,
}

struct BenchmarkResult {
    operation: String,
    duration: Duration,
    ops_per_sec: f64,
    mb_per_sec: Option<f64>,
}

impl BenchmarkResult {
    fn new(operation: &str, duration: Duration, ops: usize, bytes: Option<usize>) -> Self {
        let ops_per_sec = ops as f64 / duration.as_secs_f64();
        let mb_per_sec = bytes.map(|b| (b as f64 / 1_048_576.0) / duration.as_secs_f64());
        
        Self {
            operation: operation.to_string(),
            duration,
            ops_per_sec,
            mb_per_sec,
        }
    }

    fn print(&self) {
        print!("  {:<30} {:>10.3} ms", self.operation, self.duration.as_secs_f64() * 1000.0);
        print!(" | {:>8.1} ops/s", self.ops_per_sec);
        if let Some(mb_per_sec) = self.mb_per_sec {
            print!(" | {:>8.2} MB/s", mb_per_sec);
        }
        println!();
    }
}

async fn benchmark_storage(fs: &dyn FileSystem, args: &Args, test_name: &str) -> Vec<BenchmarkResult> {
    let mut results = Vec::new();
    println!("\n=== {} ===", test_name);
    
    // 准备测试数据
    let small_data = vec![b'a'; args.small_size];
    let large_data = vec![b'b'; args.large_size];
    
    // 使用相对路径，不要以斜杠开头
    let test_dir = Path::new("benchmark_test");
    let _ = fs.delete(test_dir).await;
    
    // 先创建父目录（如果需要）
    if let Err(e) = fs.create_directory(test_dir).await {
        println!("警告：创建测试目录失败: {:?}，尝试继续...", e);
    }
    
    // 1. 小文件写入测试
    println!("\n测试小文件写入 ({} 个文件, 每个 {} 字节)...", args.num_files, args.small_size);
    let start = Instant::now();
    for i in 0..args.num_files {
        let path = test_dir.join(format!("small_{}.txt", i));
        fs.write_file(&path, &small_data).await.expect("写入小文件失败");
    }
    let duration = start.elapsed();
    results.push(BenchmarkResult::new(
        "小文件写入",
        duration,
        args.num_files,
        Some(args.num_files * args.small_size),
    ));
    
    // 2. 小文件读取测试
    println!("测试小文件读取...");
    let start = Instant::now();
    for i in 0..args.num_files {
        let path = test_dir.join(format!("small_{}.txt", i));
        let _ = fs.read_file(&path).await.expect("读取小文件失败");
    }
    let duration = start.elapsed();
    results.push(BenchmarkResult::new(
        "小文件读取",
        duration,
        args.num_files,
        Some(args.num_files * args.small_size),
    ));
    
    // 3. 大文件写入测试
    let large_count = 10.min(args.num_files);
    println!("\n测试大文件写入 ({} 个文件, 每个 {} 字节)...", large_count, args.large_size);
    let start = Instant::now();
    for i in 0..large_count {
        let path = test_dir.join(format!("large_{}.txt", i));
        fs.write_file(&path, &large_data).await.expect("写入大文件失败");
    }
    let duration = start.elapsed();
    results.push(BenchmarkResult::new(
        "大文件写入",
        duration,
        large_count,
        Some(large_count * args.large_size),
    ));
    
    // 4. 大文件读取测试
    println!("测试大文件读取...");
    let start = Instant::now();
    for i in 0..large_count {
        let path = test_dir.join(format!("large_{}.txt", i));
        let _ = fs.read_file(&path).await.expect("读取大文件失败");
    }
    let duration = start.elapsed();
    results.push(BenchmarkResult::new(
        "大文件读取",
        duration,
        large_count,
        Some(large_count * args.large_size),
    ));
    
    // 5. 目录列表测试
    println!("\n测试目录列表...");
    let start = Instant::now();
    for _ in 0..100 {
        let _ = fs.list_directory(test_dir).await.expect("列出目录失败");
    }
    let duration = start.elapsed();
    results.push(BenchmarkResult::new(
        "目录列表（100次）",
        duration,
        100,
        None,
    ));
    
    // 6. 元数据获取测试
    println!("测试元数据获取...");
    let mut metadata_count = 0;
    let start = Instant::now();
    // 尝试获取仍然存在的文件的元数据
    for i in 0..large_count {
        let path = test_dir.join(format!("large_{}.txt", i));
        if fs.get_metadata(&path).await.is_ok() {
            metadata_count += 1;
        }
    }
    let duration = start.elapsed();
    if metadata_count > 0 {
        results.push(BenchmarkResult::new(
            "元数据获取",
            duration,
            metadata_count,
            None,
        ));
    }
    
    // 7. 文件删除测试 - 重新创建文件再删除
    println!("测试文件删除...");
    // 先创建要删除的文件
    for i in 0..args.num_files {
        let path = test_dir.join(format!("delete_{}.txt", i));
        if let Err(e) = fs.write_file(&path, &small_data).await {
            println!("创建待删除文件失败: {:?}", e);
            continue;
        }
    }
    
    let start = Instant::now();
    let mut delete_count = 0;
    for i in 0..args.num_files {
        let path = test_dir.join(format!("delete_{}.txt", i));
        if fs.delete(&path).await.is_ok() {
            delete_count += 1;
        }
    }
    let duration = start.elapsed();
    if delete_count > 0 {
        results.push(BenchmarkResult::new(
            "文件删除",
            duration,
            delete_count,
            None,
        ));
    }
    
    // 清理
    let _ = fs.delete(test_dir).await;
    
    results
}

fn print_results(results: &[BenchmarkResult]) {
    println!("\n📊 性能测试结果:");
    println!("  {:<30} {:>10} | {:>8} | {:>8}", "操作", "耗时", "吞吐量", "带宽");
    println!("  {}", "-".repeat(70));
    for result in results {
        result.print();
    }
}

fn compare_results(tokio_results: &[BenchmarkResult], rustix_results: &[BenchmarkResult]) {
    println!("\n📈 性能对比:");
    println!("  {:<30} {:>15} {:>15} {:>10}", "操作", "Tokio (ms)", "Rustix (ms)", "差异");
    println!("  {}", "-".repeat(75));
    
    for (tokio, rustix) in tokio_results.iter().zip(rustix_results.iter()) {
        let tokio_ms = tokio.duration.as_secs_f64() * 1000.0;
        let rustix_ms = rustix.duration.as_secs_f64() * 1000.0;
        let diff_percent = ((rustix_ms - tokio_ms) / tokio_ms * 100.0);
        
        print!("  {:<30}", tokio.operation);
        print!(" {:>15.3}", tokio_ms);
        print!(" {:>15.3}", rustix_ms);
        
        if diff_percent < -5.0 {
            print!(" \x1b[32m{:>9.1}%\x1b[0m", diff_percent);  // 绿色：Rustix 更快
        } else if diff_percent > 5.0 {
            print!(" \x1b[31m{:>9.1}%\x1b[0m", diff_percent);   // 红色：Tokio 更快
        } else {
            print!(" {:>9.1}%", diff_percent);                   // 普通：差异不大
        }
        println!();
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    
    println!("🚀 RHSS 性能基准测试");
    println!("配置：");
    println!("  热存储: {:?}", args.hot);
    println!("  冷存储: {:?}", args.cold);
    println!("  阈值: {} 字节", args.threshold);
    println!("  测试文件数: {}", args.num_files);
    println!("  小文件大小: {} 字节", args.small_size);
    println!("  大文件大小: {} 字节", args.large_size);
    
    let mut tokio_results = Vec::new();
    let mut rustix_results = Vec::new();
    
    // 测试 Tokio 存储
    if args.mode == StorageMode::Tokio || args.mode == StorageMode::Both {
        println!("\n\n🔧 测试 LocalStorage (tokio::fs) 后端...");
        let hot_storage = Box::new(LocalStorage::new(args.hot.join("tokio")));
        let cold_storage = Box::new(LocalStorage::new(args.cold.join("tokio")));
        let fs = HybridStorage::new(hot_storage, cold_storage, args.threshold);
        
        tokio_results = benchmark_storage(&fs, &args, "LocalStorage (Tokio)").await;
        print_results(&tokio_results);
    }
    
    // 测试 Rustix 存储
    if args.mode == StorageMode::Rustix || args.mode == StorageMode::Both {
        println!("\n\n🔧 测试 PosixStorage (rustix) 后端...");
        let uid = getuid();
        let gid = getgid();
        let mode = Mode::from(0o644);
        
        let hot_storage = Box::new(PosixStorage::new(args.hot.join("rustix"), uid, gid, mode));
        let cold_storage = Box::new(PosixStorage::new(args.cold.join("rustix"), uid, gid, mode));
        let fs = HybridStorage::new(hot_storage, cold_storage, args.threshold);
        
        rustix_results = benchmark_storage(&fs, &args, "PosixStorage (Rustix)").await;
        print_results(&rustix_results);
    }
    
    // 对比结果
    if args.mode == StorageMode::Both && !tokio_results.is_empty() && !rustix_results.is_empty() {
        compare_results(&tokio_results, &rustix_results);
    }
    
    println!("\n✅ 基准测试完成！");
}
