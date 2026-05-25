//! rhss CLI — P1.
//!
//! Reads a TOML config (multi-backend per tier), opens the SQLite path index,
//! runs the first-scan if the index is empty, starts the access tracker,
//! mounts FUSE in a background session, waits for SIGINT/SIGTERM/SIGHUP.

use std::path::PathBuf;
use std::process::Command;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use clap::Parser;
use rhss::access::AccessTracker;
use rhss::backend::Backend;
use rhss::config::RhssConfig;
use rhss::fuse::FuseConfig;
use rhss::index::{PathIndex, SqlitePathIndex, TierId};
use rhss::lock::StorageLock;
use rhss::policy::{PopularityPolicy, TieringPolicy};
use rhss::scan;
use rhss::tier::{MostFreePlacement, Tier, TierRouter};
use rhss::tierer::{OpenFileTracker, Tierer};
use rhss::{FuseAdapter, PosixBackend};
use tracing::{error, info, warn};
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the TOML config file.
    #[arg(short, long)]
    config: PathBuf,

    /// Force startup even if a stale storage lock exists.
    #[arg(long, default_value_t = false)]
    force: bool,
}

fn main() {
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .with_ansi(true)
        .init();

    let args = Args::parse();
    let cfg = match RhssConfig::load(&args.config) {
        Ok(c) => c,
        Err(e) => {
            error!("load config {}: {e}", args.config.display());
            std::process::exit(2);
        }
    };

    if let Err(e) = std::fs::create_dir_all(&cfg.mount) {
        error!("create mount point {}: {e}", cfg.mount.display());
        std::process::exit(1);
    }
    if let Some(parent) = cfg.db.parent() {
        if !parent.as_os_str().is_empty() {
            let _ = std::fs::create_dir_all(parent);
        }
    }

    // The lock is just a coarse "only one rhss process at a time" gate. Use
    // the db file's parent as both hot and cold paths for the lock — same
    // physical location.
    let lock_dir = cfg.db.parent().map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
    let lock = Arc::new(std::sync::Mutex::new(StorageLock::new(&lock_dir, &lock_dir)));
    {
        let mut g = lock.lock().unwrap();
        let res = if args.force { g.force_lock() } else { g.try_lock() };
        if let Err(e) = res {
            error!("acquire storage lock: {e}");
            std::process::exit(1);
        }
    }
    info!("acquired storage lock");

    // Ensure every backend root exists.
    let all_roots: Vec<&std::path::Path> = cfg
        .tier
        .fast
        .iter()
        .chain(cfg.tier.slow.iter())
        .map(|b| b.root.as_path())
        .collect();
    if let Err(e) = scan::ensure_managed_dirs(all_roots.iter().copied()) {
        error!("prepare backend dirs: {e}");
        std::process::exit(1);
    }

    // Build router.
    let make_backend = |id: &str, root: &std::path::Path| -> Arc<dyn Backend> {
        Arc::new(PosixBackend::new(id, root.to_path_buf()).expect("backend init"))
    };
    let fast_backends: Vec<Arc<dyn Backend>> = cfg
        .tier
        .fast
        .iter()
        .map(|b| make_backend(&b.id, &b.root))
        .collect();
    let slow_backends: Vec<Arc<dyn Backend>> = cfg
        .tier
        .slow
        .iter()
        .map(|b| make_backend(&b.id, &b.root))
        .collect();

    let fast = Tier::new(TierId::Fast, fast_backends, Box::new(MostFreePlacement)).expect("fast tier");
    let slow = Tier::new(TierId::Slow, slow_backends, Box::new(MostFreePlacement)).expect("slow tier");
    let router = Arc::new(TierRouter::new(fast, slow));

    // Open index.
    let index: Arc<dyn PathIndex> = match SqlitePathIndex::open(&cfg.db) {
        Ok(i) => i,
        Err(e) => {
            error!("open index {}: {e}", cfg.db.display());
            std::process::exit(1);
        }
    };

    // First-scan if empty. Conflicts → hard fail (D13).
    if index.count().unwrap_or(0) == 0 {
        info!("path index is empty, running first scan");
    }
    match scan::first_scan(&router, &index) {
        Ok(stats) => {
            if !stats.conflicts.is_empty() {
                error!(
                    count = stats.conflicts.len(),
                    "first-scan hard-fail: cross-backend logical-path conflicts; aborting"
                );
                for p in stats.conflicts.iter().take(20) {
                    error!("  conflict: {}", p.display());
                }
                std::process::exit(1);
            }
        }
        Err(e) => {
            error!("first scan: {e}");
            std::process::exit(1);
        }
    }

    let access = AccessTracker::start(Arc::clone(&index), Duration::from_secs(5));
    let open_tracker = Arc::new(OpenFileTracker::new());
    let policy: Arc<dyn TieringPolicy> = Arc::new(PopularityPolicy::default());

    let (_tierer, tierer_handle) = Tierer::spawn(
        Arc::clone(&router),
        Arc::clone(&index),
        Arc::clone(&open_tracker),
        Arc::clone(&policy),
    );
    info!("background tierer started");

    let adapter = FuseAdapter::new(
        Arc::clone(&router),
        Arc::clone(&index),
        Arc::clone(&policy),
        Arc::clone(&open_tracker),
        Some(tierer_handle),
        Some(access),
        FuseConfig::default(),
    );

    let session = match adapter.spawn_mount(&cfg.mount) {
        Ok(s) => s,
        Err(e) => {
            error!("mount {}: {e}", cfg.mount.display());
            std::process::exit(1);
        }
    };
    info!("rhss mounted at {}", cfg.mount.display());

    let stop = Arc::new(AtomicBool::new(false));
    {
        let stop = Arc::clone(&stop);
        if let Err(e) = ctrlc::set_handler(move || {
            info!("signal received, shutting down");
            stop.store(true, Ordering::SeqCst);
        }) {
            warn!("install signal handler: {e}");
        }
    }

    while !stop.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(200));
    }

    info!("stopping adapter");
    adapter.stop();
    drop(session);

    std::thread::sleep(Duration::from_millis(200));
    if is_still_mounted(&cfg.mount) {
        warn!("mount still appears active; running explicit unmount");
        let _ = unmount(&cfg.mount);
    }

    {
        let mut g = lock.lock().unwrap();
        if let Err(e) = g.unlock() {
            warn!("release storage lock: {e}");
        }
    }
    info!("clean shutdown");
}

fn is_still_mounted(mount: &PathBuf) -> bool {
    let Ok(out) = Command::new("mount").output() else {
        return false;
    };
    let s = String::from_utf8_lossy(&out.stdout);
    s.contains(mount.to_string_lossy().as_ref())
}

fn unmount(mount: &PathBuf) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        let out = Command::new("diskutil")
            .arg("unmount")
            .arg(mount.as_os_str())
            .output()?;
        if !out.status.success() {
            let _ = Command::new("umount").arg(mount.as_os_str()).output()?;
        }
    }
    #[cfg(target_os = "linux")]
    {
        let _ = Command::new("fusermount")
            .arg("-u")
            .arg(mount.as_os_str())
            .output()?;
    }
    Ok(())
}
