//! `rhss mount` — the original foreground-mount flow, now reachable via
//! subcommand. Same behavior as v2.3's `rhss --config ...`.

use std::path::PathBuf;
use std::process::Command;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use tracing::{error, info, warn};

use crate::access::AccessTracker;
use crate::backend::Backend;
use crate::error::Result;
use crate::fuse::FuseConfig;
use crate::index::{PathIndex, SqlitePathIndex, TierId};
use crate::lock::StorageLock;
use crate::policy::{PopularityPolicy, TieringPolicy};
use crate::scan;
use crate::tier::{MostFreePlacement, Tier, TierRouter};
use crate::tierer::{OpenFileTracker, Tierer};
use crate::{FuseAdapter, PosixBackend};

use super::common::CliContext;
use super::MountArgs;

pub fn run(ctx: &CliContext, args: MountArgs) -> Result<()> {
    let cfg = ctx.load_config()?;

    if let Err(e) = std::fs::create_dir_all(&cfg.mount) {
        error!("create mount point {}: {e}", cfg.mount.display());
        std::process::exit(1);
    }
    if let Some(parent) = cfg.db.parent() {
        if !parent.as_os_str().is_empty() {
            let _ = std::fs::create_dir_all(parent);
        }
    }

    let lock_dir = cfg
        .db
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let lock = Arc::new(std::sync::Mutex::new(StorageLock::new(&lock_dir, &lock_dir)));
    {
        let mut g = lock.lock().unwrap();
        let res = if args.force {
            g.force_lock()
        } else {
            g.try_lock()
        };
        if let Err(e) = res {
            error!("acquire storage lock: {e}");
            std::process::exit(1);
        }
    }
    info!("acquired storage lock");

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

    let fast = Tier::new(TierId::Fast, fast_backends, Box::new(MostFreePlacement))
        .expect("fast tier");
    let slow = Tier::new(TierId::Slow, slow_backends, Box::new(MostFreePlacement))
        .expect("slow tier");
    let router = Arc::new(TierRouter::new(fast, slow));

    let index: Arc<dyn PathIndex> = match SqlitePathIndex::open(&cfg.db) {
        Ok(i) => i,
        Err(e) => {
            error!("open index {}: {e}", cfg.db.display());
            std::process::exit(1);
        }
    };

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

    // Silence unused warning when access is moved into adapter via Some(access).
    let _ = ctx.json;

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
    Ok(())
}

fn is_still_mounted(mount: &std::path::Path) -> bool {
    let Ok(out) = Command::new("mount").output() else {
        return false;
    };
    let s = String::from_utf8_lossy(&out.stdout);
    s.contains(mount.to_string_lossy().as_ref())
}

fn unmount(mount: &std::path::Path) -> std::io::Result<()> {
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
