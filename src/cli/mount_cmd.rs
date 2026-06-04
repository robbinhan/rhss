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
use crate::backend::{Backend, S3Backend, S3Config};
use crate::config::TierPolicy;
use crate::control::{server::OpContext, socket_path_for, ControlServer};
use crate::error::{FsError, Result};
use crate::fuse::FuseConfig;
use crate::index::{PathIndex, SqlitePathIndex, TierId};
use crate::lock::StorageLock;
use crate::policy::{PopularityPolicy, TieringPolicy};
use crate::scan;
use crate::tier::{
    CostAwarePlacement, MirrorPlacement, MostFreePlacement, Placement, RoundRobinPlacement, Tier,
    TierRouter,
};
use crate::tierer::{OpenFileTracker, Tierer};
use crate::{FuseAdapter, PosixBackend};

fn make_placement(pol: Option<&TierPolicy>) -> Result<Box<dyn Placement>> {
    let name = pol.map(|p| p.placement.as_str()).unwrap_or("most_free");
    Ok(match name {
        "most_free" => Box::new(MostFreePlacement),
        "round_robin" => Box::new(RoundRobinPlacement::new()),
        "mirror" => Box::new(MirrorPlacement::new()),
        "cost_aware" => Box::new(CostAwarePlacement::new()),
        other => return Err(FsError::Storage(format!("unknown placement: {other}"))),
    })
}

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

    let make_backend = |b: &crate::config::BackendConfig| -> Arc<dyn Backend> {
        Arc::new(
            PosixBackend::with_cost(b.id.clone(), b.root.clone(), b.cost_per_gb_month)
                .expect("backend init"),
        )
    };
    let fast_backends: Vec<Arc<dyn Backend>> =
        cfg.tier.fast.iter().map(make_backend).collect();
    let slow_backends: Vec<Arc<dyn Backend>> =
        cfg.tier.slow.iter().map(make_backend).collect();

    let fast_pl = match make_placement(cfg.tier.fast_policy.as_ref()) {
        Ok(p) => p,
        Err(e) => {
            error!("fast tier placement: {e}");
            std::process::exit(1);
        }
    };
    let slow_pl = match make_placement(cfg.tier.slow_policy.as_ref()) {
        Ok(p) => p,
        Err(e) => {
            error!("slow tier placement: {e}");
            std::process::exit(1);
        }
    };
    let fast = Tier::new(TierId::Fast, fast_backends, fast_pl).expect("fast tier");
    let slow = Tier::new(TierId::Slow, slow_backends, slow_pl).expect("slow tier");
    let mut router = TierRouter::new(fast, slow);

    // Archive tier (optional). Each S3-style backend needs its creds via env
    // vars (config holds the env-var NAMES, never the secrets).
    if !cfg.tier.archive.is_empty() {
        let mut archive_backends: Vec<Arc<dyn Backend>> = Vec::new();
        for a in &cfg.tier.archive {
            let staging = a.staging_dir.clone().unwrap_or_else(|| {
                cfg.db
                    .parent()
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from("."))
                    .join(".rhss_staging")
                    .join(&a.id)
            });
            let ak = match std::env::var(&a.access_key_env) {
                Ok(v) => v,
                Err(_) => {
                    error!(
                        "archive backend {} missing env var {}",
                        a.id, a.access_key_env
                    );
                    std::process::exit(1);
                }
            };
            let sk = match std::env::var(&a.secret_key_env) {
                Ok(v) => v,
                Err(_) => {
                    error!(
                        "archive backend {} missing env var {}",
                        a.id, a.secret_key_env
                    );
                    std::process::exit(1);
                }
            };
            let backend = match S3Backend::new(S3Config {
                id: a.id.clone(),
                endpoint: a.endpoint.clone(),
                bucket: a.bucket.clone(),
                region: a.region.clone(),
                storage_class: a.storage_class.clone(),
                access_key: ak,
                secret_key: sk,
                staging_root: staging,
                prefix: a.prefix.clone(),
                cost_per_gb_month: a.cost_per_gb_month,
            }) {
                Ok(b) => b as Arc<dyn Backend>,
                Err(e) => {
                    error!("init archive backend {}: {e}", a.id);
                    std::process::exit(1);
                }
            };
            archive_backends.push(backend);
        }
        let archive_pl = match make_placement(cfg.tier.archive_policy.as_ref()) {
            Ok(p) => p,
            Err(e) => {
                error!("archive tier placement: {e}");
                std::process::exit(1);
            }
        };
        let archive_tier = Tier::new(TierId::Archive, archive_backends, archive_pl)
            .map_err(|e| FsError::Storage(format!("archive tier: {e}")))
            .unwrap_or_else(|e| {
                error!("{e}");
                std::process::exit(1);
            });
        router = router.with_archive(archive_tier);
        info!("archive tier configured with {} backend(s)", cfg.tier.archive.len());
    }

    let router = Arc::new(router);

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

    // Control socket — CLI commands (`rhss pin/oneshot/...`) talk to this.
    let control_server = match ControlServer::start(
        socket_path_for(&cfg.db),
        OpContext {
            router: Arc::clone(&router),
            index: Arc::clone(&index),
            open_tracker: Arc::clone(&open_tracker),
            tierer: tierer_handle.clone(),
            config_db_path: cfg.db.clone(),
        },
    ) {
        Ok(srv) => Some(srv),
        Err(e) => {
            warn!("control socket disabled: {e}");
            None
        }
    };

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
    drop(control_server);
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
