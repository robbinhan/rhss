//! rhss CLI — P0 baseline.
//!
//! Mounts a single backend (one directory) at a mount point. Tiering, multi-
//! disk routing, and the path index all arrive in P1+. This binary's purpose
//! at P0 is to exercise the sync `Backend` + offset-aware FUSE pipeline so
//! `dd bs=4k` of a 1 GiB file roundtrips correctly.

use std::path::PathBuf;
use std::process::Command;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use clap::Parser;
use rhss::{FuseAdapter, PosixBackend};
use rhss::fuse::FuseConfig;
use rhss::lock::StorageLock;
use tracing::{error, info, warn};
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Mount point (must be an empty directory).
    #[arg(short, long)]
    mount: PathBuf,

    /// Backend root directory. For P0 this is a single directory; P1 will
    /// switch to a config file that declares multiple per-tier backends.
    #[arg(short, long)]
    backend: PathBuf,

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

    if let Err(e) = std::fs::create_dir_all(&args.mount) {
        error!("create mount point {}: {}", args.mount.display(), e);
        std::process::exit(1);
    }
    if !args.backend.is_dir() {
        error!("backend directory does not exist: {}", args.backend.display());
        std::process::exit(1);
    }

    let lock = Arc::new(std::sync::Mutex::new(StorageLock::new(
        &args.backend,
        &args.backend,
    )));
    {
        let mut g = lock.lock().unwrap();
        let res = if args.force { g.force_lock() } else { g.try_lock() };
        if let Err(e) = res {
            error!("acquire storage lock: {e}");
            std::process::exit(1);
        }
    }
    info!("acquired storage lock");

    let backend = match PosixBackend::new("posix0", args.backend.clone()) {
        Ok(b) => Arc::new(b) as Arc<dyn rhss::Backend>,
        Err(e) => {
            error!("init backend: {e}");
            std::process::exit(1);
        }
    };

    let adapter = FuseAdapter::new(backend, FuseConfig::default());

    // Mount in background so the main thread can wait on signals.
    let session = match adapter.spawn_mount(&args.mount) {
        Ok(s) => s,
        Err(e) => {
            error!("mount {}: {e}", args.mount.display());
            std::process::exit(1);
        }
    };
    info!("rhss mounted at {}", args.mount.display());

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
    drop(session); // triggers unmount

    // Best-effort: if the OS didn't catch the AutoUnmount option, run umount.
    std::thread::sleep(Duration::from_millis(200));
    if is_still_mounted(&args.mount) {
        warn!("mount still appears active; running explicit unmount");
        let _ = unmount(&args.mount);
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
