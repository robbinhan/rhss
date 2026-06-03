//! Background tierer + migrate primitive.
//!
//! - `migrate()` moves one file from its current tier/backend to a target
//!   tier (using that tier's Placement to pick the destination backend).
//!   Skips files that are currently open (autotier-style; D7). Preserves
//!   `atime`/`mtime` (D16). Updates the index in a single SQLite swap.
//!
//! - `Tierer::run` is the background loop: sleeps `tier_period`, evicts the
//!   `coldest_N` files from Fast when usage > `low_watermark`, runs a daily
//!   full sweep (D19).

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};
use tracing::{debug, info, warn};

use crate::backend::Backend;
use crate::error::{FsError, Result};
use crate::index::{Location, PathIndex, ReplicaLoc, TierId};
use crate::policy::TieringPolicy;
use crate::tier::TierRouter;

fn compressed_or_raw(path: &Path, compressed: bool) -> std::path::PathBuf {
    if compressed {
        compress::compressed_path(path)
    } else {
        path.to_path_buf()
    }
}

pub mod compress;
pub mod open_tracker;
pub use compress::{compress_between, ensure_decompressed, hash_file};
pub use open_tracker::OpenFileTracker;

const COPY_BUF_SIZE: usize = 1 << 20; // 1 MiB chunks

/// Migrate a single file. Returns `Ok(false)` if the file was skipped because
/// it's currently open (this is normal; retry next tier cycle).
pub fn migrate(
    router: &TierRouter,
    index: &Arc<dyn PathIndex>,
    open: &OpenFileTracker,
    logical: &Path,
    target_tier: TierId,
) -> Result<bool> {
    if open.is_open(logical) {
        debug!("skip migrate {} (open)", logical.display());
        return Ok(false);
    }

    let row = match index.get(logical)? {
        Some(r) => r,
        None => return Err(FsError::NotFound(logical.to_string_lossy().to_string())),
    };

    if row.location.tier == target_tier {
        return Ok(false);
    }
    if row.pinned_tier.is_some() {
        return Ok(false);
    }

    let src_backend = router
        .resolve_backend(row.location.tier, &row.location.backend_id)
        .ok_or_else(|| {
            FsError::Storage(format!(
                "source backend {} not found",
                row.location.backend_id
            ))
        })?;
    let dst_tier = router
        .tier(target_tier)
        .ok_or_else(|| FsError::Storage(format!("target tier {:?} not configured", target_tier)))?;
    let dst_backends = dst_tier.placement.pick_all(&dst_tier.backends)?;
    let is_mirror = dst_tier.placement.is_replicated();

    // Same-backend short-circuit only applies in the single-replica case.
    if !is_mirror && dst_backends.len() == 1 && Arc::ptr_eq(src_backend, dst_backends[0]) {
        return Ok(false);
    }

    let dst_path = row.location.backend_path.clone();

    // D24: compress immutable files when demoting to Slow. (Archive
    // compression is left for v2 — S3 already does TLS+content-type
    // negotiation and the latency cost of compress-on-PUT is unclear.)
    let should_compress = row.mutability == crate::index::Mutability::Immutable
        && target_tier == TierId::Slow;
    let mut new_hash: Option<String> = row.content_hash.clone();

    // 1. Copy src -> all dst backends (compressed or raw). Roll back any
    //    failure.
    let mut written: Vec<&Arc<dyn Backend>> = Vec::with_capacity(dst_backends.len());
    for dst in &dst_backends {
        let copy_result = if should_compress {
            compress_between(src_backend, &row.location.backend_path, dst, &dst_path)
                .map(|h| {
                    new_hash = Some(h);
                })
        } else {
            copy_streaming(src_backend, &row.location.backend_path, dst, &dst_path)
        };
        if let Err(e) = copy_result {
            warn!(
                "migrate {} replica {} failed; rolling back",
                logical.display(),
                dst.id()
            );
            for already in &written {
                let _ = already.remove(&compressed_or_raw(&dst_path, should_compress));
            }
            return Err(e);
        }
        let actual_path = compressed_or_raw(&dst_path, should_compress);
        if let Err(e) = dst.fsync(&actual_path) {
            warn!(
                "migrate {} replica {} fsync failed; rolling back",
                logical.display(),
                dst.id()
            );
            let _ = dst.remove(&actual_path);
            for already in &written {
                let _ = already.remove(&compressed_or_raw(&dst_path, should_compress));
            }
            return Err(e);
        }
        written.push(dst);
    }

    // 2. Preserve atime/mtime on every replica (D16). Use the actual
    //    on-disk path (`.zst` suffix if compressed) since set_times needs
    //    to find the file.
    if let Ok(orig_meta) = src_backend.metadata(&row.location.backend_path) {
        let actual = compressed_or_raw(&dst_path, should_compress);
        for dst in &written {
            let _ = dst.set_times(&actual, Some(orig_meta.atime), Some(orig_meta.mtime));
        }
    }

    // 3. Update the index. Primary = first replica; full list in `replicas`
    //    when mirroring. For single-replica we leave replicas empty so we
    //    don't bloat the index for the common case.
    let primary = written[0];
    let new_loc = Location {
        tier: target_tier,
        backend_id: primary.id().to_string(),
        backend_path: dst_path.clone(),
        size: row.location.size,
    };
    let replicas: Vec<ReplicaLoc> = if is_mirror {
        written
            .iter()
            .map(|b| ReplicaLoc::new(b.id().to_string(), dst_path.clone()))
            .collect()
    } else {
        Vec::new()
    };

    // swap_location handles the columns; we need a separate write for
    // replicas, but the cleanest is a full row replace via insert.
    let mut full_row = row.clone();
    full_row.location = new_loc;
    full_row.replicas = replicas;
    full_row.state = crate::index::FileState::Stable;
    full_row.compressed = should_compress;
    if let Some(h) = new_hash {
        full_row.content_hash = Some(h);
    }
    index.insert(full_row)?;

    // 4. Best-effort source unlink. Orphans cleaned by startup scrub /
    //    fsck. For mirror migration the "source" can itself be one of the
    //    destinations (same tier replication); we never delete in that case.
    let src_is_dst = written.iter().any(|d| Arc::ptr_eq(src_backend, d));
    if !src_is_dst {
        if let Err(e) = src_backend.remove(&row.location.backend_path) {
            warn!("migrate {} src-unlink failed: {:?}", logical.display(), e);
        }
    }

    Ok(true)
}

fn copy_streaming(
    src: &Arc<dyn Backend>,
    src_path: &Path,
    dst: &Arc<dyn Backend>,
    dst_path: &Path,
) -> Result<()> {
    // P3.5: try kernel fast paths first (Linux copy_file_range, macOS APFS
    // clonefile). Both fail gracefully across-FS / when unavailable —
    // we just fall back to the streaming loop below.
    #[cfg(target_os = "linux")]
    {
        use std::fs::File;
        use std::os::unix::io::AsRawFd;
        if let (Ok(s), Ok(d)) = (
            File::open(src.resolve(src_path)),
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(dst.resolve(dst_path)),
        ) {
            let len = s.metadata().map(|m| m.len()).unwrap_or(0);
            if len > 0 {
                // SAFETY: both fds are valid for the duration of the call.
                let rc = unsafe {
                    libc::copy_file_range(
                        s.as_raw_fd(),
                        std::ptr::null_mut(),
                        d.as_raw_fd(),
                        std::ptr::null_mut(),
                        len as usize,
                        0,
                    )
                };
                if rc as i64 == len as i64 {
                    return Ok(());
                }
                // Otherwise fall through to streaming.
            } else {
                return Ok(());
            }
        }
    }

    let mut offset = 0u64;
    loop {
        let chunk = src.read_at(src_path, offset, COPY_BUF_SIZE as u32)?;
        if chunk.is_empty() {
            return Ok(());
        }
        let written = dst.write_at(dst_path, offset, &chunk)? as u64;
        offset += written;
        if (chunk.len() as u64) < COPY_BUF_SIZE as u64 {
            return Ok(());
        }
    }
}

/// Background tierer.
pub struct Tierer {
    tx: Sender<TierMessage>,
    busy: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

#[derive(Debug)]
enum TierMessage {
    Oneshot,
    Stop,
}

/// Cheaply-clonable handle the FUSE layer holds to nudge the tierer.
#[derive(Clone)]
pub struct TiererHandle {
    tx: Sender<TierMessage>,
    busy: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
}

impl TiererHandle {
    /// Fire a one-shot eviction request. Best-effort: if the channel is full
    /// the tierer is already busy with a previous request, which is fine.
    pub fn trigger_oneshot(&self) {
        let _ = self.tx.try_send(TierMessage::Oneshot);
    }

    /// Block (sleeping 10 ms) until the tierer is idle, or `timeout` elapses.
    /// Used by FUSE write on ENOSPC to wait for an in-flight emergency
    /// eviction before retrying pwrite.
    pub fn wait_idle(&self, timeout: Duration) -> bool {
        let start = Instant::now();
        while self.busy.load(Ordering::SeqCst) {
            if start.elapsed() >= timeout {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        true
    }

    /// Pause/resume the background tierer. While paused, the loop wakes up
    /// on its period but skips the eviction pass. `oneshot` requests are
    /// also no-ops while paused (so an ENOSPC retry can't sneak past).
    pub fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::SeqCst);
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }
}

impl Tierer {
    pub fn spawn(
        router: Arc<TierRouter>,
        index: Arc<dyn PathIndex>,
        open_tracker: Arc<OpenFileTracker>,
        policy: Arc<dyn TieringPolicy>,
    ) -> (Self, TiererHandle) {
        let (tx, rx) = bounded::<TierMessage>(16);
        let busy = Arc::new(AtomicBool::new(false));
        let paused = Arc::new(AtomicBool::new(false));
        let busy_for_thread = Arc::clone(&busy);
        let paused_for_thread = Arc::clone(&paused);
        let handle = std::thread::Builder::new()
            .name("rhss-tierer".into())
            .spawn(move || {
                tierer_loop(
                    router,
                    index,
                    open_tracker,
                    policy,
                    rx,
                    busy_for_thread,
                    paused_for_thread,
                )
            })
            .expect("spawn tierer");
        let h = TiererHandle {
            tx: tx.clone(),
            busy: Arc::clone(&busy),
            paused: Arc::clone(&paused),
        };
        (
            Self {
                tx,
                busy,
                paused,
                handle: Some(handle),
            },
            h,
        )
    }

    pub fn handle(&self) -> TiererHandle {
        TiererHandle {
            tx: self.tx.clone(),
            busy: Arc::clone(&self.busy),
            paused: Arc::clone(&self.paused),
        }
    }
}

impl Drop for Tierer {
    fn drop(&mut self) {
        let _ = self.tx.send(TierMessage::Stop);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn tierer_loop(
    router: Arc<TierRouter>,
    index: Arc<dyn PathIndex>,
    open_tracker: Arc<OpenFileTracker>,
    policy: Arc<dyn TieringPolicy>,
    rx: Receiver<TierMessage>,
    busy: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
) {
    let mut last_full_sweep = Instant::now();
    let day = Duration::from_secs(86_400);

    loop {
        let wait = policy.tier_period().unwrap_or(Duration::from_secs(60 * 60));

        // Wait either for the next period or an oneshot signal.
        let msg = if policy.tier_period().is_none() {
            // Manual-only: block until a message arrives.
            match rx.recv() {
                Ok(m) => m,
                Err(_) => return,
            }
        } else {
            match rx.recv_timeout(wait) {
                Ok(m) => m,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => TierMessage::Oneshot,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
            }
        };

        match msg {
            TierMessage::Stop => return,
            TierMessage::Oneshot => {}
        }

        // Drain any extra oneshot signals so we don't loop without work.
        loop {
            match rx.try_recv() {
                Ok(TierMessage::Stop) => return,
                Ok(TierMessage::Oneshot) => {}
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        }

        if paused.load(Ordering::SeqCst) {
            debug!("tierer: paused — skipping eviction pass");
            continue;
        }

        busy.store(true, Ordering::SeqCst);
        evict_cold(&router, &index, &open_tracker, &policy);

        if last_full_sweep.elapsed() >= day {
            full_sweep(&index, &policy);
            last_full_sweep = Instant::now();
        }
        busy.store(false, Ordering::SeqCst);
    }
}

fn evict_cold(
    router: &TierRouter,
    index: &Arc<dyn PathIndex>,
    open_tracker: &Arc<OpenFileTracker>,
    policy: &Arc<dyn TieringPolicy>,
) {
    // Chain 1: Fast → Slow on the usual watermarks.
    evict_chain(
        router,
        index,
        open_tracker,
        TierId::Fast,
        TierId::Slow,
        policy.low_watermark(),
        policy.high_watermark(),
        policy.min_age_to_evict(),
        || router.fast.capacity(),
        || router.fast.usage_ratio(),
    );

    // Chain 2: Slow → Archive, only when an archive tier is configured.
    if router.has_archive() {
        // Standard age-gated chain for all files.
        let slow_usage = router.slow.usage_ratio();
        if slow_usage > policy.slow_archive_watermark() {
            let target_usage = (policy.slow_archive_watermark() - 0.10).max(0.0);
            evict_chain(
                router,
                index,
                open_tracker,
                TierId::Slow,
                TierId::Archive,
                target_usage,
                policy.slow_archive_watermark(),
                policy.min_age_to_archive(),
                || router.slow.capacity(),
                || router.slow.usage_ratio(),
            );
        }
        // D24: aggressive demotion for immutable Slow-tier files. Skip the
        // age check entirely — if a file is declared immutable and is the
        // coldest by popularity, send it to Archive regardless of how
        // recently it was accessed. The watermark still gates so we don't
        // demote when Slow is nearly empty.
        if router.slow.usage_ratio() > policy.low_watermark() {
            evict_immutable_to_archive(router, index, open_tracker);
        }
    }
}

fn evict_immutable_to_archive(
    router: &TierRouter,
    index: &Arc<dyn PathIndex>,
    open_tracker: &Arc<OpenFileTracker>,
) {
    // Cheap: pull a handful of coldest Slow rows with min_age=0, filter
    // for immutable, demote. Cap at 100 to avoid hot-loops on giant indexes.
    let coldest = match index.coldest(TierId::Slow, u64::MAX, std::time::Duration::ZERO) {
        Ok(c) => c,
        Err(e) => {
            warn!("evict_immutable: coldest query: {:?}", e);
            return;
        }
    };
    for (path, _size) in coldest.into_iter().take(100) {
        let row = match index.get(&path).ok().flatten() {
            Some(r) => r,
            None => continue,
        };
        if row.mutability != crate::index::Mutability::Immutable {
            continue;
        }
        match migrate(router, index, open_tracker, &path, TierId::Archive) {
            Ok(true) => debug!("immutable demote {} → Archive", path.display()),
            Ok(false) => {}
            Err(e) => warn!("immutable migrate {}: {:?}", path.display(), e),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn evict_chain(
    router: &TierRouter,
    index: &Arc<dyn PathIndex>,
    open_tracker: &Arc<OpenFileTracker>,
    src_tier: TierId,
    dst_tier: TierId,
    low_wm: f64,
    high_wm: f64,
    min_age: std::time::Duration,
    capacity_fn: impl Fn() -> (u64, u64, u64),
    usage_fn: impl Fn() -> f64,
) {
    let usage = usage_fn();
    if usage <= low_wm {
        return;
    }

    let target_usage = (low_wm + high_wm) / 2.0;
    let (total, used, _free) = capacity_fn();
    let target_used = (total as f64 * target_usage) as u64;
    let to_free = used.saturating_sub(target_used);
    if to_free == 0 {
        return;
    }

    info!(
        chain = format!("{:?} -> {:?}", src_tier, dst_tier),
        usage = format!("{:.1}%", usage * 100.0),
        target_bytes = to_free,
        "tierer: starting eviction chain"
    );

    let victims = match index.coldest(src_tier, to_free, min_age) {
        Ok(v) => v,
        Err(e) => {
            warn!("coldest query for {:?}: {:?}", src_tier, e);
            return;
        }
    };

    for (path, _size) in victims {
        match migrate(router, index, open_tracker, &path, dst_tier) {
            Ok(true) => debug!("{:?} -> {:?}: {}", src_tier, dst_tier, path.display()),
            Ok(false) => debug!("skipped {} (open or pinned)", path.display()),
            Err(e) => warn!("migrate {}: {:?}", path.display(), e),
        }
    }
}

fn full_sweep(index: &Arc<dyn PathIndex>, _policy: &Arc<dyn TieringPolicy>) {
    // Recompute popularity for every file based on the access counts that
    // accumulated since last sweep. This is the autotier "calc_popularity +
    // sort + simulate" big-batch correction (D19).
    //
    // For v0.1 we keep this minimal: just bump everyone's popularity using
    // their current hit_count over a 1-day window, then reset hit_count.
    // The full "sort + simulate + move" pass can be added later.
    debug!("tierer: daily full-sweep recompute (stub)");
    let _ = index;
    // P2 ships the loop; the recompute pass is intentionally a stub to be
    // fleshed out post-MVP. Hit counts will drift slightly but eviction
    // still works via the per-tier-period coldest_N path.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::PosixBackend;
    use crate::index::{FileRow, FileState, Location, SqlitePathIndex};
    use crate::tier::{MostFreePlacement, Tier};
    use std::path::PathBuf;
    use std::time::UNIX_EPOCH;
    use tempfile::TempDir;

    fn build(
        ssd: &Path,
        hdd: &Path,
        db: &Path,
    ) -> (
        Arc<TierRouter>,
        Arc<dyn PathIndex>,
        Arc<OpenFileTracker>,
    ) {
        let ssd_b: Arc<dyn Backend> = Arc::new(PosixBackend::new("ssd", ssd.to_path_buf()).unwrap());
        let hdd_b: Arc<dyn Backend> = Arc::new(PosixBackend::new("hdd", hdd.to_path_buf()).unwrap());
        let router = TierRouter::new(
            Tier::new(TierId::Fast, vec![ssd_b], Box::new(MostFreePlacement)).unwrap(),
            Tier::new(TierId::Slow, vec![hdd_b], Box::new(MostFreePlacement)).unwrap(),
        );
        let idx = SqlitePathIndex::open(db).unwrap() as Arc<dyn PathIndex>;
        (Arc::new(router), idx, Arc::new(OpenFileTracker::new()))
    }

    fn fixture_row(path: &str) -> FileRow {
        FileRow {
            logical_path: PathBuf::from(path),
            location: Location {
                tier: TierId::Fast,
                backend_id: "ssd".into(),
                backend_path: PathBuf::from(path.trim_start_matches('/')),
                size: 0,

            },
            last_access: UNIX_EPOCH,
            hit_count: 0,
            popularity: 0.0,
            pinned_tier: None,
            state: FileState::Stable,
            replicas: Vec::new(),
            mutability: crate::index::Mutability::Unknown,
            compressed: false,
            content_hash: None,
        }
    }

    #[test]
    fn migrate_moves_file_and_preserves_data() {
        let ssd = TempDir::new().unwrap();
        let hdd = TempDir::new().unwrap();
        let db = TempDir::new().unwrap();
        let (router, idx, open) = build(ssd.path(), hdd.path(), &db.path().join("idx.db"));

        // Write a real file on the SSD side and index it.
        let data = b"hello migrate";
        std::fs::write(ssd.path().join("x.bin"), data).unwrap();
        let mut row = fixture_row("/x.bin");
        row.location.size = data.len() as u64;
        idx.insert(row).unwrap();

        let moved = migrate(&router, &idx, &open, Path::new("/x.bin"), TierId::Slow).unwrap();
        assert!(moved);

        // Now lives on HDD, gone from SSD.
        let loc = idx.locate(Path::new("/x.bin")).unwrap().unwrap();
        assert_eq!(loc.tier, TierId::Slow);
        assert_eq!(loc.backend_id, "hdd");
        assert!(!ssd.path().join("x.bin").exists());
        let got = std::fs::read(hdd.path().join("x.bin")).unwrap();
        assert_eq!(got, data);
    }

    #[test]
    fn migrate_skips_open_files() {
        let ssd = TempDir::new().unwrap();
        let hdd = TempDir::new().unwrap();
        let db = TempDir::new().unwrap();
        let (router, idx, open) = build(ssd.path(), hdd.path(), &db.path().join("idx.db"));
        std::fs::write(ssd.path().join("o.bin"), b"open").unwrap();
        idx.insert({
            let mut r = fixture_row("/o.bin");
            r.location.size = 4;
            r
        })
        .unwrap();
        open.register(Path::new("/o.bin"));
        let moved = migrate(&router, &idx, &open, Path::new("/o.bin"), TierId::Slow).unwrap();
        assert!(!moved);
        // Still on SSD.
        let loc = idx.locate(Path::new("/o.bin")).unwrap().unwrap();
        assert_eq!(loc.tier, TierId::Fast);
    }

    #[test]
    fn migrate_respects_pinned_tier() {
        let ssd = TempDir::new().unwrap();
        let hdd = TempDir::new().unwrap();
        let db = TempDir::new().unwrap();
        let (router, idx, open) = build(ssd.path(), hdd.path(), &db.path().join("idx.db"));
        std::fs::write(ssd.path().join("p.bin"), b"pin").unwrap();
        let mut r = fixture_row("/p.bin");
        r.location.size = 3;
        r.pinned_tier = Some(TierId::Fast);
        idx.insert(r).unwrap();

        let moved = migrate(&router, &idx, &open, Path::new("/p.bin"), TierId::Slow).unwrap();
        assert!(!moved);
    }

    #[test]
    fn migrate_preserves_mtime() {
        let ssd = TempDir::new().unwrap();
        let hdd = TempDir::new().unwrap();
        let db = TempDir::new().unwrap();
        let (router, idx, open) = build(ssd.path(), hdd.path(), &db.path().join("idx.db"));

        std::fs::write(ssd.path().join("t.bin"), b"timestamped").unwrap();
        // Set mtime to a known historical value.
        let target_mtime = UNIX_EPOCH + Duration::from_secs(1_000_000_000);
        rustix::fs::utimensat(
            rustix::fs::CWD,
            ssd.path().join("t.bin").as_os_str(),
            &rustix::fs::Timestamps {
                last_access: rustix::fs::Timespec {
                    tv_sec: 1_000_000_000,
                    tv_nsec: 0,
                },
                last_modification: rustix::fs::Timespec {
                    tv_sec: 1_000_000_000,
                    tv_nsec: 0,
                },
            },
            rustix::fs::AtFlags::empty(),
        )
        .unwrap();

        let mut r = fixture_row("/t.bin");
        r.location.size = 11;
        idx.insert(r).unwrap();

        migrate(&router, &idx, &open, Path::new("/t.bin"), TierId::Slow).unwrap();

        // Now check HDD copy has the same mtime.
        let meta = std::fs::metadata(hdd.path().join("t.bin")).unwrap();
        let mtime = meta.modified().unwrap();
        assert_eq!(mtime, target_mtime);
    }
}
