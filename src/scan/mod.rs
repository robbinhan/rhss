//! First-scan ingestion (D13).
//!
//! Walks every backend's `.rhss_managed/` subtree and registers each file in
//! the `PathIndex`. Idempotent: re-running over an already-indexed tree is a
//! no-op. Resumable: rows are inserted as we go; if we crash, the next run
//! continues from where we left off.
//!
//! Conflicts (same logical path on multiple backends) **hard-fail** — see D13.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tracing::{info, warn};
use walkdir::WalkDir;

use crate::backend::Backend;
use crate::error::{FsError, Result};
use crate::index::{FileRow, FileState, Location, PathIndex, TierId};
use crate::tier::TierRouter;

/// Stats reported back to the caller for logging / UI.
#[derive(Debug, Default, Clone)]
pub struct ScanStats {
    pub indexed: u64,
    pub skipped_existing: u64,
    pub conflicts: Vec<PathBuf>,
}

/// Run a single full scan over both tiers.
///
/// Conflicts are collected and returned in `ScanStats.conflicts`. If any are
/// present the caller MUST treat this as a hard failure and abort the mount.
pub fn first_scan(router: &TierRouter, index: &Arc<dyn PathIndex>) -> Result<ScanStats> {
    let mut stats = ScanStats::default();

    // Map of logical_path -> (tier, backend_id) we've claimed during THIS scan
    // (in addition to any already-indexed). This is how we detect cross-backend
    // conflicts cleanly.
    let mut claimed: HashMap<PathBuf, (TierId, String)> = HashMap::new();

    for (tier_id, backend) in router.all_backends() {
        info!(
            tier = ?tier_id,
            backend = backend.id(),
            root = %backend.root().display(),
            "scanning backend"
        );
        scan_one(backend, tier_id, index, &mut claimed, &mut stats)?;
    }

    if !stats.conflicts.is_empty() {
        warn!(
            count = stats.conflicts.len(),
            "first-scan: cross-backend logical-path conflicts"
        );
    }
    info!(indexed = stats.indexed, skipped = stats.skipped_existing, "scan complete");
    Ok(stats)
}

fn scan_one(
    backend: &Arc<dyn Backend>,
    tier: TierId,
    index: &Arc<dyn PathIndex>,
    claimed: &mut HashMap<PathBuf, (TierId, String)>,
    stats: &mut ScanStats,
) -> Result<()> {
    let root = backend.root().to_path_buf();
    for entry in WalkDir::new(&root).follow_links(false).into_iter() {
        let entry = entry.map_err(|e| FsError::Storage(format!("walk: {e}")))?;
        if entry.file_type().is_dir() {
            continue;
        }
        if !entry.file_type().is_file() {
            // skip symlinks / sockets / etc.
            continue;
        }

        let abs = entry.path();
        let rel = match abs.strip_prefix(&root) {
            Ok(r) => r.to_path_buf(),
            Err(_) => continue,
        };
        let logical = PathBuf::from("/").join(&rel);

        // Conflict detection: did another backend already register this logical
        // path during THIS scan?
        if let Some((other_tier, other_id)) = claimed.get(&logical) {
            if other_id != backend.id() {
                warn!(
                    logical = %logical.display(),
                    a = %format!("{:?}:{}", other_tier, other_id),
                    b = %format!("{:?}:{}", tier, backend.id()),
                    "conflict during scan"
                );
                stats.conflicts.push(logical.clone());
                continue;
            }
        }

        // Did a prior run already index this path? If so, leave it alone
        // (idempotent). The previous run wrote its tier+backend; trust it.
        if index.locate(&logical)?.is_some() {
            claimed.insert(logical.clone(), (tier, backend.id().to_string()));
            stats.skipped_existing += 1;
            continue;
        }

        // Stat to get initial size + mtime → last_access (D17: macOS atime
        // unreliable, so we use mtime; popularity initial is set by P2's policy
        // when it picks up the file).
        let meta = backend.metadata(&rel)?;
        let row = FileRow {
            logical_path: logical.clone(),
            location: Location {
                tier,
                backend_id: backend.id().to_string(),
                backend_path: rel.clone(),
                size: meta.size,
            },
            last_access: meta.mtime,
            hit_count: 0,
            popularity: 0.0,
            pinned_tier: None,
            state: FileState::Stable,
        };
        index.insert(row)?;
        claimed.insert(logical, (tier, backend.id().to_string()));
        stats.indexed += 1;
    }
    Ok(())
}

/// Verify and prepare backend root directories. Creates `.rhss_managed/` if
/// missing. Returns an error if any root cannot be created.
pub fn ensure_managed_dirs(roots: impl IntoIterator<Item = impl AsRef<Path>>) -> Result<()> {
    for root in roots {
        let root = root.as_ref();
        if !root.exists() {
            std::fs::create_dir_all(root).map_err(FsError::Io)?;
        } else if !root.is_dir() {
            return Err(FsError::Storage(format!(
                "expected directory, got file: {}",
                root.display()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::PosixBackend;
    use crate::index::SqlitePathIndex;
    use crate::tier::{MostFreePlacement, Tier, TierRouter};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn make_router(
        fast_roots: &[&Path],
        slow_roots: &[&Path],
    ) -> TierRouter {
        let fast: Vec<Arc<dyn Backend>> = fast_roots
            .iter()
            .enumerate()
            .map(|(i, r)| {
                Arc::new(PosixBackend::new(format!("ssd-{i}"), r.to_path_buf()).unwrap())
                    as Arc<dyn Backend>
            })
            .collect();
        let slow: Vec<Arc<dyn Backend>> = slow_roots
            .iter()
            .enumerate()
            .map(|(i, r)| {
                Arc::new(PosixBackend::new(format!("hdd-{i}"), r.to_path_buf()).unwrap())
                    as Arc<dyn Backend>
            })
            .collect();
        TierRouter::new(
            Tier::new(TierId::Fast, fast, Box::new(MostFreePlacement)).unwrap(),
            Tier::new(TierId::Slow, slow, Box::new(MostFreePlacement)).unwrap(),
        )
    }

    #[test]
    fn scans_files_into_index() {
        let ssd = TempDir::new().unwrap();
        let hdd = TempDir::new().unwrap();
        let db = TempDir::new().unwrap();

        std::fs::write(ssd.path().join("a.txt"), b"hi").unwrap();
        std::fs::create_dir_all(hdd.path().join("dir")).unwrap();
        std::fs::write(hdd.path().join("dir/b.bin"), b"bytes").unwrap();

        let router = make_router(&[ssd.path()], &[hdd.path()]);
        let index = SqlitePathIndex::open(db.path().join("idx.db")).unwrap()
            as Arc<dyn PathIndex>;
        let stats = first_scan(&router, &index).unwrap();
        assert_eq!(stats.indexed, 2);
        assert!(stats.conflicts.is_empty());

        let row_a = index.get(Path::new("/a.txt")).unwrap().unwrap();
        assert_eq!(row_a.location.backend_id, "ssd-0");
        let row_b = index.get(Path::new("/dir/b.bin")).unwrap().unwrap();
        assert_eq!(row_b.location.backend_id, "hdd-0");
    }

    #[test]
    fn detects_cross_backend_conflict() {
        let ssd_a = TempDir::new().unwrap();
        let ssd_b = TempDir::new().unwrap();
        let hdd = TempDir::new().unwrap();
        let db = TempDir::new().unwrap();

        // Same relative path on two SSDs → conflict.
        std::fs::write(ssd_a.path().join("dup"), b"a").unwrap();
        std::fs::write(ssd_b.path().join("dup"), b"b").unwrap();

        let router = make_router(&[ssd_a.path(), ssd_b.path()], &[hdd.path()]);
        let index = SqlitePathIndex::open(db.path().join("idx.db")).unwrap()
            as Arc<dyn PathIndex>;
        let stats = first_scan(&router, &index).unwrap();
        assert_eq!(stats.conflicts.len(), 1);
        assert_eq!(stats.conflicts[0], Path::new("/dup"));
    }

    #[test]
    fn idempotent_rescan_no_duplicates() {
        let ssd = TempDir::new().unwrap();
        let hdd = TempDir::new().unwrap();
        let db = TempDir::new().unwrap();

        std::fs::write(ssd.path().join("x"), b"hi").unwrap();

        let router = make_router(&[ssd.path()], &[hdd.path()]);
        let index = SqlitePathIndex::open(db.path().join("idx.db")).unwrap()
            as Arc<dyn PathIndex>;
        let s1 = first_scan(&router, &index).unwrap();
        let s2 = first_scan(&router, &index).unwrap();
        assert_eq!(s1.indexed, 1);
        assert_eq!(s2.indexed, 0);
        assert_eq!(s2.skipped_existing, 1);
        assert_eq!(index.count().unwrap(), 1);
    }
}
