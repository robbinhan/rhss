//! Placement strategies — decide which backend within a tier gets a new file.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::backend::Backend;
use crate::error::{FsError, Result};

pub trait Placement: Send + Sync {
    /// Pick a single backend for a new file. Used by callers that handle
    /// only one location at a time (legacy callers, FUSE create on
    /// non-mirror tiers, etc.).
    fn pick<'a>(&self, backends: &'a [Arc<dyn Backend>]) -> Result<&'a Arc<dyn Backend>>;

    /// Pick all backends a write should land on. For single-location
    /// placements (MostFree, RoundRobin) returns one backend; for
    /// `MirrorPlacement` returns all backends.
    fn pick_all<'a>(
        &self,
        backends: &'a [Arc<dyn Backend>],
    ) -> Result<Vec<&'a Arc<dyn Backend>>> {
        Ok(vec![self.pick(backends)?])
    }

    /// Hint flag: does this placement strategy use multiple backends per file?
    fn is_replicated(&self) -> bool {
        false
    }
}

/// Pick the backend with the most free space. MVP default.
pub struct MostFreePlacement;

impl Placement for MostFreePlacement {
    fn pick<'a>(&self, backends: &'a [Arc<dyn Backend>]) -> Result<&'a Arc<dyn Backend>> {
        let mut best: Option<(u64, &Arc<dyn Backend>)> = None;
        for b in backends {
            if let Ok(s) = b.statvfs() {
                match best {
                    Some((free, _)) if s.free_bytes <= free => {}
                    _ => best = Some((s.free_bytes, b)),
                }
            }
        }
        best.map(|(_, b)| b).ok_or_else(|| {
            FsError::Storage("MostFreePlacement: no backend has reachable statvfs".into())
        })
    }
}

/// Round-robin. Mostly useful for tests; production usually wants MostFree.
pub struct RoundRobinPlacement {
    next: AtomicUsize,
}

impl RoundRobinPlacement {
    pub fn new() -> Self {
        Self {
            next: AtomicUsize::new(0),
        }
    }
}

impl Default for RoundRobinPlacement {
    fn default() -> Self {
        Self::new()
    }
}

impl Placement for RoundRobinPlacement {
    fn pick<'a>(&self, backends: &'a [Arc<dyn Backend>]) -> Result<&'a Arc<dyn Backend>> {
        if backends.is_empty() {
            return Err(FsError::Storage("round-robin: empty backend list".into()));
        }
        let i = self.next.fetch_add(1, Ordering::SeqCst) % backends.len();
        Ok(&backends[i])
    }
}

/// Mirror — every write lands on every backend in the tier (D23). For
/// reads, `pick` returns one chosen backend (round-robin) so callers that
/// only need one location don't try to download from N at once. Use
/// `pick_all` for the N-write path during migration.
pub struct MirrorPlacement {
    next: AtomicUsize,
}

impl MirrorPlacement {
    pub fn new() -> Self {
        Self {
            next: AtomicUsize::new(0),
        }
    }
}

impl Default for MirrorPlacement {
    fn default() -> Self {
        Self::new()
    }
}

impl Placement for MirrorPlacement {
    fn pick<'a>(&self, backends: &'a [Arc<dyn Backend>]) -> Result<&'a Arc<dyn Backend>> {
        if backends.is_empty() {
            return Err(FsError::Storage("mirror: empty backend list".into()));
        }
        let i = self.next.fetch_add(1, Ordering::SeqCst) % backends.len();
        Ok(&backends[i])
    }

    fn pick_all<'a>(
        &self,
        backends: &'a [Arc<dyn Backend>],
    ) -> Result<Vec<&'a Arc<dyn Backend>>> {
        if backends.is_empty() {
            return Err(FsError::Storage("mirror: empty backend list".into()));
        }
        Ok(backends.iter().collect())
    }

    fn is_replicated(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{Backend, BackendStats, FileMetadata};
    use std::path::{Path, PathBuf};
    use std::time::SystemTime;

    struct FakeBackend {
        id: String,
        free: u64,
    }
    impl Backend for FakeBackend {
        fn id(&self) -> &str {
            &self.id
        }
        fn root(&self) -> &Path {
            Path::new("/tmp")
        }
        fn resolve(&self, _: &Path) -> PathBuf {
            PathBuf::new()
        }
        fn read_at(&self, _: &Path, _: u64, _: u32) -> Result<Vec<u8>> {
            unimplemented!()
        }
        fn write_at(&self, _: &Path, _: u64, _: &[u8]) -> Result<u32> {
            unimplemented!()
        }
        fn truncate(&self, _: &Path, _: u64) -> Result<()> {
            unimplemented!()
        }
        fn fsync(&self, _: &Path) -> Result<()> {
            unimplemented!()
        }
        fn metadata(&self, _: &Path) -> Result<FileMetadata> {
            unimplemented!()
        }
        fn exists(&self, _: &Path) -> Result<bool> {
            unimplemented!()
        }
        fn list_dir(&self, _: &Path) -> Result<Vec<String>> {
            unimplemented!()
        }
        fn create_dir(&self, _: &Path) -> Result<()> {
            unimplemented!()
        }
        fn create_file(&self, _: &Path) -> Result<()> {
            unimplemented!()
        }
        fn remove(&self, _: &Path) -> Result<()> {
            unimplemented!()
        }
        fn rename(&self, _: &Path, _: &Path) -> Result<()> {
            unimplemented!()
        }
        fn set_permissions(&self, _: &Path, _: u32) -> Result<()> {
            unimplemented!()
        }
        fn set_times(&self, _: &Path, _: Option<SystemTime>, _: Option<SystemTime>) -> Result<()> {
            unimplemented!()
        }
        fn statvfs(&self) -> Result<BackendStats> {
            Ok(BackendStats {
                total_bytes: 1_000_000,
                free_bytes: self.free,
                used_bytes: 1_000_000 - self.free,
            })
        }
    }

    #[test]
    fn most_free_picks_emptiest() {
        let bs: Vec<Arc<dyn Backend>> = vec![
            Arc::new(FakeBackend { id: "a".into(), free: 100 }),
            Arc::new(FakeBackend { id: "b".into(), free: 999 }),
            Arc::new(FakeBackend { id: "c".into(), free: 500 }),
        ];
        let p = MostFreePlacement;
        let chosen = p.pick(&bs).unwrap();
        assert_eq!(chosen.id(), "b");
    }

    #[test]
    fn mirror_returns_all_backends() {
        let bs: Vec<Arc<dyn Backend>> = vec![
            Arc::new(FakeBackend { id: "a".into(), free: 100 }),
            Arc::new(FakeBackend { id: "b".into(), free: 999 }),
            Arc::new(FakeBackend { id: "c".into(), free: 500 }),
        ];
        let p = MirrorPlacement::new();
        let all = p.pick_all(&bs).unwrap();
        assert_eq!(all.len(), 3);
        let ids: Vec<&str> = all.iter().map(|b| b.id()).collect();
        assert!(ids.contains(&"a"));
        assert!(ids.contains(&"b"));
        assert!(ids.contains(&"c"));
        assert!(p.is_replicated());
    }

    #[test]
    fn most_free_pick_all_returns_one() {
        let bs: Vec<Arc<dyn Backend>> = vec![
            Arc::new(FakeBackend { id: "a".into(), free: 100 }),
            Arc::new(FakeBackend { id: "b".into(), free: 999 }),
        ];
        let p = MostFreePlacement;
        let all = p.pick_all(&bs).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].id(), "b");
        assert!(!p.is_replicated());
    }

    #[test]
    fn round_robin_cycles() {
        let bs: Vec<Arc<dyn Backend>> = vec![
            Arc::new(FakeBackend { id: "a".into(), free: 100 }),
            Arc::new(FakeBackend { id: "b".into(), free: 200 }),
        ];
        let p = RoundRobinPlacement::new();
        assert_eq!(p.pick(&bs).unwrap().id(), "a");
        assert_eq!(p.pick(&bs).unwrap().id(), "b");
        assert_eq!(p.pick(&bs).unwrap().id(), "a");
    }
}
