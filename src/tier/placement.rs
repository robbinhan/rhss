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

/// Cost-aware placement (D26). Picks the **cheapest** backend with at
/// least `min_free_bytes` available. Backends without a declared
/// `cost_per_gb_month` are treated as infinitely expensive — i.e. they
/// only get chosen when every priced backend is full.
///
/// When NO backend has declared a cost, falls back to `MostFreePlacement`
/// behavior so users can switch to `cost_aware` without immediately
/// breaking when they forget to add costs.
pub struct CostAwarePlacement {
    pub min_free_bytes: u64,
}

impl CostAwarePlacement {
    pub fn new() -> Self {
        Self {
            min_free_bytes: 1024 * 1024 * 1024, // 1 GiB headroom
        }
    }

    pub fn with_headroom(min_free_bytes: u64) -> Self {
        Self { min_free_bytes }
    }
}

impl Default for CostAwarePlacement {
    fn default() -> Self {
        Self::new()
    }
}

impl Placement for CostAwarePlacement {
    fn pick<'a>(&self, backends: &'a [Arc<dyn Backend>]) -> Result<&'a Arc<dyn Backend>> {
        if backends.is_empty() {
            return Err(FsError::Storage("cost-aware: empty backend list".into()));
        }
        // Check if ANY backend has a declared cost. If not, fall through
        // to MostFree so the flag is forgiving in misconfigurations.
        let any_priced = backends.iter().any(|b| b.cost_per_gb_month().is_some());
        if !any_priced {
            return MostFreePlacement.pick(backends);
        }

        let mut best: Option<(f64, &Arc<dyn Backend>)> = None;
        for b in backends {
            let free = b.statvfs().map(|s| s.free_bytes).unwrap_or(0);
            if free < self.min_free_bytes {
                continue;
            }
            // Backends without a cost are treated as infinitely expensive.
            // They still beat "no backend at all" but lose to any priced
            // backend with room.
            let cost = b.cost_per_gb_month().unwrap_or(f64::INFINITY);
            match best {
                Some((existing, _)) if cost >= existing => {}
                _ => best = Some((cost, b)),
            }
        }
        best.map(|(_, b)| b).ok_or_else(|| {
            FsError::Storage(
                "cost-aware: no backend has enough free space (min_free_bytes)".into(),
            )
        })
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

    struct CostBackend {
        id: String,
        free: u64,
        cost: Option<f64>,
    }
    impl Backend for CostBackend {
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
        fn metadata(&self, _: &Path) -> Result<crate::backend::FileMetadata> {
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
        fn set_times(
            &self,
            _: &Path,
            _: Option<std::time::SystemTime>,
            _: Option<std::time::SystemTime>,
        ) -> Result<()> {
            unimplemented!()
        }
        fn statvfs(&self) -> Result<BackendStats> {
            Ok(BackendStats {
                total_bytes: 1_000_000_000_000,
                free_bytes: self.free,
                used_bytes: 1_000_000_000_000 - self.free,
            })
        }
        fn cost_per_gb_month(&self) -> Option<f64> {
            self.cost
        }
    }

    #[test]
    fn cost_aware_picks_cheapest_priced_backend() {
        let bs: Vec<Arc<dyn Backend>> = vec![
            Arc::new(CostBackend {
                id: "r2".into(),
                free: 100 * 1024 * 1024 * 1024, // 100 GiB free
                cost: Some(0.015),
            }),
            Arc::new(CostBackend {
                id: "b2".into(),
                free: 50 * 1024 * 1024 * 1024,
                cost: Some(0.006),
            }),
            Arc::new(CostBackend {
                id: "expensive".into(),
                free: 1_000_000_000_000,
                cost: Some(0.023),
            }),
        ];
        let p = CostAwarePlacement::new();
        assert_eq!(p.pick(&bs).unwrap().id(), "b2");
    }

    #[test]
    fn cost_aware_skips_full_backends() {
        let bs: Vec<Arc<dyn Backend>> = vec![
            Arc::new(CostBackend {
                id: "cheap-full".into(),
                free: 100, // way under headroom
                cost: Some(0.001),
            }),
            Arc::new(CostBackend {
                id: "expensive-roomy".into(),
                free: 1_000_000_000_000,
                cost: Some(0.10),
            }),
        ];
        let p = CostAwarePlacement::with_headroom(1024 * 1024 * 1024);
        assert_eq!(p.pick(&bs).unwrap().id(), "expensive-roomy");
    }

    #[test]
    fn cost_aware_falls_back_when_no_costs_declared() {
        let bs: Vec<Arc<dyn Backend>> = vec![
            Arc::new(CostBackend {
                id: "a".into(),
                free: 100 * 1024 * 1024 * 1024,
                cost: None,
            }),
            Arc::new(CostBackend {
                id: "b".into(),
                free: 500 * 1024 * 1024 * 1024,
                cost: None,
            }),
        ];
        let p = CostAwarePlacement::new();
        // Falls back to MostFree → b has more free.
        assert_eq!(p.pick(&bs).unwrap().id(), "b");
    }

    #[test]
    fn cost_aware_prefers_priced_over_unpriced() {
        let bs: Vec<Arc<dyn Backend>> = vec![
            Arc::new(CostBackend {
                id: "free-but-unknown".into(),
                free: 1_000_000_000_000,
                cost: None,
            }),
            Arc::new(CostBackend {
                id: "priced".into(),
                free: 100 * 1024 * 1024 * 1024,
                cost: Some(0.01),
            }),
        ];
        let p = CostAwarePlacement::new();
        assert_eq!(p.pick(&bs).unwrap().id(), "priced");
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
