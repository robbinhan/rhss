//! Tier = one or more backends + a placement strategy for new files.
//!
//! D4 / D11: multi-disk is **not optional**. A tier always holds a `Vec` of
//! backends; single-disk is just the special case.

use std::sync::Arc;

use crate::backend::Backend;
use crate::error::{FsError, Result};
use crate::index::TierId;

pub mod placement;

pub use placement::{MirrorPlacement, MostFreePlacement, Placement, RoundRobinPlacement};

/// One tier of storage. Owns its backends and the strategy that picks which
/// backend a new file lands on.
pub struct Tier {
    pub id: TierId,
    pub backends: Vec<Arc<dyn Backend>>,
    pub placement: Box<dyn Placement>,
}

impl Tier {
    pub fn new(id: TierId, backends: Vec<Arc<dyn Backend>>, placement: Box<dyn Placement>) -> Result<Self> {
        if backends.is_empty() {
            return Err(FsError::Storage(format!(
                "tier {:?} has no backends",
                id
            )));
        }
        Ok(Self {
            id,
            backends,
            placement,
        })
    }

    pub fn find_backend(&self, id: &str) -> Option<&Arc<dyn Backend>> {
        self.backends.iter().find(|b| b.id() == id)
    }

    /// Pick a backend for a new file. Errors only if no backend has free space.
    pub fn pick(&self) -> Result<&Arc<dyn Backend>> {
        self.placement.pick(&self.backends)
    }

    /// Aggregated usage ratio across all backends in this tier.
    pub fn usage_ratio(&self) -> f64 {
        let mut total: u64 = 0;
        let mut used: u64 = 0;
        for b in &self.backends {
            if let Ok(s) = b.statvfs() {
                total = total.saturating_add(s.total_bytes);
                used = used.saturating_add(s.used_bytes);
            }
        }
        if total == 0 {
            0.0
        } else {
            used as f64 / total as f64
        }
    }

    /// Aggregated total/used/free bytes (for `statfs`).
    pub fn capacity(&self) -> (u64, u64, u64) {
        let mut total = 0u64;
        let mut used = 0u64;
        let mut free = 0u64;
        for b in &self.backends {
            if let Ok(s) = b.statvfs() {
                total = total.saturating_add(s.total_bytes);
                used = used.saturating_add(s.used_bytes);
                free = free.saturating_add(s.free_bytes);
            }
        }
        (total, used, free)
    }
}

/// Router holding all tiers + a way to resolve `backend_id` to the backend
/// instance. Fast and Slow are mandatory; Archive is optional — when absent
/// the system runs as a two-tier system (existing v2.3 behavior).
pub struct TierRouter {
    pub fast: Tier,
    pub slow: Tier,
    pub archive: Option<Tier>,
}

impl TierRouter {
    pub fn new(fast: Tier, slow: Tier) -> Self {
        Self {
            fast,
            slow,
            archive: None,
        }
    }

    pub fn with_archive(mut self, archive: Tier) -> Self {
        self.archive = Some(archive);
        self
    }

    /// Look up a tier by id. Returns `None` only for Archive when no archive
    /// tier is configured.
    pub fn tier(&self, id: TierId) -> Option<&Tier> {
        match id {
            TierId::Fast => Some(&self.fast),
            TierId::Slow => Some(&self.slow),
            TierId::Archive => self.archive.as_ref(),
        }
    }

    /// Like `tier()` but panics on missing — only use when the caller has
    /// already proven the tier exists (e.g. just got it from index lookup).
    pub fn tier_unchecked(&self, id: TierId) -> &Tier {
        self.tier(id)
            .unwrap_or_else(|| panic!("tier {:?} not configured", id))
    }

    pub fn has_archive(&self) -> bool {
        self.archive.is_some()
    }

    pub fn resolve_backend(&self, tier: TierId, backend_id: &str) -> Option<&Arc<dyn Backend>> {
        self.tier(tier).and_then(|t| t.find_backend(backend_id))
    }

    pub fn all_backends(&self) -> impl Iterator<Item = (TierId, &Arc<dyn Backend>)> {
        let mut v: Vec<(TierId, &Arc<dyn Backend>)> = Vec::new();
        for b in &self.fast.backends {
            v.push((TierId::Fast, b));
        }
        for b in &self.slow.backends {
            v.push((TierId::Slow, b));
        }
        if let Some(arc) = &self.archive {
            for b in &arc.backends {
                v.push((TierId::Archive, b));
            }
        }
        v.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::PosixBackend;
    use tempfile::TempDir;

    fn fake(id: &str) -> Arc<dyn Backend> {
        let d = TempDir::new().unwrap();
        let p = d.path().to_path_buf();
        std::mem::forget(d); // leak — only for the test
        Arc::new(PosixBackend::new(id, p).unwrap())
    }

    #[test]
    fn router_without_archive_returns_none() {
        let r = TierRouter::new(
            Tier::new(TierId::Fast, vec![fake("ssd")], Box::new(MostFreePlacement)).unwrap(),
            Tier::new(TierId::Slow, vec![fake("hdd")], Box::new(MostFreePlacement)).unwrap(),
        );
        assert!(!r.has_archive());
        assert!(r.tier(TierId::Archive).is_none());
        assert_eq!(r.all_backends().count(), 2);
    }

    #[test]
    fn router_with_archive_includes_third_tier() {
        let r = TierRouter::new(
            Tier::new(TierId::Fast, vec![fake("ssd")], Box::new(MostFreePlacement)).unwrap(),
            Tier::new(TierId::Slow, vec![fake("hdd")], Box::new(MostFreePlacement)).unwrap(),
        )
        .with_archive(
            Tier::new(
                TierId::Archive,
                vec![fake("s3")],
                Box::new(MostFreePlacement),
            )
            .unwrap(),
        );
        assert!(r.has_archive());
        assert!(r.tier(TierId::Archive).is_some());
        assert_eq!(r.all_backends().count(), 3);
    }
}
