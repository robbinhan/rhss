//! Tier = one or more backends + a placement strategy for new files.
//!
//! D4 / D11: multi-disk is **not optional**. A tier always holds a `Vec` of
//! backends; single-disk is just the special case.

use std::sync::Arc;

use crate::backend::Backend;
use crate::error::{FsError, Result};
use crate::index::TierId;

pub mod placement;

pub use placement::{MostFreePlacement, Placement, RoundRobinPlacement};

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

/// Router holding both tiers + a way to resolve `backend_id` to the backend
/// instance. The path index records `backend_id`, so the FUSE layer needs to
/// be able to translate back to the actual `Backend` to call its methods.
pub struct TierRouter {
    pub fast: Tier,
    pub slow: Tier,
}

impl TierRouter {
    pub fn new(fast: Tier, slow: Tier) -> Self {
        Self { fast, slow }
    }

    pub fn tier(&self, id: TierId) -> &Tier {
        match id {
            TierId::Fast => &self.fast,
            TierId::Slow => &self.slow,
        }
    }

    pub fn resolve_backend(&self, tier: TierId, backend_id: &str) -> Option<&Arc<dyn Backend>> {
        self.tier(tier).find_backend(backend_id)
    }

    pub fn all_backends(&self) -> impl Iterator<Item = (TierId, &Arc<dyn Backend>)> {
        self.fast
            .backends
            .iter()
            .map(|b| (TierId::Fast, b))
            .chain(self.slow.backends.iter().map(|b| (TierId::Slow, b)))
    }
}
