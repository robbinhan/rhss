//! rhss — Rust Hybrid Storage System.
//!
//! See `docs/plan/README.md` for the v2.3 plan. P0 + P1 baseline:
//! sync Backend trait, offset-aware FUSE, persistent SQLite path index,
//! multi-disk per-tier, first-scan ingestion. Background tierer comes in P2.

pub mod access;
pub mod backend;
pub mod config;
pub mod error;
pub mod fuse;
pub mod index;
pub mod lock;
pub mod scan;
pub mod tier;

pub use backend::{Backend, BackendStats, FileMetadata, PosixBackend};
pub use config::RhssConfig;
pub use error::{FsError, Result};
pub use fuse::FuseAdapter;
pub use index::{PathIndex, SqlitePathIndex, TierId};
pub use tier::{Tier, TierRouter};
