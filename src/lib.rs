//! rhss — Rust Hybrid Storage System.
//!
//! v2.3 plan: see `docs/plan/README.md`.

pub mod access;
pub mod backend;
pub mod cli;
pub mod config;
pub mod error;
pub mod fuse;
pub mod index;
pub mod lock;
pub mod policy;
pub mod scan;
pub mod tier;
pub mod tierer;

pub use backend::{Backend, BackendStats, FileMetadata, PosixBackend};
pub use config::RhssConfig;
pub use error::{FsError, Result};
pub use fuse::FuseAdapter;
pub use index::{PathIndex, SqlitePathIndex, TierId};
pub use policy::{PopularityPolicy, TieringPolicy};
pub use tier::{Tier, TierRouter};
pub use tierer::{OpenFileTracker, Tierer, TiererHandle};
