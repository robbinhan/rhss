//! rhss — Rust Hybrid Storage System.
//!
//! See `docs/plan/README.md` for the v2.3 plan. This is the P0 baseline:
//! sync `Backend` trait + `PosixBackend` + offset-aware FUSE IO.
//! Indexing, multi-disk, tierer, etc. come in P1+.

pub mod backend;
pub mod error;
pub mod fuse;
pub mod lock;

pub use backend::{Backend, BackendStats, FileMetadata, PosixBackend};
pub use error::{FsError, Result};
pub use fuse::FuseAdapter;
