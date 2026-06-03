//! Synchronous storage backend abstraction.
//!
//! A `Backend` is one physical storage location (one mounted disk). It uses
//! POSIX positional IO (`pread`/`pwrite`) so callers can safely interleave
//! reads/writes on the same file from multiple threads.
//!
//! The previous async `FileSystem` / `Storage` traits in `src/fs/` and
//! `src/storage/` are replaced by this. FUSE callbacks are themselves
//! synchronous; the async layer added overhead with no concurrency benefit.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

pub mod posix;
pub mod s3;

pub use posix::PosixBackend;
pub use s3::{S3Backend, S3Config};

use crate::error::Result;

/// File metadata returned by `Backend::metadata`.
#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub size: u64,
    pub is_dir: bool,
    pub mode: u32,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
}

/// Capacity stats for one backend.
#[derive(Debug, Clone, Copy)]
pub struct BackendStats {
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub used_bytes: u64,
}

impl BackendStats {
    pub fn usage_ratio(&self) -> f64 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        self.used_bytes as f64 / self.total_bytes as f64
    }
}

/// A `Backend` is one physical storage location.
///
/// Paths passed in are relative to the backend's root (the `.rhss_managed/`
/// subdirectory on the underlying disk). Implementations resolve them to
/// absolute paths internally.
pub trait Backend: Send + Sync {
    /// Stable identifier used in `PathIndex` to record which backend owns a file.
    fn id(&self) -> &str;

    /// Root path of this backend (the `.rhss_managed/` directory).
    fn root(&self) -> &Path;

    // Positional IO (pread / pwrite)

    fn read_at(&self, path: &Path, offset: u64, size: u32) -> Result<Vec<u8>>;
    fn write_at(&self, path: &Path, offset: u64, data: &[u8]) -> Result<u32>;
    fn truncate(&self, path: &Path, size: u64) -> Result<()>;
    fn fsync(&self, path: &Path) -> Result<()>;

    // Metadata

    fn metadata(&self, path: &Path) -> Result<FileMetadata>;
    fn exists(&self, path: &Path) -> Result<bool>;

    // Directory ops

    fn list_dir(&self, path: &Path) -> Result<Vec<String>>;
    fn create_dir(&self, path: &Path) -> Result<()>;

    // File lifecycle

    fn create_file(&self, path: &Path) -> Result<()>;
    fn remove(&self, path: &Path) -> Result<()>;

    /// Rename within this backend. Cross-backend moves go through the tierer's
    /// migrate path (copy + remove), not this method.
    fn rename(&self, from: &Path, to: &Path) -> Result<()>;

    // Attribute changes

    fn set_permissions(&self, path: &Path, mode: u32) -> Result<()>;
    fn set_times(
        &self,
        path: &Path,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
    ) -> Result<()>;

    // Capacity

    fn statvfs(&self) -> Result<BackendStats>;

    /// Resolve a logical-relative path to the absolute path on the backing disk.
    /// Used by FUSE `open` to get the real fd that goes into `fi->fh`.
    fn resolve(&self, path: &Path) -> PathBuf;
}
