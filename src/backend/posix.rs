//! POSIX filesystem backend.
//!
//! Uses `std::os::unix::fs::FileExt` for positional IO (`pread`/`pwrite`),
//! `File::set_len` for truncate, and `libc::statvfs` for capacity stats.

use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::{FsError, Result};

use super::{Backend, BackendStats, FileMetadata};

/// POSIX backend rooted at a directory (typically `<disk>/.rhss_managed/`).
pub struct PosixBackend {
    id: String,
    root: PathBuf,
}

impl PosixBackend {
    /// Create a new backend rooted at `root`. The directory must exist.
    pub fn new(id: impl Into<String>, root: impl Into<PathBuf>) -> Result<Self> {
        let id = id.into();
        let root = root.into();
        if !root.is_dir() {
            return Err(FsError::Storage(format!(
                "backend root does not exist or is not a directory: {}",
                root.display()
            )));
        }
        Ok(Self { id, root })
    }

    fn full(&self, rel: &Path) -> PathBuf {
        // Strip leading "/" so join treats `rel` as relative.
        let rel = rel.strip_prefix("/").unwrap_or(rel);
        self.root.join(rel)
    }
}

impl Backend for PosixBackend {
    fn id(&self) -> &str {
        &self.id
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn resolve(&self, path: &Path) -> PathBuf {
        self.full(path)
    }

    fn read_at(&self, path: &Path, offset: u64, size: u32) -> Result<Vec<u8>> {
        let f = File::open(self.full(path))?;
        let mut buf = vec![0u8; size as usize];
        let n = f.read_at(&mut buf, offset)?;
        buf.truncate(n);
        Ok(buf)
    }

    fn write_at(&self, path: &Path, offset: u64, data: &[u8]) -> Result<u32> {
        let f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.full(path))?;
        let n = f.write_at(data, offset)?;
        Ok(n as u32)
    }

    fn truncate(&self, path: &Path, size: u64) -> Result<()> {
        let f = OpenOptions::new().write(true).open(self.full(path))?;
        f.set_len(size)?;
        Ok(())
    }

    fn fsync(&self, path: &Path) -> Result<()> {
        let f = OpenOptions::new().write(true).open(self.full(path))?;
        // On macOS, fsync only flushes to the drive's internal cache.
        // F_FULLFSYNC actually pushes data to platters/cells. Use it at
        // critical persistence points (the migrate path is the main caller).
        #[cfg(target_os = "macos")]
        {
            use std::os::unix::io::AsRawFd;
            // SAFETY: f is a valid open file; fcntl with F_FULLFSYNC takes
            // no extra argument and returns 0 on success / -1 on error. We
            // fall back to a normal sync_all on failure.
            let rc = unsafe { libc::fcntl(f.as_raw_fd(), libc::F_FULLFSYNC) };
            if rc == -1 {
                f.sync_all()?;
            }
        }
        #[cfg(not(target_os = "macos"))]
        {
            f.sync_all()?;
        }
        Ok(())
    }

    fn metadata(&self, path: &Path) -> Result<FileMetadata> {
        let m = fs::symlink_metadata(self.full(path))?;
        Ok(FileMetadata {
            size: m.len(),
            is_dir: m.is_dir(),
            mode: m.permissions().mode(),
            atime: ts_from_secs(m.atime()),
            mtime: ts_from_secs(m.mtime()),
            ctime: ts_from_secs(m.ctime()),
        })
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        Ok(self.full(path).exists())
    }

    fn list_dir(&self, path: &Path) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for entry in fs::read_dir(self.full(path))? {
            let entry = entry?;
            if let Some(name) = entry.file_name().to_str() {
                out.push(name.to_string());
            }
        }
        Ok(out)
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        fs::create_dir_all(self.full(path))?;
        Ok(())
    }

    fn create_file(&self, path: &Path) -> Result<()> {
        let full = self.full(path);
        if let Some(parent) = full.parent() {
            fs::create_dir_all(parent)?;
        }
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&full)?;
        Ok(())
    }

    fn remove(&self, path: &Path) -> Result<()> {
        let full = self.full(path);
        let m = fs::symlink_metadata(&full)?;
        if m.is_dir() {
            fs::remove_dir(&full)?;
        } else {
            fs::remove_file(&full)?;
        }
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        fs::rename(self.full(from), self.full(to))?;
        Ok(())
    }

    fn set_permissions(&self, path: &Path, mode: u32) -> Result<()> {
        let perms = fs::Permissions::from_mode(mode);
        fs::set_permissions(self.full(path), perms)?;
        Ok(())
    }

    fn set_times(
        &self,
        path: &Path,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
    ) -> Result<()> {
        // Use rustix to call utimensat with proper UTIME_OMIT for None values.
        use rustix::fs::{utimensat, AtFlags, Timestamps};

        let to_ts = |opt: Option<SystemTime>| -> rustix::fs::Timespec {
            match opt {
                Some(t) => {
                    let dur = t.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
                    rustix::fs::Timespec {
                        tv_sec: dur.as_secs() as _,
                        tv_nsec: dur.subsec_nanos() as _,
                    }
                }
                None => rustix::fs::Timespec {
                    tv_sec: 0,
                    tv_nsec: rustix::fs::UTIME_OMIT,
                },
            }
        };

        let ts = Timestamps {
            last_access: to_ts(atime),
            last_modification: to_ts(mtime),
        };
        utimensat(
            rustix::fs::CWD,
            self.full(path).as_os_str(),
            &ts,
            AtFlags::empty(),
        )
        .map_err(|e| FsError::Io(std::io::Error::from(e)))?;
        Ok(())
    }

    fn statvfs(&self) -> Result<BackendStats> {
        use rustix::fs::statvfs;
        let s = statvfs(self.root.as_os_str())
            .map_err(|e| FsError::Io(std::io::Error::from(e)))?;
        let block_size = s.f_frsize as u64;
        let total = s.f_blocks as u64 * block_size;
        let free = s.f_bavail as u64 * block_size;
        Ok(BackendStats {
            total_bytes: total,
            free_bytes: free,
            used_bytes: total.saturating_sub(free),
        })
    }
}

fn ts_from_secs(secs: i64) -> SystemTime {
    if secs >= 0 {
        UNIX_EPOCH + Duration::from_secs(secs as u64)
    } else {
        UNIX_EPOCH - Duration::from_secs((-secs) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_backend() -> (TempDir, PosixBackend) {
        let dir = TempDir::new().unwrap();
        let backend = PosixBackend::new("test", dir.path().to_path_buf()).unwrap();
        (dir, backend)
    }

    #[test]
    fn write_then_read_roundtrip() {
        let (_dir, b) = make_backend();
        let p = Path::new("foo.bin");
        let data = b"hello world";
        let n = b.write_at(p, 0, data).unwrap();
        assert_eq!(n as usize, data.len());

        let got = b.read_at(p, 0, data.len() as u32).unwrap();
        assert_eq!(got, data);
    }

    #[test]
    fn write_at_offset_does_not_truncate() {
        let (_dir, b) = make_backend();
        let p = Path::new("a.bin");

        // Write 1024 bytes at offset 0
        let chunk1 = vec![b'a'; 1024];
        b.write_at(p, 0, &chunk1).unwrap();

        // Write 1024 bytes at offset 4096 (hole in between)
        let chunk2 = vec![b'b'; 1024];
        b.write_at(p, 4096, &chunk2).unwrap();

        let meta = b.metadata(p).unwrap();
        assert_eq!(meta.size, 4096 + 1024);

        // Read back chunk1: must still be there (v1 bug: pwrite would not have
        // truncated, but rhss v1's whole-file write would have lost it)
        let got1 = b.read_at(p, 0, 1024).unwrap();
        assert_eq!(got1, chunk1);

        let got2 = b.read_at(p, 4096, 1024).unwrap();
        assert_eq!(got2, chunk2);
    }

    #[test]
    fn truncate_changes_size() {
        let (_dir, b) = make_backend();
        let p = Path::new("t.bin");
        b.write_at(p, 0, &[0u8; 1000]).unwrap();
        b.truncate(p, 500).unwrap();
        assert_eq!(b.metadata(p).unwrap().size, 500);
        b.truncate(p, 2000).unwrap();
        assert_eq!(b.metadata(p).unwrap().size, 2000);
    }

    #[test]
    fn statvfs_returns_nonzero_total() {
        let (_dir, b) = make_backend();
        let s = b.statvfs().unwrap();
        assert!(s.total_bytes > 0);
    }

    #[test]
    fn list_dir_works() {
        let (_dir, b) = make_backend();
        b.create_file(Path::new("a.txt")).unwrap();
        b.create_file(Path::new("b.txt")).unwrap();
        let mut entries = b.list_dir(Path::new("")).unwrap();
        entries.sort();
        assert_eq!(entries, vec!["a.txt", "b.txt"]);
    }

    #[test]
    fn rename_within_backend() {
        let (_dir, b) = make_backend();
        b.write_at(Path::new("old.bin"), 0, b"data").unwrap();
        b.rename(Path::new("old.bin"), Path::new("new.bin")).unwrap();
        assert!(!b.exists(Path::new("old.bin")).unwrap());
        assert!(b.exists(Path::new("new.bin")).unwrap());
    }
}
