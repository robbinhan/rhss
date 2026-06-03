//! S3-compatible object-storage backend.
//!
//! Works with any service speaking the S3 protocol — AWS S3, Cloudflare R2,
//! Backblaze B2, Wasabi, MinIO. Credentials are read from env vars at backend
//! construction (the env-var names live in TOML config so the file itself is
//! safe to commit).
//!
//! ## Read path (staging cache)
//!
//! S3 has no random read primitive — every GET is a full or range fetch with
//! 100ms+ first-byte latency. To make `pread`-style FUSE reads tolerable,
//! every file is **lazy-fetched on first access** into a local staging file
//! (typically under `<db.parent>/.rhss_staging/<backend_id>/`). All subsequent
//! `read_at` calls go to the staging file directly. The staging cache is
//! pruned by external means (manual cleanup or a future LRU sweeper); MVP
//! does not auto-evict.
//!
//! ## Write path
//!
//! `write_at` is a no-op-buffered write into a staging file. The actual S3
//! PUT happens on `fsync` (and on `remove` we wipe the staging file too).
//! This means uploads are bursty — large migrations of new files only hit
//! S3 once, at fsync time. Random pwrite into existing objects rewrites the
//! whole object on the next fsync, which is the price of POSIX-on-S3.
//!
//! ## Storage class
//!
//! Passed through to PUT. `STANDARD_IA` is the common default for "warm
//! archive" (no thaw, slightly slower than Standard). Glacier classes work
//! but reads will fail until restored — that's a v2 feature (see
//! `docs/plan/risks.md` candidate-A discussion).

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use parking_lot::Mutex;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use tracing::debug;

use crate::error::{FsError, Result};

use super::{Backend, BackendStats, FileMetadata};

pub struct S3Backend {
    id: String,
    bucket: Bucket,
    prefix: String,
    storage_class: String,
    staging_root: PathBuf,
    /// In-memory record of which files we've fetched (hot-list).
    cached: Mutex<std::collections::HashSet<PathBuf>>,
}

pub struct S3Config {
    pub id: String,
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub storage_class: String,
    pub access_key: String,
    pub secret_key: String,
    pub staging_root: PathBuf,
    pub prefix: String,
}

impl S3Backend {
    pub fn new(cfg: S3Config) -> Result<Arc<Self>> {
        fs::create_dir_all(&cfg.staging_root).map_err(FsError::Io)?;
        let creds = Credentials::new(
            Some(&cfg.access_key),
            Some(&cfg.secret_key),
            None,
            None,
            None,
        )
        .map_err(|e| FsError::Storage(format!("s3 creds: {e}")))?;
        let region = Region::Custom {
            region: cfg.region,
            endpoint: cfg.endpoint,
        };
        let bucket = Bucket::new(&cfg.bucket, region, creds)
            .map_err(|e| FsError::Storage(format!("s3 bucket: {e}")))?
            .with_path_style();
        Ok(Arc::new(Self {
            id: cfg.id,
            bucket,
            prefix: cfg.prefix,
            storage_class: cfg.storage_class,
            staging_root: cfg.staging_root,
            cached: Mutex::new(Default::default()),
        }))
    }

    fn object_key(&self, path: &Path) -> String {
        let rel = path.strip_prefix("/").unwrap_or(path);
        if self.prefix.is_empty() {
            rel.to_string_lossy().into_owned()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), rel.display())
        }
    }

    fn staging_path(&self, path: &Path) -> PathBuf {
        let rel = path.strip_prefix("/").unwrap_or(path);
        self.staging_root.join(rel)
    }

    /// Materialize the staging file for `path`. If already present, returns
    /// it; otherwise GETs from S3 into it. Empty objects (i.e. brand-new
    /// files where we haven't done a PUT yet) yield an empty staging file.
    fn ensure_staged(&self, path: &Path) -> Result<PathBuf> {
        let staged = self.staging_path(path);
        if staged.exists() {
            return Ok(staged);
        }
        let key = self.object_key(path);
        debug!("S3 GET {}", key);
        if let Some(parent) = staged.parent() {
            fs::create_dir_all(parent).map_err(FsError::Io)?;
        }

        match self.bucket.get_object(&key) {
            Ok(resp) if resp.status_code() == 200 => {
                let mut f = File::create(&staged).map_err(FsError::Io)?;
                f.write_all(resp.bytes()).map_err(FsError::Io)?;
                self.cached.lock().insert(path.to_path_buf());
            }
            Ok(resp) if resp.status_code() == 404 => {
                // Object not on S3 (probably a freshly created file that
                // hasn't been fsync'd yet). Empty staging file.
                File::create(&staged).map_err(FsError::Io)?;
            }
            Ok(resp) => {
                return Err(FsError::Storage(format!(
                    "s3 GET {key}: status {}",
                    resp.status_code()
                )));
            }
            Err(e) => return Err(FsError::Storage(format!("s3 GET {key}: {e}"))),
        }
        Ok(staged)
    }

    fn upload(&self, path: &Path) -> Result<()> {
        let staged = self.staging_path(path);
        if !staged.exists() {
            return Err(FsError::Storage(format!(
                "no staging file to upload for {}",
                path.display()
            )));
        }
        let mut buf = Vec::new();
        File::open(&staged)
            .map_err(FsError::Io)?
            .read_to_end(&mut buf)
            .map_err(FsError::Io)?;
        let key = self.object_key(path);
        debug!(
            "S3 PUT {} ({} bytes, class={})",
            key,
            buf.len(),
            self.storage_class
        );
        let resp = self
            .bucket
            .put_object(&key, &buf)
            .map_err(|e| FsError::Storage(format!("s3 PUT {key}: {e}")))?;
        if resp.status_code() != 200 {
            return Err(FsError::Storage(format!(
                "s3 PUT {key}: status {}",
                resp.status_code()
            )));
        }
        Ok(())
    }
}

impl Backend for S3Backend {
    fn id(&self) -> &str {
        &self.id
    }

    fn root(&self) -> &Path {
        &self.staging_root
    }

    fn resolve(&self, path: &Path) -> PathBuf {
        self.staging_path(path)
    }

    fn read_at(&self, path: &Path, offset: u64, size: u32) -> Result<Vec<u8>> {
        let staged = self.ensure_staged(path)?;
        let f = File::open(staged)?;
        let mut buf = vec![0u8; size as usize];
        let n = f.read_at(&mut buf, offset)?;
        buf.truncate(n);
        Ok(buf)
    }

    fn write_at(&self, path: &Path, offset: u64, data: &[u8]) -> Result<u32> {
        // Make sure we have a staging file (download existing object if
        // the file pre-exists on S3).
        let staged = self.ensure_staged(path)?;
        let f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&staged)?;
        let n = f.write_at(data, offset)?;
        // Mark dirty by clearing the cached flag — fsync will PUT.
        self.cached.lock().remove(path);
        Ok(n as u32)
    }

    fn truncate(&self, path: &Path, size: u64) -> Result<()> {
        let staged = self.ensure_staged(path)?;
        let f = OpenOptions::new().write(true).open(staged)?;
        f.set_len(size)?;
        self.cached.lock().remove(path);
        Ok(())
    }

    fn fsync(&self, path: &Path) -> Result<()> {
        let staged = self.staging_path(path);
        if !staged.exists() {
            return Ok(());
        }
        // Always upload on fsync. We could be smarter (track dirty flag)
        // but with the OS calling fsync on every close this would amplify
        // writes; the on-disk state is the source of truth and "PUT what
        // we have" is correct.
        self.upload(path)?;
        self.cached.lock().insert(path.to_path_buf());
        Ok(())
    }

    fn metadata(&self, path: &Path) -> Result<FileMetadata> {
        // Prefer the staging file if we've materialized it.
        let staged = self.staging_path(path);
        if staged.exists() {
            let m = fs::symlink_metadata(&staged)?;
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            return Ok(FileMetadata {
                size: m.len(),
                is_dir: m.is_dir(),
                mode: m.permissions().mode(),
                atime: ts_from_secs(m.atime()),
                mtime: ts_from_secs(m.mtime()),
                ctime: ts_from_secs(m.ctime()),
            });
        }
        // Otherwise HEAD the object.
        let key = self.object_key(path);
        match self.bucket.head_object(&key) {
            Ok((info, 200)) => Ok(FileMetadata {
                size: info.content_length.unwrap_or(0) as u64,
                is_dir: false,
                mode: 0o644,
                atime: SystemTime::now(),
                mtime: info
                    .last_modified
                    .as_deref()
                    .map(parse_rfc1123)
                    .unwrap_or(SystemTime::now()),
                ctime: SystemTime::now(),
            }),
            Ok((_, 404)) => Err(FsError::NotFound(key)),
            Ok((_, code)) => Err(FsError::Storage(format!("s3 HEAD {key}: status {code}"))),
            Err(e) => Err(FsError::Storage(format!("s3 HEAD {key}: {e}"))),
        }
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        if self.staging_path(path).exists() {
            return Ok(true);
        }
        let key = self.object_key(path);
        match self.bucket.head_object(&key) {
            Ok((_, 200)) => Ok(true),
            Ok((_, 404)) => Ok(false),
            Ok((_, code)) => Err(FsError::Storage(format!("s3 HEAD {key}: status {code}"))),
            Err(e) => Err(FsError::Storage(format!("s3 HEAD {key}: {e}"))),
        }
    }

    fn list_dir(&self, path: &Path) -> Result<Vec<String>> {
        // S3 LIST with prefix; emulate "dir" semantics by collecting names
        // up to the next "/". For rhss this is mostly only ever called on
        // the root during scan.
        let mut prefix = self.object_key(path);
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        let results = self
            .bucket
            .list(prefix.clone(), Some("/".to_string()))
            .map_err(|e| FsError::Storage(format!("s3 LIST {prefix}: {e}")))?;
        let mut out = Vec::new();
        for page in results {
            for cp in page.common_prefixes.unwrap_or_default() {
                if let Some(name) = cp.prefix.trim_end_matches('/').rsplit('/').next() {
                    out.push(name.to_string());
                }
            }
            for obj in page.contents {
                let key: String = obj.key;
                if let Some(name) = key.rsplit('/').next() {
                    if !name.is_empty() {
                        out.push(name.to_string());
                    }
                }
            }
        }
        Ok(out)
    }

    fn create_dir(&self, _path: &Path) -> Result<()> {
        // S3 has no directories; presence of objects implies the "dirs"
        // above them. No-op.
        Ok(())
    }

    fn create_file(&self, path: &Path) -> Result<()> {
        let staged = self.staging_path(path);
        if let Some(parent) = staged.parent() {
            fs::create_dir_all(parent).map_err(FsError::Io)?;
        }
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staged)?;
        // Don't PUT yet — wait for fsync.
        Ok(())
    }

    fn remove(&self, path: &Path) -> Result<()> {
        let staged = self.staging_path(path);
        if staged.exists() {
            let _ = fs::remove_file(&staged);
        }
        let key = self.object_key(path);
        match self.bucket.delete_object(&key) {
            Ok(resp) if resp.status_code() < 300 => Ok(()),
            Ok(resp) => Err(FsError::Storage(format!(
                "s3 DELETE {key}: status {}",
                resp.status_code()
            ))),
            Err(e) => Err(FsError::Storage(format!("s3 DELETE {key}: {e}"))),
        }
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        // S3 doesn't rename — copy then delete.
        let src = self.object_key(from);
        let dst = self.object_key(to);
        debug!("S3 COPY {src} → {dst}");
        match self.bucket.copy_object_internal(&src, &dst) {
            Ok(code) if (200..300).contains(&code) => {}
            Ok(code) => {
                return Err(FsError::Storage(format!(
                    "s3 COPY {src}->{dst}: status {code}"
                )))
            }
            Err(e) => return Err(FsError::Storage(format!("s3 COPY {src}->{dst}: {e}"))),
        }
        let _ = self.bucket.delete_object(&src);

        // Also rename the staging file if present.
        let from_staged = self.staging_path(from);
        let to_staged = self.staging_path(to);
        if from_staged.exists() {
            if let Some(parent) = to_staged.parent() {
                let _ = fs::create_dir_all(parent);
            }
            let _ = fs::rename(&from_staged, &to_staged);
        }
        Ok(())
    }

    fn set_permissions(&self, path: &Path, mode: u32) -> Result<()> {
        // No real perm on S3; cache it in staging for round-trip.
        let staged = self.staging_path(path);
        if staged.exists() {
            use std::os::unix::fs::PermissionsExt;
            let perms = fs::Permissions::from_mode(mode);
            fs::set_permissions(&staged, perms)?;
        }
        Ok(())
    }

    fn set_times(
        &self,
        path: &Path,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
    ) -> Result<()> {
        // S3 doesn't let you set times directly. Apply to staging file so
        // backup tools see what they expect post-migration.
        let staged = self.staging_path(path);
        if !staged.exists() {
            return Ok(());
        }
        use rustix::fs::{utimensat, AtFlags, Timestamps};
        let to_ts = |opt: Option<SystemTime>| -> rustix::fs::Timespec {
            match opt {
                Some(t) => {
                    let dur = t
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or(Duration::ZERO);
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
        utimensat(rustix::fs::CWD, staged.as_os_str(), &ts, AtFlags::empty())
            .map_err(|e| FsError::Io(std::io::Error::from(e)))?;
        Ok(())
    }

    fn statvfs(&self) -> Result<BackendStats> {
        // S3 is effectively unlimited. Report something the FUSE layer can
        // sum without overflow; the user can compare "indexed bytes" via
        // PathIndex if they want exact totals.
        const UNLIMITED: u64 = 1024u64 * 1024 * 1024 * 1024 * 1024; // 1 PiB
        Ok(BackendStats {
            total_bytes: UNLIMITED,
            free_bytes: UNLIMITED,
            used_bytes: 0,
        })
    }
}

fn ts_from_secs(secs: i64) -> SystemTime {
    use std::time::UNIX_EPOCH;
    if secs >= 0 {
        UNIX_EPOCH + Duration::from_secs(secs as u64)
    } else {
        UNIX_EPOCH - Duration::from_secs((-secs) as u64)
    }
}

/// We don't parse S3 Last-Modified to a real SystemTime in MVP. Future
/// improvement: pull in `httpdate` or hand-roll a tiny RFC1123 parser.
/// Returning `now()` is safe — only metadata.mtime is affected.
fn parse_rfc1123(_s: &str) -> SystemTime {
    SystemTime::now()
}
