//! FUSE adapter — sync, offset-aware.
//!
//! P0 baseline. Just one `Backend` (no tiering yet — P1+). Key properties:
//!
//! - read/write pass through `offset` to `Backend::read_at` / `write_at`.
//!   v1 corrupted large files because it read the whole file and ignored `_offset`.
//! - `setattr` actually applies size/mode/atime/mtime (v1 was a no-op).
//! - Multi-threaded dispatch via `fuser::Session::spawn_mount2` (D12).
//! - `fh` stores the resolved backend path; in P2 we'll switch it to a raw fd
//!   for zero-overhead passthrough plus `OpenFileTracker` integration.

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow, FUSE_ROOT_ID,
};
use libc::{EEXIST, EIO, ENOENT, ENOSYS};
use parking_lot::Mutex;
use tracing::{debug, error, info, warn};

use crate::backend::Backend;

const TTL: Duration = Duration::from_secs(1);

/// FUSE filter config — paths/patterns the adapter should reject without
/// hitting the backend.
#[derive(Debug, Clone)]
pub struct FuseConfig {
    ignore_names: HashSet<String>,
    ignore_prefixes: Vec<String>,
}

impl Default for FuseConfig {
    fn default() -> Self {
        let mut ignore_names = HashSet::new();
        ignore_names.insert(".DS_Store".to_string());
        Self {
            ignore_names,
            // macOS metadata files. Was previously enforced in the storage layer
            // (wrong place); now lives here per P4.6 plan.
            ignore_prefixes: vec!["._".to_string()],
        }
    }
}

impl FuseConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_ignore_names(mut self, names: impl IntoIterator<Item = String>) -> Self {
        self.ignore_names.extend(names);
        self
    }

    pub fn with_ignore_prefixes(mut self, prefixes: impl IntoIterator<Item = String>) -> Self {
        self.ignore_prefixes.extend(prefixes);
        self
    }

    pub fn should_ignore(&self, path: &Path) -> bool {
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            return false;
        };
        if self.ignore_names.contains(name) {
            return true;
        }
        self.ignore_prefixes
            .iter()
            .any(|prefix| name.starts_with(prefix))
        // v1 had `name.len() == 1 → true` which broke any single-char filename.
        // Removed — see CHANGELOG.
    }
}

struct InodeMap {
    path_to_ino: HashMap<PathBuf, u64>,
    ino_to_path: HashMap<u64, PathBuf>,
    next_ino: u64,
}

impl InodeMap {
    fn new() -> Self {
        let root_path = PathBuf::from("");
        let mut path_to_ino = HashMap::new();
        let mut ino_to_path = HashMap::new();
        path_to_ino.insert(root_path.clone(), FUSE_ROOT_ID);
        ino_to_path.insert(FUSE_ROOT_ID, root_path);
        Self {
            path_to_ino,
            ino_to_path,
            next_ino: FUSE_ROOT_ID + 1,
        }
    }

    fn allocate(&mut self, path: PathBuf) -> u64 {
        if let Some(&ino) = self.path_to_ino.get(&path) {
            return ino;
        }
        let ino = self.next_ino;
        self.next_ino += 1;
        self.path_to_ino.insert(path.clone(), ino);
        self.ino_to_path.insert(ino, path);
        ino
    }

    fn lookup_path(&self, ino: u64) -> Option<PathBuf> {
        self.ino_to_path.get(&ino).cloned()
    }

    fn remove(&mut self, path: &Path) -> Option<u64> {
        let ino = self.path_to_ino.remove(path)?;
        self.ino_to_path.remove(&ino);
        Some(ino)
    }
}

struct FuseState {
    backend: Arc<dyn Backend>,
    inodes: Mutex<InodeMap>,
    fh_table: Mutex<HashMap<u64, PathBuf>>,
    next_fh: AtomicU64,
    config: FuseConfig,
    running: AtomicBool,
}

impl FuseState {
    fn new(backend: Arc<dyn Backend>, config: FuseConfig) -> Self {
        Self {
            backend,
            inodes: Mutex::new(InodeMap::new()),
            fh_table: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            config,
            running: AtomicBool::new(true),
        }
    }

    fn path_for(&self, parent: u64, name: Option<&OsStr>) -> Option<PathBuf> {
        let inodes = self.inodes.lock();
        let mut path = inodes.lookup_path(parent)?;
        if let Some(name) = name {
            path.push(name);
        }
        Some(path)
    }

    fn allocate_fh(&self, path: PathBuf) -> u64 {
        let fh = self.next_fh.fetch_add(1, Ordering::SeqCst);
        self.fh_table.lock().insert(fh, path);
        fh
    }

    fn path_from_fh(&self, fh: u64) -> Option<PathBuf> {
        self.fh_table.lock().get(&fh).cloned()
    }

    fn release_fh(&self, fh: u64) {
        self.fh_table.lock().remove(&fh);
    }

    fn make_attr(&self, ino: u64, meta: &crate::backend::FileMetadata) -> FileAttr {
        FileAttr {
            ino,
            size: meta.size,
            blocks: (meta.size + 511) / 512,
            atime: meta.atime,
            mtime: meta.mtime,
            ctime: meta.ctime,
            crtime: meta.ctime,
            kind: if meta.is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            },
            perm: meta.mode as u16,
            nlink: 1,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            flags: 0,
            blksize: 4096,
        }
    }

    fn root_attr(&self) -> FileAttr {
        let now = SystemTime::now();
        FileAttr {
            ino: FUSE_ROOT_ID,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            flags: 0,
            blksize: 4096,
        }
    }
}

/// Top-level FUSE filesystem.
#[derive(Clone)]
pub struct FuseAdapter {
    state: Arc<FuseState>,
}

impl FuseAdapter {
    pub fn new(backend: Arc<dyn Backend>, config: FuseConfig) -> Self {
        Self {
            state: Arc::new(FuseState::new(backend, config)),
        }
    }

    /// Block on a synchronous mount. Caller's thread becomes the FUSE dispatcher.
    ///
    /// Single-threaded by default — for multi-threaded dispatch (D12) use
    /// [`FuseAdapter::spawn_mount`].
    pub fn mount(&self, mount_point: &Path) -> std::io::Result<()> {
        info!("mounting rhss at {}", mount_point.display());
        fuser::mount2(self.clone(), mount_point, &Self::mount_options())?;
        Ok(())
    }

    /// Multi-threaded mount. Returns a `BackgroundSession` whose drop unmounts.
    pub fn spawn_mount(&self, mount_point: &Path) -> std::io::Result<fuser::BackgroundSession> {
        info!("mounting rhss (multi-thread) at {}", mount_point.display());
        fuser::spawn_mount2(self.clone(), mount_point, &Self::mount_options())
    }

    fn mount_options() -> Vec<MountOption> {
        let mut opts = vec![
            MountOption::DefaultPermissions,
            MountOption::FSName("rhss".to_string()),
            MountOption::AutoUnmount,
        ];
        #[cfg(target_os = "macos")]
        {
            opts.push(MountOption::CUSTOM("volname=rhss".to_string()));
            opts.push(MountOption::CUSTOM("local".to_string()));
            opts.push(MountOption::CUSTOM("noapplexattr".to_string()));
        }
        #[cfg(target_os = "linux")]
        {
            opts.push(MountOption::AllowOther);
            // Linux-specific perf options will land in P3.5 (max_read/max_write/etc).
        }
        opts
    }

    pub fn stop(&self) {
        self.state.running.store(false, Ordering::SeqCst);
        info!("rhss stop requested");
    }
}

fn errno(err: &crate::error::FsError) -> libc::c_int {
    use crate::error::FsError;
    match err {
        FsError::Io(io) => io.raw_os_error().unwrap_or(EIO),
        FsError::NotFound(_) => ENOENT,
        FsError::PermissionDenied(_) => libc::EACCES,
        FsError::InvalidOperation(_) => libc::EINVAL,
        _ => EIO,
    }
}

impl Filesystem for FuseAdapter {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if !self.state.running.load(Ordering::SeqCst) {
            reply.error(ENOSYS);
            return;
        }
        let Some(path) = self.state.path_for(parent, Some(name)) else {
            reply.error(ENOENT);
            return;
        };
        if self.state.config.should_ignore(&path) {
            reply.error(ENOENT);
            return;
        }
        debug!("lookup {}", path.display());
        match self.state.backend.metadata(&path) {
            Ok(meta) => {
                let ino = self.state.inodes.lock().allocate(path.clone());
                let attr = self.state.make_attr(ino, &meta);
                reply.entry(&TTL, &attr, 0);
            }
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if ino == FUSE_ROOT_ID {
            reply.attr(&TTL, &self.state.root_attr());
            return;
        }
        let Some(path) = self.state.inodes.lock().lookup_path(ino) else {
            reply.error(ENOENT);
            return;
        };
        match self.state.backend.metadata(&path) {
            Ok(meta) => {
                let attr = self.state.make_attr(ino, &meta);
                reply.attr(&TTL, &attr);
            }
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let Some(path) = self.state.path_from_fh(fh) else {
            reply.error(ENOENT);
            return;
        };
        match self.state.backend.read_at(&path, offset as u64, size) {
            Ok(data) => reply.data(&data),
            Err(e) => {
                // EOF: io::Error of UnexpectedEof or read returning 0 length.
                if let crate::error::FsError::Io(io) = &e {
                    if io.kind() == std::io::ErrorKind::UnexpectedEof {
                        reply.data(&[]);
                        return;
                    }
                }
                error!("read {} offset={} size={}: {:?}", path.display(), offset, size, e);
                reply.error(errno(&e));
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let Some(path) = self.state.path_from_fh(fh) else {
            reply.error(ENOENT);
            return;
        };
        match self.state.backend.write_at(&path, offset as u64, data) {
            Ok(n) => reply.written(n),
            Err(e) => {
                error!("write {} offset={} len={}: {:?}", path.display(), offset, data.len(), e);
                reply.error(errno(&e));
            }
        }
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        let Some(path) = self.state.inodes.lock().lookup_path(ino) else {
            reply.error(ENOENT);
            return;
        };
        // Verify backend has it (cheap stat).
        match self.state.backend.exists(&path) {
            Ok(true) => {
                let fh = self.state.allocate_fh(path);
                reply.opened(fh, 0);
            }
            Ok(false) => reply.error(ENOENT),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn release(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.state.release_fh(fh);
        reply.ok();
    }

    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let Some(path) = self.state.path_for(parent, Some(name)) else {
            reply.error(ENOENT);
            return;
        };
        if self.state.config.should_ignore(&path) {
            reply.error(EEXIST);
            return;
        }
        match self.state.backend.create_file(&path) {
            Ok(()) => {
                let _ = self.state.backend.set_permissions(&path, mode);
                match self.state.backend.metadata(&path) {
                    Ok(meta) => {
                        let ino = self.state.inodes.lock().allocate(path.clone());
                        let fh = self.state.allocate_fh(path);
                        let attr = self.state.make_attr(ino, &meta);
                        reply.created(&TTL, &attr, 0, fh, 0);
                    }
                    Err(e) => reply.error(errno(&e)),
                }
            }
            Err(e) => {
                error!("create {}: {:?}", path.display(), e);
                reply.error(errno(&e));
            }
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let Some(path) = self.state.path_for(parent, Some(name)) else {
            reply.error(ENOENT);
            return;
        };
        match self.state.backend.create_dir(&path) {
            Ok(()) => {
                let _ = self.state.backend.set_permissions(&path, mode);
                match self.state.backend.metadata(&path) {
                    Ok(meta) => {
                        let ino = self.state.inodes.lock().allocate(path);
                        let attr = self.state.make_attr(ino, &meta);
                        reply.entry(&TTL, &attr, 0);
                    }
                    Err(e) => reply.error(errno(&e)),
                }
            }
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let Some(path) = self.state.path_for(parent, Some(name)) else {
            reply.error(ENOENT);
            return;
        };
        match self.state.backend.remove(&path) {
            Ok(()) => {
                self.state.inodes.lock().remove(&path);
                reply.ok();
            }
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let Some(path) = self.state.path_for(parent, Some(name)) else {
            reply.error(ENOENT);
            return;
        };
        match self.state.backend.remove(&path) {
            Ok(()) => {
                self.state.inodes.lock().remove(&path);
                reply.ok();
            }
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let Some(dir_path) = self.state.inodes.lock().lookup_path(ino) else {
            reply.error(ENOENT);
            return;
        };
        let entries = match self.state.backend.list_dir(&dir_path) {
            Ok(e) => e,
            Err(e) => {
                reply.error(errno(&e));
                return;
            }
        };

        let mut all = Vec::with_capacity(entries.len() + 2);
        all.push((ino, FileType::Directory, ".".to_string()));
        all.push((ino, FileType::Directory, "..".to_string()));
        for name in entries {
            let entry_path = dir_path.join(&name);
            if self.state.config.should_ignore(&entry_path) {
                continue;
            }
            let kind = self
                .state
                .backend
                .metadata(&entry_path)
                .map(|m| {
                    if m.is_dir {
                        FileType::Directory
                    } else {
                        FileType::RegularFile
                    }
                })
                .unwrap_or(FileType::RegularFile);
            let entry_ino = self.state.inodes.lock().allocate(entry_path);
            all.push((entry_ino, kind, name));
        }

        for (i, (entry_ino, kind, name)) in all.into_iter().enumerate().skip(offset as usize) {
            if reply.add(entry_ino, (i + 1) as i64, kind, &name) {
                break;
            }
        }
        reply.ok();
    }

    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let path = match fh.and_then(|h| self.state.path_from_fh(h)) {
            Some(p) => p,
            None => match self.state.inodes.lock().lookup_path(ino) {
                Some(p) => p,
                None => {
                    reply.error(ENOENT);
                    return;
                }
            },
        };

        if let Some(new_size) = size {
            if let Err(e) = self.state.backend.truncate(&path, new_size) {
                error!("setattr truncate {}: {:?}", path.display(), e);
                reply.error(errno(&e));
                return;
            }
        }
        if let Some(new_mode) = mode {
            if let Err(e) = self.state.backend.set_permissions(&path, new_mode) {
                warn!("setattr chmod {}: {:?}", path.display(), e);
            }
        }
        if atime.is_some() || mtime.is_some() {
            let at = atime.map(|t| match t {
                TimeOrNow::SpecificTime(t) => t,
                TimeOrNow::Now => SystemTime::now(),
            });
            let mt = mtime.map(|t| match t {
                TimeOrNow::SpecificTime(t) => t,
                TimeOrNow::Now => SystemTime::now(),
            });
            if let Err(e) = self.state.backend.set_times(&path, at, mt) {
                warn!("setattr utimes {}: {:?}", path.display(), e);
            }
        }

        match self.state.backend.metadata(&path) {
            Ok(meta) => {
                let attr = self.state.make_attr(ino, &meta);
                reply.attr(&TTL, &attr);
            }
            Err(e) => reply.error(errno(&e)),
        }
    }
}
