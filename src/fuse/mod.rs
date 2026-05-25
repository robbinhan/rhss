//! FUSE adapter — P1 version.
//!
//! Now backed by `TierRouter` (multi-disk) and `PathIndex` (SQLite). FUSE
//! callbacks resolve `logical_path → Location → Backend` and call the right
//! disk. Background tierer (P2) hasn't landed yet; new files always go to
//! Fast for now, with no migration.

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow, FUSE_ROOT_ID,
};
use libc::{EEXIST, EIO, ENOENT, ENOSYS};
use parking_lot::Mutex;
use tracing::{debug, error, info, warn};

use crate::access::AccessTracker;
use crate::backend::{Backend, FileMetadata as BackendMeta};
use crate::error::FsError;
use crate::index::{FileRow, FileState, Location, PathIndex};
use crate::policy::TieringPolicy;
use crate::tier::TierRouter;
use crate::tierer::OpenFileTracker;

const TTL: Duration = Duration::from_secs(1);

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
            ignore_prefixes: vec!["._".to_string()],
        }
    }
}

impl FuseConfig {
    pub fn new() -> Self {
        Self::default()
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
    }
}

struct InodeMap {
    path_to_ino: HashMap<PathBuf, u64>,
    ino_to_path: HashMap<u64, PathBuf>,
    next_ino: u64,
}

impl InodeMap {
    fn new() -> Self {
        let root_path = PathBuf::from("/");
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

    fn remove(&mut self, path: &Path) {
        if let Some(ino) = self.path_to_ino.remove(path) {
            self.ino_to_path.remove(&ino);
        }
    }

    #[allow(dead_code)]
    fn rename(&mut self, from: &Path, to: PathBuf) {
        if let Some(ino) = self.path_to_ino.remove(from) {
            self.path_to_ino.insert(to.clone(), ino);
            self.ino_to_path.insert(ino, to);
        }
    }
}

struct FhEntry {
    logical: PathBuf,
    backend: Arc<dyn Backend>,
    backend_path: PathBuf,
}

struct FuseState {
    router: Arc<TierRouter>,
    index: Arc<dyn PathIndex>,
    policy: Arc<dyn TieringPolicy>,
    open_tracker: Arc<OpenFileTracker>,
    access: Option<AccessTracker>,
    inodes: Mutex<InodeMap>,
    fh_table: Mutex<HashMap<u64, FhEntry>>,
    next_fh: AtomicU64,
    config: FuseConfig,
    running: AtomicBool,
}

impl FuseState {
    fn make_attr(&self, ino: u64, meta: &BackendMeta) -> FileAttr {
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

    fn path_for(&self, parent: u64, name: &OsStr) -> Option<PathBuf> {
        let inodes = self.inodes.lock();
        let mut path = inodes.lookup_path(parent)?;
        path.push(name);
        Some(path)
    }

    /// Resolve a logical path to (backend, backend-relative path) by looking
    /// up the path index. Returns `None` if not indexed.
    fn resolve(&self, logical: &Path) -> Option<(Arc<dyn Backend>, PathBuf)> {
        let loc = self.index.locate(logical).ok().flatten()?;
        let backend = self.router.resolve_backend(loc.tier, &loc.backend_id)?;
        Some((Arc::clone(backend), loc.backend_path))
    }

    fn allocate_fh(&self, entry: FhEntry) -> u64 {
        let fh = self.next_fh.fetch_add(1, Ordering::SeqCst);
        self.fh_table.lock().insert(fh, entry);
        fh
    }

    fn fh(&self, fh: u64) -> Option<(Arc<dyn Backend>, PathBuf, PathBuf)> {
        let t = self.fh_table.lock();
        t.get(&fh)
            .map(|e| (Arc::clone(&e.backend), e.backend_path.clone(), e.logical.clone()))
    }

    fn release_fh(&self, fh: u64) -> Option<PathBuf> {
        self.fh_table.lock().remove(&fh).map(|e| e.logical)
    }
}

/// Top-level FUSE adapter.
#[derive(Clone)]
pub struct FuseAdapter {
    state: Arc<FuseState>,
}

impl FuseAdapter {
    pub fn new(
        router: Arc<TierRouter>,
        index: Arc<dyn PathIndex>,
        policy: Arc<dyn TieringPolicy>,
        open_tracker: Arc<OpenFileTracker>,
        access: Option<AccessTracker>,
        config: FuseConfig,
    ) -> Self {
        Self {
            state: Arc::new(FuseState {
                router,
                index,
                policy,
                open_tracker,
                access,
                inodes: Mutex::new(InodeMap::new()),
                fh_table: Mutex::new(HashMap::new()),
                next_fh: AtomicU64::new(1),
                config,
                running: AtomicBool::new(true),
            }),
        }
    }

    pub fn mount(&self, mount_point: &Path) -> std::io::Result<()> {
        info!("mounting rhss at {}", mount_point.display());
        fuser::mount2(self.clone(), mount_point, &Self::mount_options())?;
        Ok(())
    }

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
        }
        opts
    }

    pub fn stop(&self) {
        self.state.running.store(false, Ordering::SeqCst);
        info!("rhss stop requested");
    }
}

fn errno(err: &FsError) -> libc::c_int {
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
        let Some(path) = self.state.path_for(parent, name) else {
            reply.error(ENOENT);
            return;
        };
        if self.state.config.should_ignore(&path) {
            reply.error(ENOENT);
            return;
        }
        debug!("lookup {}", path.display());

        // Two possibilities: directory (resolved via filesystem walk on any
        // backend) or file (must be in index).
        if let Some((backend, bpath)) = self.state.resolve(&path) {
            match backend.metadata(&bpath) {
                Ok(meta) => {
                    let ino = self.state.inodes.lock().allocate(path);
                    let attr = self.state.make_attr(ino, &meta);
                    reply.entry(&TTL, &attr, 0);
                }
                Err(e) => reply.error(errno(&e)),
            }
            return;
        }

        // Maybe it's a directory. Probe each fast backend's filesystem (P1
        // simplification: directories aren't tracked in the index; they live on
        // every backend that has anything below them).
        for (_tier, backend) in self.state.router.all_backends() {
            // Strip leading "/" since backend.metadata takes a relative path.
            let rel = path.strip_prefix("/").unwrap_or(&path);
            if let Ok(meta) = backend.metadata(rel) {
                if meta.is_dir {
                    let ino = self.state.inodes.lock().allocate(path);
                    let attr = self.state.make_attr(ino, &meta);
                    reply.entry(&TTL, &attr, 0);
                    return;
                }
            }
        }
        reply.error(ENOENT);
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

        if let Some((backend, bpath)) = self.state.resolve(&path) {
            match backend.metadata(&bpath) {
                Ok(meta) => reply.attr(&TTL, &self.state.make_attr(ino, &meta)),
                Err(e) => reply.error(errno(&e)),
            }
            return;
        }

        // Directory probe (same as lookup).
        for (_tier, backend) in self.state.router.all_backends() {
            let rel = path.strip_prefix("/").unwrap_or(&path);
            if let Ok(meta) = backend.metadata(rel) {
                reply.attr(&TTL, &self.state.make_attr(ino, &meta));
                return;
            }
        }
        reply.error(ENOENT);
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
        let Some((backend, bpath, logical)) = self.state.fh(fh) else {
            reply.error(ENOENT);
            return;
        };
        match backend.read_at(&bpath, offset as u64, size) {
            Ok(data) => {
                if let Some(t) = &self.state.access {
                    t.record(logical, SystemTime::now());
                }
                reply.data(&data);
            }
            Err(e) => {
                error!("read {} offset={} size={}: {:?}", bpath.display(), offset, size, e);
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
        let Some((backend, bpath, logical)) = self.state.fh(fh) else {
            reply.error(ENOENT);
            return;
        };
        match backend.write_at(&bpath, offset as u64, data) {
            Ok(n) => {
                if let Some(t) = &self.state.access {
                    t.record(logical, SystemTime::now());
                }
                reply.written(n);
            }
            Err(e) => {
                error!(
                    "write {} offset={} len={}: {:?}",
                    bpath.display(),
                    offset,
                    data.len(),
                    e
                );
                reply.error(errno(&e));
            }
        }
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        let Some(logical) = self.state.inodes.lock().lookup_path(ino) else {
            reply.error(ENOENT);
            return;
        };
        let Some((backend, bpath)) = self.state.resolve(&logical) else {
            reply.error(ENOENT);
            return;
        };
        if let Err(e) = backend.exists(&bpath) {
            reply.error(errno(&e));
            return;
        }
        self.state.open_tracker.register(&logical);
        let fh = self.state.allocate_fh(FhEntry {
            logical: logical.clone(),
            backend,
            backend_path: bpath,
        });
        if let Some(t) = &self.state.access {
            t.record(logical, SystemTime::now());
        }
        reply.opened(fh, 0);
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
        if let Some(logical) = self.state.release_fh(fh) {
            self.state.open_tracker.release(&logical);
        }
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
        let Some(logical) = self.state.path_for(parent, name) else {
            reply.error(ENOENT);
            return;
        };
        if self.state.config.should_ignore(&logical) {
            reply.error(EEXIST);
            return;
        }

        // Watermark routing (D6 / D17 / D20). When Fast is over panic, new
        // files go directly to Slow so we don't hit ENOSPC on Fast.
        let fast_usage = self.state.router.fast.usage_ratio();
        let tier = self.state.policy.tier_for_create(fast_usage);
        let backend = match self.state.router.tier(tier).pick() {
            Ok(b) => Arc::clone(b),
            Err(e) => {
                reply.error(errno(&e));
                return;
            }
        };
        let rel = logical.strip_prefix("/").unwrap_or(&logical).to_path_buf();

        if let Err(e) = backend.create_file(&rel) {
            error!("create {}: {:?}", logical.display(), e);
            reply.error(errno(&e));
            return;
        }
        let _ = backend.set_permissions(&rel, mode);
        let meta = match backend.metadata(&rel) {
            Ok(m) => m,
            Err(e) => {
                reply.error(errno(&e));
                return;
            }
        };

        let row = FileRow {
            logical_path: logical.clone(),
            location: Location {
                tier,
                backend_id: backend.id().to_string(),
                backend_path: rel.clone(),
                size: meta.size,
            },
            last_access: SystemTime::now(),
            hit_count: 0,
            popularity: self.state.policy.initial_popularity(), // D17
            pinned_tier: None,
            state: FileState::Stable,
        };
        if let Err(e) = self.state.index.insert(row) {
            reply.error(errno(&e));
            return;
        }

        let ino = self.state.inodes.lock().allocate(logical.clone());
        self.state.open_tracker.register(&logical);
        let fh = self.state.allocate_fh(FhEntry {
            logical,
            backend,
            backend_path: rel,
        });
        let attr = self.state.make_attr(ino, &meta);
        reply.created(&TTL, &attr, 0, fh, 0);
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
        let Some(logical) = self.state.path_for(parent, name) else {
            reply.error(ENOENT);
            return;
        };
        let rel = logical.strip_prefix("/").unwrap_or(&logical).to_path_buf();
        // Create on EVERY backend so the dir is visible from anywhere.
        let mut ok_meta: Option<BackendMeta> = None;
        for (_tier, b) in self.state.router.all_backends() {
            if let Err(e) = b.create_dir(&rel) {
                warn!("mkdir on {}: {:?}", b.id(), e);
            } else {
                let _ = b.set_permissions(&rel, mode);
                if ok_meta.is_none() {
                    ok_meta = b.metadata(&rel).ok();
                }
            }
        }
        let Some(meta) = ok_meta else {
            reply.error(EIO);
            return;
        };
        let ino = self.state.inodes.lock().allocate(logical);
        let attr = self.state.make_attr(ino, &meta);
        reply.entry(&TTL, &attr, 0);
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let Some(logical) = self.state.path_for(parent, name) else {
            reply.error(ENOENT);
            return;
        };
        let Some((backend, bpath)) = self.state.resolve(&logical) else {
            reply.error(ENOENT);
            return;
        };
        if let Err(e) = backend.remove(&bpath) {
            reply.error(errno(&e));
            return;
        }
        if let Err(e) = self.state.index.remove(&logical) {
            warn!("index.remove {}: {:?}", logical.display(), e);
        }
        self.state.inodes.lock().remove(&logical);
        reply.ok();
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let Some(logical) = self.state.path_for(parent, name) else {
            reply.error(ENOENT);
            return;
        };
        let rel = logical.strip_prefix("/").unwrap_or(&logical).to_path_buf();
        let mut last_err: Option<FsError> = None;
        let mut removed_anywhere = false;
        for (_tier, b) in self.state.router.all_backends() {
            match b.remove(&rel) {
                Ok(()) => removed_anywhere = true,
                Err(e) => {
                    last_err = Some(e);
                }
            }
        }
        if !removed_anywhere {
            if let Some(e) = last_err {
                reply.error(errno(&e));
                return;
            }
        }
        self.state.inodes.lock().remove(&logical);
        reply.ok();
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
        let rel = dir_path.strip_prefix("/").unwrap_or(&dir_path).to_path_buf();

        // Merge entries from every backend into one logical view, deduping
        // (same name across backends shows up once).
        let mut seen: HashSet<String> = HashSet::new();
        let mut all: Vec<(u64, FileType, String)> = Vec::new();
        all.push((ino, FileType::Directory, ".".to_string()));
        all.push((ino, FileType::Directory, "..".to_string()));

        for (_tier, b) in self.state.router.all_backends() {
            let entries = match b.list_dir(&rel) {
                Ok(e) => e,
                Err(_) => continue,
            };
            for name in entries {
                if !seen.insert(name.clone()) {
                    continue;
                }
                let entry_path = dir_path.join(&name);
                if self.state.config.should_ignore(&entry_path) {
                    continue;
                }
                let entry_rel = entry_path.strip_prefix("/").unwrap_or(&entry_path).to_path_buf();
                let kind = b
                    .metadata(&entry_rel)
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
        let resolved = match fh.and_then(|h| self.state.fh(h)) {
            Some((b, p, _)) => (b, p),
            None => {
                let Some(logical) = self.state.inodes.lock().lookup_path(ino) else {
                    reply.error(ENOENT);
                    return;
                };
                let Some(r) = self.state.resolve(&logical) else {
                    reply.error(ENOENT);
                    return;
                };
                r
            }
        };
        let (backend, bpath) = resolved;

        if let Some(new_size) = size {
            if let Err(e) = backend.truncate(&bpath, new_size) {
                error!("truncate {}: {:?}", bpath.display(), e);
                reply.error(errno(&e));
                return;
            }
        }
        if let Some(new_mode) = mode {
            if let Err(e) = backend.set_permissions(&bpath, new_mode) {
                warn!("chmod {}: {:?}", bpath.display(), e);
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
            if let Err(e) = backend.set_times(&bpath, at, mt) {
                warn!("utimes {}: {:?}", bpath.display(), e);
            }
        }

        match backend.metadata(&bpath) {
            Ok(meta) => reply.attr(&TTL, &self.state.make_attr(ino, &meta)),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        let (fast_total, _fast_used, fast_free) = self.state.router.fast.capacity();
        let (slow_total, _slow_used, slow_free) = self.state.router.slow.capacity();
        let total = fast_total + slow_total;
        let free = fast_free + slow_free;
        let bsize = 4096u32;
        let blocks = total / bsize as u64;
        let bfree = free / bsize as u64;
        let files = self.state.index.count().unwrap_or(0);
        reply.statfs(blocks, bfree, bfree, files, 0, bsize, 255, bsize);
    }
}
