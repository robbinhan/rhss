use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    ReplyWrite, ReplyCreate, Request, FUSE_ROOT_ID,
};
use libc::{ENOENT, ENOSYS};
use crate::fs::FileSystem;
use tracing::{info, error, debug, warn};

const TTL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct FuseConfig {
    ignore_paths: HashSet<String>,
    ignore_patterns: Vec<String>,
}

impl Default for FuseConfig {
    fn default() -> Self {
        let mut ignore_paths = HashSet::new();
        ignore_paths.insert(".DS_Store".to_string());
        ignore_paths.insert(".hidden".to_string());
        ignore_paths.insert(".git".to_string());
        ignore_paths.insert("@executable_path".to_string());

        let ignore_patterns = vec![
            "._*".to_string(),  // macOS 元数据文件
        ];

        Self {
            ignore_paths,
            ignore_patterns,
        }
    }
}

impl FuseConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_ignore_paths(mut self, paths: Vec<String>) -> Self {
        self.ignore_paths.extend(paths);
        self
    }

    pub fn with_ignore_patterns(mut self, patterns: Vec<String>) -> Self {
        self.ignore_patterns.extend(patterns);
        self
    }

    pub fn should_ignore(&self, path: &Path) -> bool {
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            // 检查完全匹配
            if self.ignore_paths.contains(name) {
                return true;
            }

            // 检查模式匹配
            for pattern in &self.ignore_patterns {
                if pattern.ends_with('*') {
                    let prefix = &pattern[..pattern.len() - 1];
                    if name.starts_with(prefix) {
                        return true;
                    }
                }
            }

            // 检查单个字母
            if name.len() == 1 {
                return true;
            }
        }
        false
    }
}

struct FuseState {
    fs: Box<dyn FileSystem>,
    path_to_ino: Mutex<HashMap<PathBuf, u64>>,
    ino_to_path: Mutex<HashMap<u64, PathBuf>>,
    next_ino: Mutex<u64>,
    next_fh: Mutex<u64>,
    fh_to_path: Mutex<HashMap<u64, PathBuf>>,
    config: FuseConfig,
}

impl FuseState {
    fn new(fs: Box<dyn FileSystem>, config: FuseConfig) -> Self {
        let mut path_to_ino = HashMap::new();
        let mut ino_to_path = HashMap::new();
        let root_path = PathBuf::from("");
        path_to_ino.insert(root_path.clone(), FUSE_ROOT_ID);
        ino_to_path.insert(FUSE_ROOT_ID, root_path);

        Self {
            fs,
            path_to_ino: Mutex::new(path_to_ino),
            ino_to_path: Mutex::new(ino_to_path),
            next_ino: Mutex::new(FUSE_ROOT_ID + 1),
            next_fh: Mutex::new(1),
            fh_to_path: Mutex::new(HashMap::new()),
            config,
        }
    }

    fn make_file_attr(&self, ino: u64, size: u64, mode: u32, is_dir: bool) -> FileAttr {
        FileAttr {
            ino,
            size,
            blocks: (size + 511) / 512,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            crtime: SystemTime::now(),
            kind: if is_dir { FileType::Directory } else { FileType::RegularFile },
            perm: mode as u16,
            nlink: 1,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            flags: 0,
            blksize: 512,
        }
    }

    fn get_path(&self, parent: u64, name: Option<&OsStr>) -> Option<PathBuf> {
        let ino_to_path = self.ino_to_path.lock().unwrap();
        let parent_path = ino_to_path.get(&parent)?;
        let mut path = parent_path.clone();
        if let Some(name) = name {
            path.push(name);
        }
        debug!("get_path: parent={}, name={:?} -> {:?}", parent, name, path);
        Some(path)
    }

    fn allocate_ino(&self, path: PathBuf) -> u64 {
        let mut path_to_ino = self.path_to_ino.lock().unwrap();
        let mut ino_to_path = self.ino_to_path.lock().unwrap();
        let mut next_ino = self.next_ino.lock().unwrap();

        if let Some(&ino) = path_to_ino.get(&path) {
            debug!("allocate_ino: reusing ino={} for path={:?}", ino, path);
            return ino;
        }

        let ino = *next_ino;
        *next_ino += 1;
        path_to_ino.insert(path.clone(), ino);
        ino_to_path.insert(ino, path.clone());
        debug!("allocate_ino: new ino={} for path={:?}", ino, path);
        ino
    }

    fn allocate_fh(&self, path: PathBuf) -> u64 {
        let mut next_fh = self.next_fh.lock().unwrap();
        let mut fh_to_path = self.fh_to_path.lock().unwrap();
        let fh = *next_fh;
        *next_fh += 1;
        fh_to_path.insert(fh, path);
        fh
    }

    fn get_path_from_fh(&self, fh: u64) -> Option<PathBuf> {
        let fh_to_path = self.fh_to_path.lock().unwrap();
        fh_to_path.get(&fh).cloned()
    }

    fn release_fh(&self, fh: u64) {
        let mut fh_to_path = self.fh_to_path.lock().unwrap();
        fh_to_path.remove(&fh);
    }
}

pub struct FuseAdapter {
    state: Arc<FuseState>,
}

impl FuseAdapter {
    pub fn new(fs: Box<dyn FileSystem>, config: FuseConfig) -> Self {
        Self {
            state: Arc::new(FuseState::new(fs, config)),
        }
    }

    pub fn mount(self, mount_point: &Path) -> std::io::Result<()> {
        info!("Mounting FUSE filesystem at {:?}", mount_point);
        fuser::mount2(self, mount_point, &[])?;
        Ok(())
    }

    fn run_async<F, T>(&self, f: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(f)
    }
}

impl Filesystem for FuseAdapter {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let path = match self.state.get_path(parent, Some(name)) {
            Some(p) => p,
            None => {
                error!("lookup: failed to get path for parent={}, name={:?}", parent, name);
                reply.error(ENOENT);
                return;
            }
        };
        debug!("lookup: {:?}", path);
        let state = Arc::clone(&self.state);
        let path_clone = path.clone();
        let _result = self.run_async(async move {
            match state.fs.get_metadata(&path_clone).await {
                Ok(metadata) => {
                    let ino = state.allocate_ino(path_clone.clone());
                    let attr = state.make_file_attr(
                        ino,
                        metadata.size,
                        metadata.permissions,
                        metadata.is_dir,
                    );
                    debug!("lookup: success for path={:?}, ino={}", path_clone, ino);
                    reply.entry(&TTL, &attr, 0);
                }
                Err(e) => {
                    if state.config.should_ignore(&path_clone) {
                        debug!("lookup: ignoring special path: {:?}", path_clone);
                    } else {
                        error!("lookup error for path={:?}: {:?}", path_clone, e);
                    }
                    reply.error(ENOENT);
                }
            }
        });
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("getattr: ino={}", ino);
        if ino == FUSE_ROOT_ID {
            let attr = self.state.make_file_attr(ino, 0, 0o755, true);
            reply.attr(&TTL, &attr);
            return;
        }

        let path = match self.state.get_path(ino, None) {
            Some(p) => p,
            None => {
                error!("getattr: failed to get path for ino={}", ino);
                reply.error(ENOENT);
                return;
            }
        };

        let state = Arc::clone(&self.state);
        let path_clone = path.clone();
        let _result = self.run_async(async move {
            match state.fs.get_metadata(&path_clone).await {
                Ok(metadata) => {
                    let attr = state.make_file_attr(
                        ino,
                        metadata.size,
                        metadata.permissions,
                        metadata.is_dir,
                    );
                    debug!("getattr: success for path={:?}, ino={}", path_clone, ino);
                    reply.attr(&TTL, &attr);
                }
                Err(e) => {
                    error!("getattr error for path={:?}: {:?}", path_clone, e);
                    reply.error(ENOENT);
                }
            }
        });
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
        let path = match self.state.get_path(parent, Some(name)) {
            Some(p) => p,
            None => {
                error!("mkdir: failed to get path for parent={}, name={:?}", parent, name);
                reply.error(ENOENT);
                return;
            }
        };
        debug!("mkdir: {:?}, mode={:o}", path, mode);
        let state = Arc::clone(&self.state);
        let path_clone = path.clone();
        let _result = self.run_async(async move {
            match state.fs.create_directory(&path_clone).await {
                Ok(()) => {
                    let ino = state.allocate_ino(path_clone.clone());
                    let attr = state.make_file_attr(ino, 0, mode, true);
                    debug!("mkdir: success for path={:?}, ino={}", path_clone, ino);
                    reply.entry(&TTL, &attr, 0);
                }
                Err(e) => {
                    error!("mkdir error for path={:?}: {:?}", path_clone, e);
                    reply.error(ENOSYS);
                }
            }
        });
    }

    fn write(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        _offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let path = match self.state.get_path_from_fh(fh) {
            Some(p) => p,
            None => {
                error!("write: failed to get path for fh={}", fh);
                reply.error(ENOENT);
                return;
            }
        };
        debug!("write: {:?}, size={}", path, data.len());
        let state = Arc::clone(&self.state);
        let path_clone = path.clone();
        let data = data.to_vec();
        let _result = self.run_async(async move {
            match state.fs.write_file(&path_clone, &data).await {
                Ok(()) => {
                    debug!("write: success for path={:?}, size={}", path_clone, data.len());
                    reply.written(data.len() as u32);
                }
                Err(e) => {
                    error!("write error for path={:?}: {:?}", path_clone, e);
                    reply.error(ENOENT);
                }
            }
        });
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
        let path = match self.state.get_path_from_fh(fh) {
            Some(p) => p,
            None => {
                error!("read: failed to get path for fh={}", fh);
                reply.error(ENOENT);
                return;
            }
        };
        debug!("read: {:?}, offset={}, size={}", path, offset, size);
        let state = Arc::clone(&self.state);
        let path_clone = path.clone();
        let _result = self.run_async(async move {
            match state.fs.read_file(&path_clone).await {
                Ok(data) => {
                    let start = offset as usize;
                    let end = (offset as usize + size as usize).min(data.len());
                    if start < data.len() {
                        debug!("read: success for path={:?}, returning {} bytes", path_clone, end - start);
                        reply.data(&data[start..end]);
                    } else {
                        warn!("read: offset {} beyond file size {} for path={:?}", start, data.len(), path_clone);
                        reply.error(ENOENT);
                    }
                }
                Err(e) => {
                    error!("read error for path={:?}: {:?}", path_clone, e);
                    reply.error(ENOENT);
                }
            }
        });
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        let path = match self.state.get_path(parent, Some(name)) {
            Some(p) => p,
            None => {
                error!("unlink: failed to get path for parent={}, name={:?}", parent, name);
                reply.error(ENOENT);
                return;
            }
        };
        debug!("unlink: {:?}", path);
        let state = Arc::clone(&self.state);
        let path_clone = path.clone();
        let _result = self.run_async(async move {
            match state.fs.delete(&path_clone).await {
                Ok(()) => {
                    let mut path_to_ino = state.path_to_ino.lock().unwrap();
                    let mut ino_to_path = state.ino_to_path.lock().unwrap();
                    if let Some(ino) = path_to_ino.remove(&path_clone) {
                        ino_to_path.remove(&ino);
                        debug!("unlink: success for path={:?}, removed ino={}", path_clone, ino);
                    } else {
                        warn!("unlink: no inode found for path={:?}", path_clone);
                    }
                    reply.ok();
                }
                Err(e) => {
                    error!("unlink error for path={:?}: {:?}", path_clone, e);
                    reply.error(ENOSYS);
                }
            }
        });
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        let path = match self.state.get_path(parent, Some(name)) {
            Some(p) => p,
            None => {
                error!("rmdir: failed to get path for parent={}, name={:?}", parent, name);
                reply.error(ENOENT);
                return;
            }
        };
        debug!("rmdir: {:?}", path);
        let state = Arc::clone(&self.state);
        let path_clone = path.clone();
        let _result = self.run_async(async move {
            match state.fs.delete(&path_clone).await {
                Ok(()) => {
                    let mut path_to_ino = state.path_to_ino.lock().unwrap();
                    let mut ino_to_path = state.ino_to_path.lock().unwrap();
                    if let Some(ino) = path_to_ino.remove(&path_clone) {
                        ino_to_path.remove(&ino);
                        debug!("rmdir: success for path={:?}, removed ino={}", path_clone, ino);
                    } else {
                        warn!("rmdir: no inode found for path={:?}", path_clone);
                    }
                    reply.ok();
                }
                Err(e) => {
                    error!("rmdir error for path={:?}: {:?}", path_clone, e);
                    reply.error(ENOSYS);
                }
            }
        });
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let path = match self.state.get_path(ino, None) {
            Some(p) => p,
            None => {
                error!("readdir: failed to get path for ino={}", ino);
                reply.error(ENOENT);
                return;
            }
        };
        debug!("readdir: {:?}, offset={}", path, offset);
        let state = Arc::clone(&self.state);
        let path_clone = path.clone();
        let _result = self.run_async(async move {
            match state.fs.list_directory(&path_clone).await {
                Ok(entries) => {
                    let mut entries_vec = vec![
                        (ino, FileType::Directory, ".".to_string()),
                        (ino, FileType::Directory, "..".to_string()),
                    ];

                    for name in entries {
                        let entry_path = path_clone.join(&name);
                        let entry_ino = state.allocate_ino(entry_path.clone());
                        let entry_type = match state.fs.get_metadata(&entry_path).await {
                            Ok(metadata) => {
                                if metadata.is_dir {
                                    FileType::Directory
                                } else {
                                    FileType::RegularFile
                                }
                            }
                            Err(_) => FileType::RegularFile,
                        };
                        debug!("readdir: found entry name={}, ino={}, type={:?}", name, entry_ino, entry_type);
                        entries_vec.push((entry_ino, entry_type, name));
                    }

                    for (i, (entry_ino, entry_type, name)) in entries_vec.into_iter().enumerate().skip(offset as usize) {
                        if reply.add(entry_ino, (i + 1) as i64, entry_type, &name) {
                            break;
                        }
                    }
                    debug!("readdir: success for path={:?}", path_clone);
                    reply.ok();
                }
                Err(e) => {
                    error!("readdir error for path={:?}: {:?}", path_clone, e);
                    reply.error(ENOSYS);
                }
            }
        });
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
        let path = match self.state.get_path(parent, Some(name)) {
            Some(p) => p,
            None => {
                error!("create: failed to get path for parent={}, name={:?}", parent, name);
                reply.error(ENOENT);
                return;
            }
        };
        debug!("create: {:?}, mode={:o}", path, mode);
        let state = Arc::clone(&self.state);
        let path_clone = path.clone();
        let _result = self.run_async(async move {
            match state.fs.create_file(&path_clone).await {
                Ok(()) => {
                    let ino = state.allocate_ino(path_clone.clone());
                    let fh = state.allocate_fh(path_clone.clone());
                    let attr = state.make_file_attr(ino, 0, mode, false);
                    debug!("create: success for path={:?}, ino={}, fh={}", path_clone, ino, fh);
                    reply.created(&TTL, &attr, 0, fh, 0);
                }
                Err(e) => {
                    error!("create error for path={:?}: {:?}", path_clone, e);
                    reply.error(ENOSYS);
                }
            }
        });
    }

    fn open(
        &mut self,
        _req: &Request,
        _ino: u64,
        _flags: i32,
        reply: fuser::ReplyOpen,
    ) {
        let path = match self.state.get_path(_ino, None) {
            Some(p) => p,
            None => {
                error!("open: failed to get path for ino={}", _ino);
                reply.error(ENOENT);
                return;
            }
        };
        debug!("open: {:?}", path);
        let state = Arc::clone(&self.state);
        let path_clone = path.clone();
        let _result = self.run_async(async move {
            match state.fs.get_metadata(&path_clone).await {
                Ok(_) => {
                    let fh = state.allocate_fh(path_clone.clone());
                    debug!("open: success for path={:?}, fh={}", path_clone, fh);
                    reply.opened(fh, 0);  // flags=0
                }
                Err(e) => {
                    error!("open error for path={:?}: {:?}", path_clone, e);
                    reply.error(ENOENT);
                }
            }
        });
    }

    fn release(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        debug!("release: fh={}", fh);
        self.state.release_fh(fh);
        reply.ok();
    }
} 