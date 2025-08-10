use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    ReplyWrite, ReplyCreate, Request, FUSE_ROOT_ID, MountOption,
};
use libc::{ENOENT, ENOSYS};
use crate::fs::FileSystem;
use tracing::{info, error, debug, warn};
use tokio::runtime::Handle;

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
    running: Arc<AtomicBool>,
    runtime_handle: Handle,
}

impl FuseState {
    fn new(fs: Box<dyn FileSystem>, config: FuseConfig, running: Arc<AtomicBool>, runtime_handle: Handle) -> Self {
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
            running,
            runtime_handle,
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

    // 添加公共方法来检查运行状态
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

#[derive(Clone)]
pub struct FuseAdapter {
    state: Arc<FuseState>,
}

impl FuseAdapter {
    pub fn new(fs: Box<dyn FileSystem>, config: FuseConfig, runtime_handle: Handle) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        Self {
            state: Arc::new(FuseState::new(fs, config, running, runtime_handle)),
        }
    }

    pub fn mount(&self, mount_point: &Path) -> std::io::Result<()> {
        info!("Mounting FUSE filesystem at {:?}", mount_point);
        let options = [
            MountOption::CUSTOM("volname=RHSS_Mount".to_string()),
            MountOption::CUSTOM("local".to_string()),
            MountOption::FSName("rhss_fs".to_string()),
            MountOption::DefaultPermissions,
            MountOption::CUSTOM("noapplex".to_string()),
            // 添加自动卸载选项，当文件系统进程退出时自动卸载
            MountOption::AutoUnmount,
        ];
        fuser::mount2(self.clone(), mount_point, &options)?;
        Ok(())
    }

    fn run_async<F, T>(&self, f: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        self.state.runtime_handle.block_on(f)
    }

    pub fn stop(&self) {
        info!("正在停止文件系统...");
        
        // 1. 设置停止标志，阻止新的操作
        self.state.running.store(false, Ordering::SeqCst);
        info!("已设置停止标志，不再接受新的文件系统操作");
        
        // 2. 等待所有文件句柄被释放
        let mut retry_count = 0;
        const MAX_RETRIES: u32 = 30;  // 增加到3秒
        const RETRY_INTERVAL_MS: u64 = 100;
        
        while retry_count < MAX_RETRIES {
            let fh_count = self.state.fh_to_path.lock().unwrap().len();
            if fh_count == 0 {
                info!("所有文件句柄已释放");
                break;
            }
            
            // 前几次重试时显示详细信息
            if retry_count < 5 || retry_count % 10 == 0 {
                info!("等待 {} 个文件句柄释放... (尝试 {}/{})", 
                      fh_count, retry_count + 1, MAX_RETRIES);
            }
            
            std::thread::sleep(std::time::Duration::from_millis(RETRY_INTERVAL_MS));
            retry_count += 1;
        }
        
        if retry_count >= MAX_RETRIES {
            let remaining_fh = self.state.fh_to_path.lock().unwrap().len();
            if remaining_fh > 0 {
                warn!("等待文件句柄释放超时，仍有 {} 个句柄未释放", remaining_fh);
                
                // 强制清理剩余的文件句柄
                let mut fh_to_path = self.state.fh_to_path.lock().unwrap();
                warn!("强制清理 {} 个未释放的文件句柄", fh_to_path.len());
                fh_to_path.clear();
            }
        }
        
        // 3. 清理 inode 映射表
        {
            let mut path_to_ino = self.state.path_to_ino.lock().unwrap();
            let mut ino_to_path = self.state.ino_to_path.lock().unwrap();
            info!("清理 inode 映射表（{} 个条目）", path_to_ino.len());
            path_to_ino.clear();
            ino_to_path.clear();
        }
        
        info!("文件系统停止完成");
    }

    // 添加一个公共方法来委托给 state.is_running()
    pub fn is_running(&self) -> bool {
        self.state.is_running()
    }
}

impl Filesystem for FuseAdapter {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if !self.state.running.load(Ordering::SeqCst) {
            reply.error(ENOSYS);
            return;
        }

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
                        error!(
                            parent_ino = parent,
                            name = ?name,
                            path = ?path_clone, 
                            error = ?e, 
                            "lookup error in fs.get_metadata"
                        );
                    }
                    reply.error(ENOENT);
                }
            }
        });
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if !self.state.running.load(Ordering::SeqCst) {
            reply.error(ENOSYS);
            return;
        }
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

    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        debug!(ino, ?mode, ?uid, ?gid, ?size, ?atime, ?mtime, ?fh, ?flags, "setattr called");

        let path = match fh.and_then(|h| self.state.get_path_from_fh(h)) {
            Some(p) => p,
            None => match self.state.get_path(ino, None) {
                Some(p) => p,
                None => {
                    error!("setattr: failed to get path for ino={}", ino);
                    reply.error(libc::ENOENT);
                    return;
                }
            }
        };

        let state = Arc::clone(&self.state);
        let path_clone = path.clone();

        let _result = self.run_async(async move {
            match state.fs.get_metadata(&path_clone).await {
                Ok(metadata) => {
                    // 完全忽略 setattr 请求的参数，仅返回当前获取的元数据
                    let attr = state.make_file_attr(
                        ino,
                        metadata.size,
                        metadata.permissions,
                        metadata.is_dir,
                    );
                    debug!("setattr: replying with UNMODIFIED attrs for path={:?}, ino={}", path_clone, ino);
                    reply.attr(&TTL, &attr);
                }
                Err(e) => {
                    error!("setattr: getattr error for path={:?}: {:?}", path_clone, e);
                    reply.error(libc::ENOENT);
                }
            }
        });
    }
} 