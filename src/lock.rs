use std::fs::{File, OpenOptions, Permissions};
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tracing::{info, warn, error};

/// 锁文件信息
#[derive(Debug, Serialize, Deserialize)]
struct LockInfo {
    /// 进程 ID
    pid: u32,
    /// 进程启动时间（用于验证 PID 是否被重用）
    start_time: u64,
    /// 主机名
    hostname: String,
    /// 锁创建时间
    created_at: u64,
    /// 程序版本
    version: String,
}

/// 存储锁管理器
pub struct StorageLock {
    /// 锁文件路径
    lock_files: Vec<PathBuf>,
    /// 存储目录路径
    storage_dirs: Vec<PathBuf>,
    /// 原始目录权限（用于恢复）
    original_permissions: Vec<Option<Permissions>>,
    /// 是否已经获取锁
    locked: bool,
}

impl StorageLock {
    /// 创建新的存储锁
    pub fn new(hot_path: &Path, cold_path: &Path) -> Self {
        let lock_files = vec![
            hot_path.join(".rhss.lock"),
            cold_path.join(".rhss.lock"),
        ];
        
        let storage_dirs = vec![
            hot_path.to_path_buf(),
            cold_path.to_path_buf(),
        ];
        
        let original_permissions = vec![None, None];
        
        Self {
            lock_files,
            storage_dirs,
            original_permissions,
            locked: false,
        }
    }
    
    /// 尝试获取锁
    pub fn try_lock(&mut self) -> Result<()> {
        if self.locked {
            return Ok(());
        }
        
        // 检查所有锁文件
        for lock_file in &self.lock_files {
            self.check_and_clean_stale_lock(lock_file)?;
            
            // 尝试创建锁文件
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(lock_file)
            {
                Ok(mut file) => {
                    // 写入锁信息
                    let lock_info = LockInfo {
                        pid: process::id(),
                        start_time: get_process_start_time(),
                        hostname: whoami::hostname(),
                        created_at: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                        version: env!("CARGO_PKG_VERSION").to_string(),
                    };
                    
                    let json = serde_json::to_string_pretty(&lock_info)?;
                    file.write_all(json.as_bytes())?;
                    file.sync_all()?;
                    
                    info!("成功获取存储锁: {:?}", lock_file);
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    // 锁文件已存在，读取信息
                    if let Ok(info) = self.read_lock_info(lock_file) {
                        return Err(anyhow!(
                            "存储目录已被锁定！\n\
                            锁定进程: PID {} @ {}\n\
                            锁定时间: {} 秒前\n\
                            锁文件: {:?}\n\
                            \n\
                            如果确定该进程已经退出，可以手动删除锁文件或使用 --force 参数强制启动",
                            info.pid,
                            info.hostname,
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs() - info.created_at,
                            lock_file
                        ));
                    } else {
                        return Err(anyhow!("存储目录已被锁定，但无法读取锁信息: {:?}", lock_file));
                    }
                }
                Err(e) => {
                    return Err(anyhow!("创建锁文件失败 {:?}: {}", lock_file, e));
                }
            }
        }
        
        // 修改目录权限，限制访问
        for (i, dir) in self.storage_dirs.iter().enumerate() {
            if dir.exists() {
                // 保存原始权限
                let metadata = std::fs::metadata(dir)?;
                self.original_permissions[i] = Some(metadata.permissions());
                
                // 设置新权限：只有所有者可以读写执行 (0o700)
                let mut new_perms = metadata.permissions();
                new_perms.set_mode(0o700);
                std::fs::set_permissions(dir, new_perms)?;
                
                info!("已限制目录访问权限: {:?} (mode=0o700)", dir);
            }
        }
        
        self.locked = true;
        Ok(())
    }
    
    /// 强制获取锁（清理现有锁）
    pub fn force_lock(&mut self) -> Result<()> {
        if self.locked {
            return Ok(());
        }
        
        // 强制删除所有锁文件
        for lock_file in &self.lock_files {
            if lock_file.exists() {
                warn!("强制删除现有锁文件: {:?}", lock_file);
                std::fs::remove_file(lock_file)?;
            }
        }
        
        // 重新获取锁
        self.try_lock()
    }
    
    /// 检查是否已经获取锁
    pub fn is_locked(&self) -> bool {
        self.locked
    }
    
    /// 释放锁
    pub fn unlock(&mut self) -> Result<()> {
        if !self.locked {
            return Ok(());
        }
        
        // 恢复目录原始权限
        for (i, dir) in self.storage_dirs.iter().enumerate() {
            if dir.exists() {
                if let Some(ref original_perms) = self.original_permissions[i] {
                    std::fs::set_permissions(dir, original_perms.clone())?;
                    info!("已恢复目录原始权限: {:?}", dir);
                } else {
                    // 如果没有保存原始权限，恢复为默认权限 (0o755)
                    let mut default_perms = std::fs::metadata(dir)?.permissions();
                    default_perms.set_mode(0o755);
                    std::fs::set_permissions(dir, default_perms)?;
                    info!("已恢复目录默认权限: {:?} (mode=0o755)", dir);
                }
            }
        }
        
        // 删除锁文件
        for lock_file in &self.lock_files {
            if lock_file.exists() {
                // 验证是否是我们的锁
                if let Ok(info) = self.read_lock_info(lock_file) {
                    if info.pid == process::id() {
                        std::fs::remove_file(lock_file)?;
                        info!("已释放存储锁: {:?}", lock_file);
                    } else {
                        warn!("锁文件不属于当前进程，跳过: {:?}", lock_file);
                    }
                }
            }
        }
        
        self.locked = false;
        Ok(())
    }
    
    /// 检查并清理过期的锁
    fn check_and_clean_stale_lock(&self, lock_file: &Path) -> Result<()> {
        if !lock_file.exists() {
            return Ok(());
        }
        
        match self.read_lock_info(lock_file) {
            Ok(info) => {
                // 检查进程是否还在运行
                if !is_process_running(info.pid) {
                    warn!("检测到过期锁文件（进程 {} 已退出），正在清理...", info.pid);
                    std::fs::remove_file(lock_file)?;
                    return Ok(());
                }
                
                // 检查锁是否太旧（超过24小时）
                let age = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs() - info.created_at;
                    
                if age > 86400 {
                    warn!("检测到超过24小时的锁文件，可能是异常情况，正在清理...");
                    std::fs::remove_file(lock_file)?;
                    return Ok(());
                }
            }
            Err(e) => {
                warn!("无法读取锁文件信息，可能已损坏: {:?}", e);
                std::fs::remove_file(lock_file)?;
                return Ok(());
            }
        }
        
        Ok(())
    }
    
    /// 读取锁信息
    fn read_lock_info(&self, lock_file: &Path) -> Result<LockInfo> {
        let mut file = File::open(lock_file)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let info: LockInfo = serde_json::from_str(&contents)?;
        Ok(info)
    }
}

impl Drop for StorageLock {
    fn drop(&mut self) {
        if self.locked {
            if let Err(e) = self.unlock() {
                error!("释放存储锁失败: {}", e);
            }
        }
    }
}

/// 检查进程是否在运行
#[cfg(unix)]
fn is_process_running(pid: u32) -> bool {
    // 发送信号 0 来检查进程是否存在
    unsafe {
        libc::kill(pid as i32, 0) == 0
    }
}

#[cfg(not(unix))]
fn is_process_running(_pid: u32) -> bool {
    // Windows 上的实现会更复杂，这里简化处理
    false
}

/// 获取进程启动时间
fn get_process_start_time() -> u64 {
    // 简化实现，使用当前时间
    // 实际应该读取 /proc/[pid]/stat 或使用系统 API
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_lock_unlock() {
        let temp_dir = TempDir::new().unwrap();
        let hot_path = temp_dir.path().join("hot");
        let cold_path = temp_dir.path().join("cold");
        
        std::fs::create_dir_all(&hot_path).unwrap();
        std::fs::create_dir_all(&cold_path).unwrap();
        
        let mut lock = StorageLock::new(&hot_path, &cold_path);
        
        // 第一次加锁应该成功
        assert!(lock.try_lock().is_ok());
        
        // 第二次加锁应该成功（因为已经持有锁）
        assert!(lock.try_lock().is_ok());
        
        // 解锁
        assert!(lock.unlock().is_ok());
        
        // 解锁后可以重新加锁
        assert!(lock.try_lock().is_ok());
    }
    
    #[test]
    fn test_lock_conflict() {
        let temp_dir = TempDir::new().unwrap();
        let hot_path = temp_dir.path().join("hot");
        let cold_path = temp_dir.path().join("cold");
        
        std::fs::create_dir_all(&hot_path).unwrap();
        std::fs::create_dir_all(&cold_path).unwrap();
        
        let mut lock1 = StorageLock::new(&hot_path, &cold_path);
        let mut lock2 = StorageLock::new(&hot_path, &cold_path);
        
        // 第一个锁成功
        assert!(lock1.try_lock().is_ok());
        
        // 第二个锁失败
        assert!(lock2.try_lock().is_err());
        
        // 释放第一个锁
        assert!(lock1.unlock().is_ok());
        
        // 现在第二个锁可以成功
        assert!(lock2.try_lock().is_ok());
    }
}
