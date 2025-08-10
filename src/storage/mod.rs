use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::os::unix::fs::MetadataExt;
use tracing::{debug, error, info};
use crate::error::{Result, FsError};
use crate::fs::{FileSystem, FileMetadata};
use crate::cache::{FileLocationCache, StorageLocation};
use std::collections::HashSet;
use rustix::fs::{Mode, OFlags};
use rustix::process::{Gid, Uid};
use std::time::SystemTime;
use std::os::fd::{AsRawFd, RawFd};
use std::fs::File;
use std::io::{Read, Write};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StorageTier {
    Hot,    // SSD
    Warm,   // 混合
    Cold,   // HDD
}

#[async_trait]
pub trait Storage: Send + Sync {
    async fn read(&self, path: &Path) -> Result<Vec<u8>>;
    async fn write(&self, path: &Path, data: &[u8]) -> Result<()>;
    async fn delete(&self, path: &Path) -> Result<()>;
    async fn exists(&self, path: &Path) -> Result<bool>;
}

pub struct HybridStorage {
    hot_storage: Box<dyn FileSystem>,
    cold_storage: Box<dyn FileSystem>,
    threshold: u64,
    cache: Arc<FileLocationCache>,
}

impl std::fmt::Debug for HybridStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HybridStorage")
            .field("threshold", &self.threshold)
            .field("cache_stats", &self.cache.stats().to_string())
            .finish()
    }
}

impl HybridStorage {
    pub fn new(hot_storage: Box<dyn FileSystem>, cold_storage: Box<dyn FileSystem>, threshold: u64) -> Self {
        // 创建缓存：TTL 300秒（5分钟），最多10000个条目
        let cache = Arc::new(FileLocationCache::new(300, 10000));
        
        Self {
            hot_storage,
            cold_storage,
            threshold,
            cache,
        }
    }
    
    /// 获取缓存统计信息
    pub fn cache_stats(&self) -> String {
        self.cache.stats().to_string()
    }
    
    /// 获取文件元数据（公开方法）
    pub async fn get_file_metadata(&self, path: &Path) -> Result<FileMetadata> {
        self.get_metadata(path).await
    }

    async fn get_storage<'a>(&'a self, path: &'a Path) -> &'a Box<dyn FileSystem> {
        match self.get_metadata(path).await {
            Ok(metadata) if metadata.size >= self.threshold => &self.cold_storage,
            _ => &self.hot_storage,
        }
    }
    
    /// 检查文件是否需要迁移
    async fn check_migration_needed(&self, path: &Path) -> Option<(StorageLocation, StorageLocation, u64)> {
        // 获取文件元数据
        let metadata = match self.get_metadata(path).await {
            Ok(m) => m,
            Err(_) => return None,
        };
        
        let size = metadata.size;
        let expected_location = if size >= self.threshold {
            StorageLocation::Cold
        } else {
            StorageLocation::Hot
        };
        
        // 检查文件实际位置
        let actual_location = if self.hot_storage.exists(path).await.unwrap_or(false) {
            if self.cold_storage.exists(path).await.unwrap_or(false) {
                StorageLocation::Both
            } else {
                StorageLocation::Hot
            }
        } else if self.cold_storage.exists(path).await.unwrap_or(false) {
            StorageLocation::Cold
        } else {
            return None;
        };
        
        // 如果位置不符合预期，需要迁移
        if actual_location != expected_location && actual_location != StorageLocation::Both {
            Some((actual_location, expected_location, size))
        } else {
            None
        }
    }
    
    /// 迁移文件到正确的存储层
    pub async fn migrate_file(&self, path: &Path) -> Result<bool> {
        // 检查是否需要迁移
        let migration_info = match self.check_migration_needed(path).await {
            Some(info) => info,
            None => return Ok(false),
        };
        
        let (from_location, to_location, size) = migration_info;
        
        debug!(
            "迁移文件 {:?}: 从 {:?} 到 {:?} (大小: {} bytes, 阈值: {} bytes)",
            path, from_location, to_location, size, self.threshold
        );
        
        // 读取文件内容
        let data = match from_location {
            StorageLocation::Hot => self.hot_storage.read_file(path).await?,
            StorageLocation::Cold => self.cold_storage.read_file(path).await?,
            StorageLocation::Both => {
                // 如果两边都有，从 hot 读取
                self.hot_storage.read_file(path).await?
            }
        };
        
        // 写入到目标存储
        match to_location {
            StorageLocation::Hot => {
                self.hot_storage.write_file(path, &data).await?;
                self.cold_storage.delete(path).await.ok();
            }
            StorageLocation::Cold => {
                self.cold_storage.write_file(path, &data).await?;
                self.hot_storage.delete(path).await.ok();
            }
            StorageLocation::Both => {
                // 不应该迁移到 Both
                return Ok(false);
            }
        }
        
        // 更新缓存
        self.cache.move_location(path, from_location, to_location);
        
        info!(
            "成功迁移文件 {:?} 从 {:?} 到 {:?}",
            path, from_location, to_location
        );
        
        Ok(true)
    }
    
    /// 批量检查并迁移目录中的文件
    pub async fn migrate_directory(&self, dir_path: &Path) -> Result<(usize, usize)> {
        let entries = self.list_directory(dir_path).await?;
        let mut checked = 0;
        let mut migrated = 0;
        
        for entry in entries {
            let file_path = dir_path.join(&entry);
            checked += 1;
            
            if self.migrate_file(&file_path).await? {
                migrated += 1;
            }
        }
        
        if migrated > 0 {
            info!(
                "目录 {:?} 迁移完成: 检查了 {} 个文件，迁移了 {} 个",
                dir_path, checked, migrated
            );
        }
        
        Ok((checked, migrated))
    }
}

#[async_trait]
impl FileSystem for HybridStorage {
    async fn list_directory<'a>(&'a self, path: &'a Path) -> Result<Vec<String>> {
        let hot_entries = self.hot_storage.list_directory(path).await?;
        let cold_entries = self.cold_storage.list_directory(path).await?;

        // 批量更新缓存
        let mut cache_updates = Vec::new();
        
        // 记录热存储中的文件
        for entry in &hot_entries {
            let entry_path = path.join(entry);
            cache_updates.push((entry_path, StorageLocation::Hot, None));
        }
        
        // 记录冷存储中的文件
        for entry in &cold_entries {
            let entry_path = path.join(entry);
            // 如果文件在两个存储中都存在
            if hot_entries.contains(entry) {
                cache_updates.push((entry_path, StorageLocation::Both, None));
            } else {
                cache_updates.push((entry_path, StorageLocation::Cold, None));
            }
        }
        
        // 批量更新缓存
        if !cache_updates.is_empty() {
            self.cache.batch_update(cache_updates);
        }

        // 计算去重后的并集
        let mut combined_set: HashSet<String> = hot_entries.into_iter().collect();
        combined_set.extend(cold_entries);

        let unique_entries: Vec<String> = combined_set.into_iter().collect();

        Ok(unique_entries)
    }

    async fn get_metadata<'a>(&'a self, path: &'a Path) -> Result<FileMetadata> {
        if let Ok(metadata) = self.hot_storage.get_metadata(path).await {
            Ok(metadata)
        } else {
            self.cold_storage.get_metadata(path).await
        }
    }

    async fn read_file<'a>(&'a self, path: &'a Path) -> Result<Vec<u8>> {
        // 先检查缓存
        if let Some(location) = self.cache.get(path) {
            debug!("使用缓存位置: {:?} -> {:?}", path, location);
            
            let result = match location {
                StorageLocation::Hot => self.hot_storage.read_file(path).await,
                StorageLocation::Cold => self.cold_storage.read_file(path).await,
                StorageLocation::Both => {
                    // 优先从 hot 读取
                    match self.hot_storage.read_file(path).await {
                        Ok(data) => Ok(data),
                        Err(_) => self.cold_storage.read_file(path).await,
                    }
                }
            };
            
            if result.is_ok() {
                return result;
            } else {
                // 缓存失效，删除条目
                self.cache.remove(path);
            }
        }
        
        // 缓存未命中，尝试从两个存储读取
        if let Ok(data) = self.hot_storage.read_file(path).await {
            debug!("read_file: 在热存储中找到 {:?}", path);
            self.cache.set(path, StorageLocation::Hot, Some(data.len() as u64));
            return Ok(data);
        }
        
        if let Ok(data) = self.cold_storage.read_file(path).await {
            debug!("read_file: 在冷存储中找到 {:?}", path);
            self.cache.set(path, StorageLocation::Cold, Some(data.len() as u64));
            return Ok(data);
        }
        
        Err(FsError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "文件不存在"
        )))
    }

    async fn write_file<'a>(&'a self, path: &'a Path, data: &'a [u8]) -> Result<()> {
        let (storage, location) = if data.len() as u64 >= self.threshold {
            (&self.cold_storage, StorageLocation::Cold)
        } else {
            (&self.hot_storage, StorageLocation::Hot)
        };
        
        // 如果文件存在于另一个存储中，先删除它（这就是自动迁移）
        let other_storage = if data.len() as u64 >= self.threshold {
            &self.hot_storage
        } else {
            &self.cold_storage
        };
        
        if other_storage.exists(path).await? {
            debug!("write_file: 自动迁移文件 {:?} 到 {:?}", path, location);
            other_storage.delete(path).await?;
        }
        
        // 写入文件
        storage.write_file(path, data).await?;
        
        // 更新缓存
        self.cache.set(path, location, Some(data.len() as u64));
        debug!("write_file: 更新缓存 {:?} -> {:?}", path, location);
        
        Ok(())
    }

    async fn create_file<'a>(&'a self, path: &'a Path) -> Result<()> {
        // 创建文件时，先在 hot 存储中创建
        self.hot_storage.create_file(path).await
    }

    async fn create_directory<'a>(&'a self, path: &'a Path) -> Result<()> {
        // 目录只在 hot 存储中创建
        self.hot_storage.create_directory(path).await
    }

    async fn delete<'a>(&'a self, path: &'a Path) -> Result<()> {
        let hot_result = self.hot_storage.delete(path).await;
        let cold_result = self.cold_storage.delete(path).await;
        
        // 如果至少有一个成功，清理缓存
        if hot_result.is_ok() || cold_result.is_ok() {
            self.cache.remove(path);
            debug!("delete: 清理缓存 {:?}", path);
        }
        
        hot_result.and(cold_result)
    }

    async fn exists<'a>(&'a self, path: &'a Path) -> Result<bool> {
        Ok(self.hot_storage.exists(path).await? || self.cold_storage.exists(path).await?)
    }
}

#[derive(Debug, Clone)]
pub struct LocalStorage {
    base_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct PosixStorage {
    base_path: PathBuf,
    uid: Uid,
    gid: Gid,
    mode: Mode,
    file_cache: Arc<Mutex<HashSet<PathBuf>>>,
}

impl LocalStorage {
    pub fn new(root: PathBuf) -> Self {
        Self { base_path: root }
    }
}

impl PosixStorage {
    pub fn new(base_path: PathBuf, uid: Uid, gid: Gid, mode: Mode) -> Self {
        Self {
            base_path,
            uid,
            gid,
            mode,
            file_cache: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

#[async_trait]
impl FileSystem for LocalStorage {
    async fn list_directory<'a>(&'a self, path: &'a Path) -> Result<Vec<String>> {
        let full_path = self.base_path.join(path);
        let mut entries = Vec::new();
        
        // 如果目录不存在，返回空列表
        if !full_path.exists() {
            return Ok(entries);
        }

        let mut dir = tokio::fs::read_dir(&full_path)
            .await
            .map_err(|e| FsError::Io(e))?;
        while let Some(entry) = dir.next_entry().await.map_err(|e| FsError::Io(e))? {
            if let Some(name) = entry.file_name().to_str() {
                // 忽略 macOS 的元数据文件
                if !name.starts_with("._") {
                    entries.push(name.to_string());
                } else {
                    debug!("Ignoring macOS metadata file in list_directory: {}", name);
                }
            }
        }
        Ok(entries)
    }

    async fn get_metadata<'a>(&'a self, path: &'a Path) -> Result<FileMetadata> {
        // 忽略 macOS 的元数据文件查询
        if let Some(name) = path.file_name() {
            if let Some(name_str) = name.to_str() {
                if name_str.starts_with("._") {
                    debug!("Ignoring macOS metadata file in get_metadata: {}", name_str);
                    return Err(FsError::NotFound(format!("Ignoring macOS metadata file: {}", name_str)));
                }
            }
        }

        let full_path = self.base_path.join(path);
        let metadata = tokio::fs::metadata(&full_path)
            .await
            .map_err(|e| FsError::Io(e))?;
        Ok(FileMetadata {
            size: metadata.len(),
            is_dir: metadata.is_dir(),
            permissions: metadata.mode(),
            modified: metadata.modified().map_err(|e| FsError::Io(e))?,
        })
    }

    async fn read_file<'a>(&'a self, path: &'a Path) -> Result<Vec<u8>> {
        let full_path = self.base_path.join(path);
        tokio::fs::read(&full_path)
            .await
            .map_err(|e| FsError::Io(e))
    }

    async fn write_file<'a>(&'a self, path: &'a Path, data: &'a [u8]) -> Result<()> {
        let full_path = self.base_path.join(path);
        debug!("write_file: writing to {:?}, size={}", full_path, data.len());
        if let Some(parent) = full_path.parent() {
            debug!("write_file: creating parent directory {:?}", parent);
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| {
                    error!("write_file: failed to create parent directory: {:?}", e);
                    FsError::Io(e)
                })?;
        }
        tokio::fs::write(&full_path, data)
            .await
            .map_err(|e| {
                error!("write_file: failed to write file: {:?}", e);
                FsError::Io(e)
            })
    }

    async fn create_file<'a>(&'a self, path: &'a Path) -> Result<()> {
        let full_path = self.base_path.join(path);
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| FsError::Io(e))?;
        }
        tokio::fs::File::create(&full_path)
            .await
            .map_err(|e| FsError::Io(e))?;
        Ok(())
    }

    async fn create_directory<'a>(&'a self, path: &'a Path) -> Result<()> {
        let full_path = self.base_path.join(path);
        debug!("create_directory: creating directory at {:?}", full_path);
        tokio::fs::create_dir_all(&full_path)
            .await
            .map_err(|e| {
                error!("create_directory error for path={:?}: {:?}", full_path, e);
                FsError::Io(e)
            })
    }

    async fn delete<'a>(&'a self, path: &'a Path) -> Result<()> {
        let full_path = self.base_path.join(path);
        if full_path.is_dir() {
            tokio::fs::remove_dir_all(&full_path)
                .await
                .map_err(|e| FsError::Io(e))
        } else {
            tokio::fs::remove_file(&full_path)
                .await
                .map_err(|e| FsError::Io(e))
        }
    }

    async fn exists<'a>(&'a self, path: &'a Path) -> Result<bool> {
        let full_path = self.base_path.join(path);
        Ok(full_path.exists())
    }
}

#[async_trait]
impl FileSystem for PosixStorage {
    async fn list_directory<'a>(&'a self, path: &'a Path) -> Result<Vec<String>> {
        let full_path = self.base_path.join(path);
        let mut entries = Vec::new();
        
        let mut dir = tokio::fs::read_dir(&full_path).await?;
        while let Some(entry) = dir.next_entry().await? {
            if let Some(name) = entry.file_name().to_str() {
                entries.push(name.to_string());
            }
        }
        
        Ok(entries)
    }

    async fn get_metadata<'a>(&'a self, path: &'a Path) -> Result<FileMetadata> {
        let full_path = self.base_path.join(path);
        let metadata = tokio::fs::metadata(&full_path).await?;
        
        Ok(FileMetadata {
            size: metadata.len(),
            is_dir: metadata.is_dir(),
            permissions: metadata.mode(),
            modified: metadata.modified()?,
        })
    }

    async fn read_file<'a>(&'a self, path: &'a Path) -> Result<Vec<u8>> {
        let full_path = self.base_path.join(path);
        tokio::fs::read(&full_path).await.map_err(Into::into)
    }

    async fn write_file<'a>(&'a self, path: &'a Path, data: &'a [u8]) -> Result<()> {
        let full_path = self.base_path.join(path);
        debug!("write_file: writing to {:?}, size={}", full_path, data.len());
        
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        
        tokio::fs::write(&full_path, data).await?;
        
        // 设置文件权限和所有者
        let fd = rustix::fs::open(
            &full_path,
            OFlags::RDWR,
            self.mode,
        ).map_err(Into::into).map_err(FsError::Io)?;
        
        rustix::fs::fchown(&fd, Some(self.uid), Some(self.gid)).map_err(Into::into).map_err(FsError::Io)?;
        
        Ok(())
    }

    async fn create_file<'a>(&'a self, path: &'a Path) -> Result<()> {
        let full_path = self.base_path.join(path);
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        
        let fd = rustix::fs::open(
            &full_path,
            OFlags::CREATE | OFlags::WRONLY,
            self.mode,
        ).map_err(Into::into).map_err(FsError::Io)?;
        
        rustix::fs::fchown(&fd, Some(self.uid), Some(self.gid)).map_err(Into::into).map_err(FsError::Io)?;
        
        Ok(())
    }

    async fn create_directory<'a>(&'a self, path: &'a Path) -> Result<()> {
        let full_path = self.base_path.join(path);
        debug!("create_directory: creating directory at {:?}", full_path);
        
        tokio::fs::create_dir_all(&full_path).await?;
        
        // 设置目录权限和所有者
        let fd = rustix::fs::open(
            &full_path,
            OFlags::RDONLY,
            self.mode,
        ).map_err(Into::into).map_err(FsError::Io)?;
        
        rustix::fs::fchown(&fd, Some(self.uid), Some(self.gid)).map_err(Into::into).map_err(FsError::Io)?;
        
        Ok(())
    }

    async fn delete<'a>(&'a self, path: &'a Path) -> Result<()> {
        let full_path = self.base_path.join(path);
        if full_path.is_dir() {
            tokio::fs::remove_dir_all(&full_path).await?;
        } else {
            tokio::fs::remove_file(&full_path).await?;
        }
        Ok(())
    }

    async fn exists<'a>(&'a self, path: &'a Path) -> Result<bool> {
        let full_path = self.base_path.join(path);
        Ok(full_path.exists())
    }
} 