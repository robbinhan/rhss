use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::os::unix::fs::MetadataExt;
use tracing::{debug, error};
use crate::error::{Result, FsError};
use crate::fs::{FileSystem, FileMetadata};

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

#[derive(Debug)]
pub struct HybridStorage {
    hot_storage: Box<dyn FileSystem>,
    cold_storage: Box<dyn FileSystem>,
    threshold: u64,
}

impl HybridStorage {
    pub fn new(hot_storage: Box<dyn FileSystem>, cold_storage: Box<dyn FileSystem>, threshold: u64) -> Self {
        Self {
            hot_storage,
            cold_storage,
            threshold,
        }
    }

    async fn get_storage<'a>(&'a self, path: &'a Path) -> &'a Box<dyn FileSystem> {
        match self.get_metadata(path).await {
            Ok(metadata) if metadata.size >= self.threshold => &self.cold_storage,
            _ => &self.hot_storage,
        }
    }
}

#[async_trait]
impl FileSystem for HybridStorage {
    async fn list_directory<'a>(&'a self, path: &'a Path) -> Result<Vec<String>> {
        let mut entries = self.hot_storage.list_directory(path).await?;
        let cold_entries = self.cold_storage.list_directory(path).await?;
        entries.extend(cold_entries);
        Ok(entries)
    }

    async fn get_metadata<'a>(&'a self, path: &'a Path) -> Result<FileMetadata> {
        if let Ok(metadata) = self.hot_storage.get_metadata(path).await {
            Ok(metadata)
        } else {
            self.cold_storage.get_metadata(path).await
        }
    }

    async fn read_file<'a>(&'a self, path: &'a Path) -> Result<Vec<u8>> {
        self.get_storage(path).await.read_file(path).await
    }

    async fn write_file<'a>(&'a self, path: &'a Path, data: &'a [u8]) -> Result<()> {
        let storage = if data.len() as u64 >= self.threshold {
            &self.cold_storage
        } else {
            &self.hot_storage
        };
        
        // 如果文件存在于另一个存储中，先删除它
        let other_storage = if data.len() as u64 >= self.threshold {
            &self.hot_storage
        } else {
            &self.cold_storage
        };
        
        if other_storage.exists(path).await? {
            other_storage.delete(path).await?;
        }
        
        storage.write_file(path, data).await
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
        hot_result.and(cold_result)
    }

    async fn exists<'a>(&'a self, path: &'a Path) -> Result<bool> {
        Ok(self.hot_storage.exists(path).await? || self.cold_storage.exists(path).await?)
    }
}

// 本地存储实现
#[derive(Debug)]
pub struct LocalStorage {
    root: PathBuf,
}

impl LocalStorage {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }
}

#[async_trait]
impl FileSystem for LocalStorage {
    async fn list_directory<'a>(&'a self, path: &'a Path) -> Result<Vec<String>> {
        let full_path = self.root.join(path);
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
                entries.push(name.to_string());
            }
        }
        Ok(entries)
    }

    async fn get_metadata<'a>(&'a self, path: &'a Path) -> Result<FileMetadata> {
        let full_path = self.root.join(path);
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
        let full_path = self.root.join(path);
        tokio::fs::read(&full_path)
            .await
            .map_err(|e| FsError::Io(e))
    }

    async fn write_file<'a>(&'a self, path: &'a Path, data: &'a [u8]) -> Result<()> {
        let full_path = self.root.join(path);
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
        let full_path = self.root.join(path);
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
        let full_path = self.root.join(path);
        debug!("create_directory: creating directory at {:?}", full_path);
        tokio::fs::create_dir_all(&full_path)
            .await
            .map_err(|e| {
                error!("create_directory error for path={:?}: {:?}", full_path, e);
                FsError::Io(e)
            })
    }

    async fn delete<'a>(&'a self, path: &'a Path) -> Result<()> {
        let full_path = self.root.join(path);
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
        let full_path = self.root.join(path);
        Ok(full_path.exists())
    }
} 