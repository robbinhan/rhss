use std::path::Path;
use async_trait::async_trait;
use crate::error::Result;
use crate::error::FsError;
use crate::storage::Storage;

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub size: u64,
    pub is_dir: bool,
    pub permissions: u32,
    pub modified: std::time::SystemTime,
}

#[async_trait]
pub trait FileSystem: Send + Sync + std::fmt::Debug {
    async fn list_directory<'a>(&'a self, path: &'a Path) -> Result<Vec<String>>;
    async fn get_metadata<'a>(&'a self, path: &'a Path) -> Result<FileMetadata>;
    async fn read_file<'a>(&'a self, path: &'a Path) -> Result<Vec<u8>>;
    async fn write_file<'a>(&'a self, path: &'a Path, data: &'a [u8]) -> Result<()>;
    async fn create_file<'a>(&'a self, path: &'a Path) -> Result<()>;
    async fn create_directory<'a>(&'a self, path: &'a Path) -> Result<()>;
    async fn delete<'a>(&'a self, path: &'a Path) -> Result<()>;
    async fn exists<'a>(&'a self, path: &'a Path) -> Result<bool>;
}

#[derive(Debug)]
pub struct VirtualFileSystem {
    storage: Box<dyn FileSystem>,
}

impl VirtualFileSystem {
    pub fn new(storage: Box<dyn FileSystem>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl FileSystem for VirtualFileSystem {
    async fn list_directory<'a>(&'a self, path: &'a Path) -> Result<Vec<String>> {
        self.storage.list_directory(path).await
    }

    async fn get_metadata<'a>(&'a self, path: &'a Path) -> Result<FileMetadata> {
        self.storage.get_metadata(path).await
    }

    async fn read_file<'a>(&'a self, path: &'a Path) -> Result<Vec<u8>> {
        self.storage.read_file(path).await
    }

    async fn write_file<'a>(&'a self, path: &'a Path, data: &'a [u8]) -> Result<()> {
        self.storage.write_file(path, data).await
    }

    async fn create_file<'a>(&'a self, path: &'a Path) -> Result<()> {
        self.storage.create_file(path).await
    }

    async fn create_directory<'a>(&'a self, path: &'a Path) -> Result<()> {
        self.storage.create_directory(path).await
    }

    async fn delete<'a>(&'a self, path: &'a Path) -> Result<()> {
        self.storage.delete(path).await
    }

    async fn exists<'a>(&'a self, path: &'a Path) -> Result<bool> {
        self.storage.exists(path).await
    }
} 