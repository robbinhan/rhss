use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, Duration};
use tracing::{debug, info};

/// 文件位置信息
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StorageLocation {
    Hot,
    Cold,
    Both,  // 文件在两个存储中都存在
}

/// 缓存条目
#[derive(Debug, Clone)]
struct CacheEntry {
    location: StorageLocation,
    last_accessed: SystemTime,
    size: Option<u64>,
}

/// 文件位置缓存
pub struct FileLocationCache {
    /// 缓存映射：路径 -> 位置信息
    cache: Arc<RwLock<HashMap<PathBuf, CacheEntry>>>,
    /// 缓存过期时间（秒）
    ttl: Duration,
    /// 最大缓存条目数
    max_entries: usize,
}

impl FileLocationCache {
    /// 创建新的文件位置缓存
    pub fn new(ttl_seconds: u64, max_entries: usize) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            ttl: Duration::from_secs(ttl_seconds),
            max_entries,
        }
    }

    /// 获取文件位置
    pub fn get(&self, path: &Path) -> Option<StorageLocation> {
        let cache = self.cache.read().unwrap();
        
        if let Some(entry) = cache.get(path) {
            // 检查是否过期
            if let Ok(elapsed) = entry.last_accessed.elapsed() {
                if elapsed < self.ttl {
                    debug!("缓存命中: {:?} -> {:?}", path, entry.location);
                    return Some(entry.location);
                }
            }
            debug!("缓存过期: {:?}", path);
        }
        
        None
    }

    /// 更新文件位置
    pub fn set(&self, path: &Path, location: StorageLocation, size: Option<u64>) {
        let mut cache = self.cache.write().unwrap();
        
        // 如果缓存已满，删除最旧的条目
        if cache.len() >= self.max_entries && !cache.contains_key(path) {
            // 找到最旧的条目
            if let Some(oldest_key) = cache
                .iter()
                .min_by_key(|(_, entry)| entry.last_accessed)
                .map(|(k, _)| k.clone())
            {
                cache.remove(&oldest_key);
                debug!("缓存已满，删除最旧条目: {:?}", oldest_key);
            }
        }
        
        let entry = CacheEntry {
            location,
            last_accessed: SystemTime::now(),
            size,
        };
        
        debug!("更新缓存: {:?} -> {:?}", path, location);
        cache.insert(path.to_path_buf(), entry);
    }

    /// 删除缓存条目
    pub fn remove(&self, path: &Path) {
        let mut cache = self.cache.write().unwrap();
        if cache.remove(path).is_some() {
            debug!("删除缓存: {:?}", path);
        }
    }

    /// 清空缓存
    pub fn clear(&self) {
        let mut cache = self.cache.write().unwrap();
        let count = cache.len();
        cache.clear();
        info!("清空缓存: {} 个条目", count);
    }

    /// 获取缓存统计信息
    pub fn stats(&self) -> CacheStats {
        let cache = self.cache.read().unwrap();
        let total = cache.len();
        let mut hot = 0;
        let mut cold = 0;
        let mut both = 0;
        let mut expired = 0;
        
        for entry in cache.values() {
            match entry.location {
                StorageLocation::Hot => hot += 1,
                StorageLocation::Cold => cold += 1,
                StorageLocation::Both => both += 1,
            }
            
            if let Ok(elapsed) = entry.last_accessed.elapsed() {
                if elapsed >= self.ttl {
                    expired += 1;
                }
            }
        }
        
        CacheStats {
            total,
            hot,
            cold,
            both,
            expired,
        }
    }

    /// 批量更新缓存（用于目录列表）
    pub fn batch_update(&self, entries: Vec<(PathBuf, StorageLocation, Option<u64>)>) {
        let mut cache = self.cache.write().unwrap();
        
        for (path, location, size) in entries {
            // 如果缓存已满，跳过
            if cache.len() >= self.max_entries && !cache.contains_key(&path) {
                continue;
            }
            
            let entry = CacheEntry {
                location,
                last_accessed: SystemTime::now(),
                size,
            };
            
            cache.insert(path, entry);
        }
        
        debug!("批量更新缓存: {} 个条目", cache.len());
    }

    /// 移动文件位置（从一个存储层到另一个）
    pub fn move_location(&self, path: &Path, from: StorageLocation, to: StorageLocation) {
        let mut cache = self.cache.write().unwrap();
        
        if let Some(entry) = cache.get_mut(path) {
            if entry.location == from {
                entry.location = to;
                entry.last_accessed = SystemTime::now();
                debug!("移动缓存位置: {:?} 从 {:?} 到 {:?}", path, from, to);
            }
        }
    }
}

/// 缓存统计信息
#[derive(Debug)]
pub struct CacheStats {
    pub total: usize,
    pub hot: usize,
    pub cold: usize,
    pub both: usize,
    pub expired: usize,
}

impl std::fmt::Display for CacheStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "缓存统计: 总计={}, 热存储={}, 冷存储={}, 两者={}, 已过期={}",
            self.total, self.hot, self.cold, self.both, self.expired
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_cache_basic() {
        let cache = FileLocationCache::new(60, 100);
        let path = Path::new("test.txt");
        
        // 初始应该没有缓存
        assert_eq!(cache.get(path), None);
        
        // 设置缓存
        cache.set(path, StorageLocation::Hot, Some(1024));
        
        // 应该能获取到
        assert_eq!(cache.get(path), Some(StorageLocation::Hot));
        
        // 删除缓存
        cache.remove(path);
        assert_eq!(cache.get(path), None);
    }

    #[test]
    fn test_cache_expiry() {
        let cache = FileLocationCache::new(1, 100); // 1秒过期
        let path = Path::new("test.txt");
        
        cache.set(path, StorageLocation::Cold, None);
        assert_eq!(cache.get(path), Some(StorageLocation::Cold));
        
        // 等待过期
        thread::sleep(Duration::from_secs(2));
        assert_eq!(cache.get(path), None);
    }

    #[test]
    fn test_cache_max_entries() {
        let cache = FileLocationCache::new(60, 2); // 最多2个条目
        
        cache.set(Path::new("file1.txt"), StorageLocation::Hot, None);
        cache.set(Path::new("file2.txt"), StorageLocation::Cold, None);
        
        // 添加第三个应该删除最旧的
        thread::sleep(Duration::from_millis(10));
        cache.set(Path::new("file3.txt"), StorageLocation::Hot, None);
        
        // file1 应该被删除
        assert_eq!(cache.get(Path::new("file1.txt")), None);
        assert_eq!(cache.get(Path::new("file2.txt")), Some(StorageLocation::Cold));
        assert_eq!(cache.get(Path::new("file3.txt")), Some(StorageLocation::Hot));
    }
}
