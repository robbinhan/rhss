use thiserror::Error;

#[derive(Error, Debug)]
pub enum FsError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Storage error: {0}")]
    Storage(String),
    
    #[error("Metadata error: {0}")]
    Metadata(String),
    
    #[error("File not found: {0}")]
    NotFound(String),
    
    #[error("Permission denied: {0}")]
    PermissionDenied(String),
    
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
}

pub type Result<T> = std::result::Result<T, FsError>; 