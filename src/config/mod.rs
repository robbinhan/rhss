use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub id: Uuid,
    pub name: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: Option<String>,
    pub key_path: Option<String>,
    pub passphrase: Option<String>,
}

impl ServerConfig {
    pub fn new(
        id: Uuid,
        name: String,
        host: String,
        port: u16,
        username: String,
        password: Option<String>,
        key_path: Option<String>,
        passphrase: Option<String>,
    ) -> Self {
        Self {
            id,
            name,
            host,
            port,
            username,
            password,
            key_path,
            passphrase,
        }
    }
} 