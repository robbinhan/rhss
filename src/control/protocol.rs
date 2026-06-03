//! Wire protocol for the control socket.
//!
//! Newline-delimited JSON. One request per line, one response per line.
//! Server reads a full line, parses, dispatches, writes a JSON response with
//! a trailing newline, then loops. Simple, debuggable with `nc -U`.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::index::TierId as IndexTierId;

/// Tier name on the wire. Maps to/from `crate::index::TierId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Fast,
    Slow,
    Archive,
}

impl From<Tier> for IndexTierId {
    fn from(t: Tier) -> Self {
        match t {
            Tier::Fast => IndexTierId::Fast,
            Tier::Slow => IndexTierId::Slow,
            Tier::Archive => IndexTierId::Archive,
        }
    }
}

impl From<IndexTierId> for Tier {
    fn from(t: IndexTierId) -> Self {
        match t {
            IndexTierId::Fast => Tier::Fast,
            IndexTierId::Slow => Tier::Slow,
            IndexTierId::Archive => Tier::Archive,
        }
    }
}

/// Every control op is a tagged enum. The CLI sends one of these; the daemon
/// matches and dispatches.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case")]
pub enum Request {
    Ping,
    Pin { path: PathBuf, tier: Tier },
    Unpin { path: PathBuf },
    Lock { path: PathBuf },
    Unlock { path: PathBuf },
    Oneshot { wait: bool },
    Migrate { path: PathBuf, to: Tier },
    Freeze,
    Unfreeze,
    Fsck { repair: bool },
    Rescan,
}

/// Responses share an envelope: `ok` + optional `data` + optional `error`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct Response {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub data: Option<ResponseData>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub error: Option<String>,
}

impl Response {
    pub fn ok_empty() -> Self {
        Self {
            ok: true,
            data: None,
            error: None,
        }
    }

    pub fn ok_data(data: ResponseData) -> Self {
        Self {
            ok: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn err(msg: impl Into<String>) -> Self {
        Self {
            ok: false,
            data: None,
            error: Some(msg.into()),
        }
    }
}

/// One file whose declared replica list doesn't match what's actually on
/// the backends. `expected` = backends per the index `replicas` column;
/// `missing` = subset of `expected` that returned `exists()=false`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaInconsistency {
    pub path: PathBuf,
    pub expected: Vec<String>,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ResponseData {
    /// `ping` response.
    Pong { version: String, frozen: bool },
    /// `pin` / `unpin` response: confirms what's now in the row.
    Pinned { path: PathBuf, tier: Option<Tier> },
    /// `lock` / `unlock` response: confirms new mutability.
    Mutability { path: PathBuf, immutable: bool },
    /// `oneshot` response: whether the wait actually completed in time.
    OneshotCompleted { waited: bool },
    /// `migrate` response: did the migration happen, or skipped (open / pinned).
    Migrated {
        path: PathBuf,
        from: Tier,
        to: Tier,
        moved: bool,
        reason: Option<String>,
    },
    /// `freeze` / `unfreeze`: confirms new state.
    FreezeState { frozen: bool },
    /// `fsck` response: orphans (on disk, not in index), ghosts (in index,
    /// not on disk), and replica inconsistencies (D23: file claims N
    /// replicas, but ≤ N actually exist on the relevant backends).
    Fsck {
        orphans: Vec<PathBuf>,
        ghosts: Vec<PathBuf>,
        inconsistencies: Vec<ReplicaInconsistency>,
        repaired: usize,
    },
    /// `rescan` response.
    Rescan {
        added: u64,
        already_indexed: u64,
        conflicts: Vec<PathBuf>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_request_roundtrips() {
        let req = Request::Pin {
            path: PathBuf::from("/Movies/foo.mkv"),
            tier: Tier::Fast,
        };
        let s = serde_json::to_string(&req).unwrap();
        let back: Request = serde_json::from_str(&s).unwrap();
        match back {
            Request::Pin { path, tier } => {
                assert_eq!(path, PathBuf::from("/Movies/foo.mkv"));
                assert_eq!(tier, Tier::Fast);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn oneshot_request_roundtrips() {
        let req = Request::Oneshot { wait: true };
        let s = serde_json::to_string(&req).unwrap();
        let back: Request = serde_json::from_str(&s).unwrap();
        match back {
            Request::Oneshot { wait } => assert!(wait),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn ok_response_serializes_compactly() {
        let r = Response::ok_empty();
        let s = serde_json::to_string(&r).unwrap();
        assert_eq!(s, r#"{"ok":true}"#);
    }

    #[test]
    fn err_response_includes_message() {
        let r = Response::err("not indexed");
        let s = serde_json::to_string(&r).unwrap();
        assert!(s.contains(r#""ok":false"#));
        assert!(s.contains(r#""error":"not indexed""#));
    }

    #[test]
    fn tier_serializes_lowercase() {
        let s = serde_json::to_string(&Tier::Fast).unwrap();
        assert_eq!(s, r#""fast""#);
    }
}
