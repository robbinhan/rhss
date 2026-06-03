//! Daemon-side control socket.
//!
//! Bound by `mount_cmd` after FUSE + Tierer are up. Listens on a Unix socket
//! under `<db.parent>/.rhss/control.sock` (mode 0600). Newline-delimited
//! JSON per [`protocol`]. Server is single-threaded but handles each accepted
//! connection in a worker thread — control ops are infrequent and short.

pub mod protocol;
pub mod server;

pub use protocol::{Request, Response, ResponseData, Tier};
pub use server::{socket_path_for, ControlServer};
