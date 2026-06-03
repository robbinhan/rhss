//! zstd compression helpers used by the tierer when migrating immutable
//! files (D24). Compressed files are stored at `<backend_path>.zst` on the
//! destination backend; the PathIndex `backend_path` keeps the logical
//! name, and `compressed=true` tells the read path to decompress before
//! opening.
//!
//! Decompression is into a sidecar staging area at
//! `<backend_root>/.rhss_decompressed/<backend_path>`. The first FUSE open
//! materializes the staging file; subsequent reads hit local POSIX speed.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sha2::{Digest, Sha256};
use tracing::debug;

use crate::backend::Backend;
use crate::error::{FsError, Result};

const ZST_SUFFIX: &str = ".zst";
const COMPRESS_LEVEL: i32 = 9;
const CHUNK: usize = 1 << 20; // 1 MiB IO chunks
const STAGING_DIR: &str = ".rhss_decompressed";

/// Append `.zst` to a backend-relative path.
pub fn compressed_path(p: &Path) -> PathBuf {
    let mut s = p.as_os_str().to_owned();
    s.push(ZST_SUFFIX);
    PathBuf::from(s)
}

/// Compress source backend's file into dst backend's `<dst_path>.zst`.
/// Returns sha256 hex of the **uncompressed** content.
pub fn compress_between(
    src: &Arc<dyn Backend>,
    src_path: &Path,
    dst: &Arc<dyn Backend>,
    dst_path: &Path,
) -> Result<String> {
    let dst_zst = compressed_path(dst_path);

    // Resolve to absolute paths so we can hand File handles to the zstd
    // streaming encoder directly — avoiding a Vec<u8> buffer for big
    // files.
    let dst_abs = dst.resolve(&dst_zst);
    if let Some(parent) = dst_abs.parent() {
        std::fs::create_dir_all(parent).map_err(FsError::Io)?;
    }
    let out_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&dst_abs)
        .map_err(FsError::Io)?;
    let mut encoder = zstd::stream::write::Encoder::new(out_file, COMPRESS_LEVEL)
        .map_err(FsError::Io)?;

    let mut hasher = Sha256::new();
    let mut offset = 0u64;
    loop {
        let chunk = src.read_at(src_path, offset, CHUNK as u32)?;
        if chunk.is_empty() {
            break;
        }
        hasher.update(&chunk);
        encoder.write_all(&chunk).map_err(FsError::Io)?;
        if (chunk.len() as u64) < CHUNK as u64 {
            offset += chunk.len() as u64;
            break;
        }
        offset += chunk.len() as u64;
    }
    encoder.finish().map_err(FsError::Io)?;
    let hash = format!("{:x}", hasher.finalize());
    debug!(
        "compressed {} ({} bytes uncompressed) → {}",
        src_path.display(),
        offset,
        dst_zst.display()
    );
    Ok(hash)
}

/// Decompress an on-backend `<path>.zst` to a sidecar staging file at
/// `<backend_root>/.rhss_decompressed/<path>`. Returns the staging path
/// **relative to the backend root** so callers can hand it to
/// `Backend::read_at` directly. Idempotent: if the staging file already
/// exists with the right size, it's reused.
pub fn ensure_decompressed(
    backend: &Arc<dyn Backend>,
    backend_path: &Path,
    expected_size: u64,
) -> Result<PathBuf> {
    let staging_rel = staging_relative(backend_path);
    let staging_abs = backend.root().join(&staging_rel);
    if let Ok(meta) = std::fs::metadata(&staging_abs) {
        if meta.len() == expected_size {
            return Ok(staging_rel);
        }
    }
    if let Some(parent) = staging_abs.parent() {
        std::fs::create_dir_all(parent).map_err(FsError::Io)?;
    }
    let zst = compressed_path(backend_path);
    let zst_abs = backend.resolve(&zst);
    let in_file = File::open(&zst_abs).map_err(FsError::Io)?;
    let mut decoder = zstd::stream::read::Decoder::new(in_file).map_err(FsError::Io)?;
    let mut out_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&staging_abs)
        .map_err(FsError::Io)?;
    let mut buf = vec![0u8; CHUNK];
    loop {
        let n = decoder.read(&mut buf).map_err(FsError::Io)?;
        if n == 0 {
            break;
        }
        out_file.write_all(&buf[..n]).map_err(FsError::Io)?;
    }
    debug!(
        "decompressed {} → {}",
        zst_abs.display(),
        staging_abs.display()
    );
    Ok(staging_rel)
}

fn staging_relative(backend_path: &Path) -> PathBuf {
    let rel = backend_path.strip_prefix("/").unwrap_or(backend_path);
    PathBuf::from(STAGING_DIR).join(rel)
}

/// Compute sha256 of the file at `backend_path` on `backend`. Streaming —
/// no whole-file buffer. Used by B5 to record content_hash on existing
/// immutable files (e.g. file becomes immutable via `rhss lock` while
/// already on Slow).
pub fn hash_file(backend: &Arc<dyn Backend>, backend_path: &Path) -> Result<String> {
    let mut hasher = Sha256::new();
    let mut offset = 0u64;
    loop {
        let chunk = backend.read_at(backend_path, offset, CHUNK as u32)?;
        if chunk.is_empty() {
            break;
        }
        hasher.update(&chunk);
        if (chunk.len() as u64) < CHUNK as u64 {
            break;
        }
        offset += chunk.len() as u64;
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::PosixBackend;
    use tempfile::TempDir;

    fn backend() -> (TempDir, Arc<dyn Backend>) {
        let d = TempDir::new().unwrap();
        let b: Arc<dyn Backend> = Arc::new(PosixBackend::new("b", d.path().to_path_buf()).unwrap());
        (d, b)
    }

    #[test]
    fn round_trip_compresses_and_decompresses() {
        let (_src_d, src) = backend();
        let (_dst_d, dst) = backend();
        let payload = b"hello world ".repeat(1024);
        src.write_at(Path::new("foo.bin"), 0, &payload).unwrap();

        let hash = compress_between(&src, Path::new("foo.bin"), &dst, Path::new("foo.bin")).unwrap();
        assert_eq!(hash.len(), 64);

        // The .zst should exist on dst.
        let zst_abs = dst.resolve(Path::new("foo.bin.zst"));
        assert!(zst_abs.exists());

        let staged_rel = ensure_decompressed(&dst, Path::new("foo.bin"), payload.len() as u64).unwrap();
        // staged_rel is relative — read via the backend.
        let got = dst.read_at(&staged_rel, 0, (payload.len() as u32) + 100).unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn hash_file_stable() {
        let (_d, b) = backend();
        b.write_at(Path::new("x.bin"), 0, b"abc").unwrap();
        let h = hash_file(&b, Path::new("x.bin")).unwrap();
        // sha256("abc")
        assert_eq!(
            h,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
