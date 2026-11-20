//! `kachedb-server` — Asynchronous Append-Only File (AOF) persistence engine.
//!
//! Provides binary crash-consistent persistence with `.kaof` frames, CRC32 integrity checks,
//! and configurable `fsync` policies (`always`, `everysec`, `no`).

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError};
use kachedb_net::aof_encode::encode_checked_frame;
pub use kachedb_net::aof_encode::{AOF_HEADER_OVERHEAD, AOF_MAGIC, AofOp, encode_frame};
pub use kachedb_net::durability::AppendFsync;
use kachedb_net::durability::{AofDispatch, Checkpoint, WriterRequest};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AofError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("AOF slab error: {0}")]
    Slab(#[from] kachedb_core::CoreError),
    #[error("Corrupt AOF magic header: expected 'KA'")]
    CorruptMagic,
    #[error("Invalid AOF opcode: {0:#04x}")]
    InvalidOpCode(u8),
    #[error(
        "AOF frame CRC32 checksum mismatch: expected {expected:#010x}, calculated {calculated:#010x}"
    )]
    ChecksumMismatch { expected: u32, calculated: u32 },
    #[error("Unexpected end of AOF file")]
    #[allow(dead_code)]
    UnexpectedEof,
    #[error("Cannot rewrite AOF for an entry without original key bytes")]
    MissingKeyBytes,
    #[error("Invalid AOF record: {0}")]
    InvalidRecord(&'static str),
    #[error("AOF replay rejected a mutation: {0}")]
    Apply(String),
    #[error("AOF and snapshot cannot be combined: {0}")]
    Migration(&'static str),
    #[error("AOF worker terminated unexpectedly")]
    WriterStopped,
    #[error("AOF record at byte {offset} is invalid: {source}")]
    AtOffset { offset: u64, source: Box<AofError> },
}

/// A parsed `.kaof` frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AofFrame<'a> {
    pub op: AofOp,
    pub timestamp_sec: u64,
    pub key: &'a [u8],
    pub value: &'a [u8],
}

/// Attempts to decode a single `.kaof` frame from a byte buffer.
/// Returns `Ok(Some((frame, consumed_bytes)))` on success, or `Ok(None)` if buffer has incomplete frame.
pub fn decode_frame(src: &[u8]) -> Result<Option<(AofFrame<'_>, usize)>, AofError> {
    if src.len() < AOF_HEADER_OVERHEAD {
        return Ok(None);
    }

    if src[0..2] != AOF_MAGIC {
        return Err(AofError::CorruptMagic);
    }

    let op = AofOp::try_from(src[2]).map_err(|_| AofError::InvalidOpCode(src[2]))?;
    let timestamp_sec = u64::from_le_bytes([
        src[3], src[4], src[5], src[6], src[7], src[8], src[9], src[10],
    ]);
    let key_len = u16::from_le_bytes([src[11], src[12]]) as usize;

    if src.len() < 13 + key_len + 4 {
        return Ok(None);
    }

    let val_offset = 13 + key_len;
    let val_len = u32::from_le_bytes([
        src[val_offset],
        src[val_offset + 1],
        src[val_offset + 2],
        src[val_offset + 3],
    ]) as usize;

    let total_frame_len = val_offset + 4 + val_len + 4;
    if src.len() < total_frame_len {
        return Ok(None);
    }

    let key = &src[13..val_offset];
    let value = &src[val_offset + 4..val_offset + 4 + val_len];

    let crc_offset = val_offset + 4 + val_len;
    let stored_crc = u32::from_le_bytes([
        src[crc_offset],
        src[crc_offset + 1],
        src[crc_offset + 2],
        src[crc_offset + 3],
    ]);

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&src[2..crc_offset]);
    let calculated_crc = hasher.finalize();

    if stored_crc != calculated_crc {
        return Err(AofError::ChecksumMismatch {
            expected: stored_crc,
            calculated: calculated_crc,
        });
    }

    Ok(Some((
        AofFrame {
            op,
            timestamp_sec,
            key,
            value,
        },
        total_frame_len,
    )))
}

pub fn open_locked(path: &Path, create: bool) -> Result<File, AofError> {
    let file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(create)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(AofError::InvalidRecord("AOF must be a regular file"));
    }
    file.try_lock().map_err(std::io::Error::from)?;
    Ok(file)
}

/// An opened journal and the worker responsible for its ordered writes.
pub struct AofWriter {
    dispatch: Arc<AofDispatch>,
    worker_handle: Option<JoinHandle<Result<(), AofError>>>,
}

impl AofWriter {
    pub fn start(
        mut file: File,
        path: &Path,
        policy: AppendFsync,
        checkpoint: Checkpoint,
        initialize: Option<bool>,
    ) -> Result<Self, AofError> {
        if let Some(requires_snapshot) = initialize {
            let mut header = checkpoint.generation.to_vec();
            header.push(u8::from(requires_snapshot));
            file.write_all(&encode_checked_frame(AofOp::Generation, &[], &header, 0))?;
            file.sync_all()?;
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            std::fs::File::open(parent)?.sync_all()?;
        }
        let (dispatch, rx) = AofDispatch::new(checkpoint, policy);
        let worker_dispatch = dispatch.clone();
        let worker_handle = thread::Builder::new()
            .name("kachedb-aof-writer".into())
            .spawn(move || {
                let result = Self::writer_loop(&mut file, policy, &rx, &worker_dispatch);
                if let Err(error) = &result {
                    log::error!("AOF writer failed: {error}");
                }
                worker_dispatch.fail();
                result
            })?;
        Ok(Self {
            dispatch,
            worker_handle: Some(worker_handle),
        })
    }

    pub fn dispatch(&self) -> Arc<AofDispatch> {
        self.dispatch.clone()
    }

    fn writer_loop(
        file: &mut std::fs::File,
        policy: AppendFsync,
        rx: &Receiver<WriterRequest>,
        dispatch: &AofDispatch,
    ) -> Result<(), AofError> {
        let mut last_sync = Instant::now();
        let mut dirty = false;
        loop {
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(WriterRequest::Record {
                    bytes,
                    done,
                    permit,
                }) => {
                    let result = file.write_all(&bytes).and_then(|()| {
                        if done.is_some() {
                            file.sync_data()
                        } else {
                            Ok(())
                        }
                    });
                    if let Err(error) = result {
                        dispatch.fail();
                        return Err(error.into());
                    }
                    dirty = done.is_none();
                    if let Some(done) = done {
                        let _ = done.send(());
                    }
                    drop(permit);
                }
                Ok(WriterRequest::Sync(done)) => {
                    file.sync_all()?;
                    let _ = done.send(());
                    last_sync = Instant::now();
                    dirty = false;
                }
                Ok(WriterRequest::Stop) => {
                    file.sync_all()?;
                    return Ok(());
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return Err(AofError::WriterStopped),
            }
            if dirty
                && policy == AppendFsync::EverySec
                && last_sync.elapsed() >= Duration::from_secs(1)
            {
                file.sync_data()?;
                last_sync = Instant::now();
                dirty = false;
            }
        }
    }

    pub fn stop(&mut self) -> Result<(), AofError> {
        if let Some(handle) = self.worker_handle.take() {
            self.dispatch.stop();
            handle.join().map_err(|_| AofError::WriterStopped)??;
        }
        Ok(())
    }
}

impl Drop for AofWriter {
    fn drop(&mut self) {
        if let Err(error) = self.stop() {
            log::error!("AOF shutdown failed: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aof_encode_decode_roundtrip() {
        let key = b"user:1001";
        let val = b"{\"name\": \"Alice\", \"role\": \"admin\"}";
        let ts = 1725450000;

        let bytes = encode_frame(AofOp::Set, key, val, ts);
        let res = decode_frame(&bytes).unwrap();
        assert!(res.is_some());

        let (frame, consumed) = res.unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(frame.op, AofOp::Set);
        assert_eq!(frame.key, key);
        assert_eq!(frame.value, val);
        assert_eq!(frame.timestamp_sec, ts);
    }

    #[test]
    fn test_aof_corrupt_checksum_detection() {
        let key = b"test_key";
        let val = b"test_val";
        let mut bytes = encode_frame(AofOp::Set, key, val, 100);

        // Corrupt one byte of value (after 4-byte val_len header)
        let val_idx = 13 + key.len() + 4;
        bytes[val_idx] ^= 0xFF;

        let res = decode_frame(&bytes);
        assert!(matches!(res, Err(AofError::ChecksumMismatch { .. })));
    }

    #[test]
    fn test_aof_partial_frame_returns_none() {
        let bytes = encode_frame(AofOp::Del, b"k", b"v", 100);
        let partial = &bytes[..bytes.len() - 5];
        let res = decode_frame(partial).unwrap();
        assert!(res.is_none());
    }
}
