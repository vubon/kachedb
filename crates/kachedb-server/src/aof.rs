//! `kachedb-server` — Asynchronous Append-Only File (AOF) persistence engine.
//!
//! Provides binary crash-consistent persistence with `.kaof` frames, CRC32 integrity checks,
//! and configurable `fsync` policies (`always`, `everysec`, `no`).

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossbeam_channel::{Receiver, Sender, bounded};
use thiserror::Error;

/// Magic bytes embedded at the beginning of every `.kaof` frame: "KA" (0x4B, 0x41).
pub const AOF_MAGIC: [u8; 2] = [0x4B, 0x41];

/// Minimum frame size with 0-byte key and 0-byte value:
/// Magic (2B) + CmdId (1B) + Timestamp (8B) + KeyLen (2B) + ValLen (4B) + CRC32 (4B) = 21B.
pub const AOF_HEADER_OVERHEAD: usize = 21;

/// AOF operation codes.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AofOp {
    Set = 0x01,
    Del = 0x02,
    Expire = 0x03,
    VAdd = 0x04,
    VIndexCreate = 0x05,
    VIndexDrop = 0x06,
}

impl TryFrom<u8> for AofOp {
    type Error = AofError;

    fn try_from(val: u8) -> Result<Self, Self::Error> {
        match val {
            0x01 => Ok(Self::Set),
            0x02 => Ok(Self::Del),
            0x03 => Ok(Self::Expire),
            0x04 => Ok(Self::VAdd),
            0x05 => Ok(Self::VIndexCreate),
            0x06 => Ok(Self::VIndexDrop),
            other => Err(AofError::InvalidOpCode(other)),
        }
    }
}

/// Fsync policy for flushing dirty AOF writes to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AppendFsync {
    /// Fsync on every batch of writes. Maximum durability.
    Always,
    /// Fsync once per second in background. Balance of speed and 1s durability guarantee.
    #[default]
    EverySec,
    /// Never explicitly fsync. Rely on OS page cache flushing. Maximum throughput.
    No,
}

impl AppendFsync {
    pub fn parse(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("always") {
            Some(Self::Always)
        } else if s.eq_ignore_ascii_case("everysec") {
            Some(Self::EverySec)
        } else if s.eq_ignore_ascii_case("no") {
            Some(Self::No)
        } else {
            None
        }
    }
}

#[derive(Debug, Error)]
pub enum AofError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
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
}

/// A parsed `.kaof` frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AofFrame {
    pub op: AofOp,
    pub timestamp_sec: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// Encodes an AOF mutation into a binary `.kaof` frame with CRC32.
#[allow(dead_code)]
pub fn encode_frame(op: AofOp, key: &[u8], value: &[u8], timestamp_sec: u64) -> Vec<u8> {
    let key_len = key.len() as u16;
    let val_len = value.len() as u32;
    let total_len = AOF_HEADER_OVERHEAD + key.len() + value.len();

    let mut buf = Vec::with_capacity(total_len);
    buf.extend_from_slice(&AOF_MAGIC);
    buf.push(op as u8);
    buf.extend_from_slice(&timestamp_sec.to_le_bytes());
    buf.extend_from_slice(&key_len.to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&val_len.to_le_bytes());
    buf.extend_from_slice(value);

    // Compute CRC32 from OpCode (offset 2) through end of value
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&buf[2..]);
    let crc = hasher.finalize();

    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

/// Attempts to decode a single `.kaof` frame from a byte buffer.
/// Returns `Ok(Some((frame, consumed_bytes)))` on success, or `Ok(None)` if buffer has incomplete frame.
pub fn decode_frame(src: &[u8]) -> Result<Option<(AofFrame, usize)>, AofError> {
    if src.len() < AOF_HEADER_OVERHEAD {
        return Ok(None);
    }

    if src[0..2] != AOF_MAGIC {
        return Err(AofError::CorruptMagic);
    }

    let op = AofOp::try_from(src[2])?;
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

    let key = src[13..val_offset].to_vec();
    let value = src[val_offset + 4..val_offset + 4 + val_len].to_vec();

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

/// Asynchronous background AOF file writer.
pub struct AofWriter {
    tx: Sender<Vec<u8>>,
    running: Arc<AtomicBool>,
    worker_handle: Option<JoinHandle<()>>,
}

impl AofWriter {
    /// Starts the background AOF writer thread for the given file path and fsync policy.
    pub fn start(path: impl Into<PathBuf>, policy: AppendFsync) -> Result<Self, AofError> {
        let path = path.into();
        let (tx, rx) = bounded::<Vec<u8>>(65536);
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = Arc::clone(&running);

        let handle = thread::Builder::new()
            .name("kachedb-aof-writer".into())
            .spawn(move || {
                Self::writer_loop(path, policy, rx, running_clone);
            })
            .map_err(AofError::Io)?;

        Ok(Self {
            tx,
            running,
            worker_handle: Some(handle),
        })
    }

    /// Returns a sender channel handle for worker threads to push mutation frames.
    pub fn channel(&self) -> Sender<Vec<u8>> {
        self.tx.clone()
    }

    /// Appends a mutation command frame asynchronously to the AOF.
    #[allow(dead_code)]
    pub fn append(&self, op: AofOp, key: &[u8], value: &[u8]) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let frame = encode_frame(op, key, value, ts);
        let _ = self.tx.try_send(frame);
    }

    /// Appends raw pre-encoded frame bytes directly to the AOF.
    #[allow(dead_code)]
    pub fn append_raw(&self, frame: Vec<u8>) {
        let _ = self.tx.try_send(frame);
    }

    fn writer_loop(
        path: PathBuf,
        policy: AppendFsync,
        rx: Receiver<Vec<u8>>,
        running: Arc<AtomicBool>,
    ) {
        let mut file = match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(f) => f,
            Err(e) => {
                log::error!("AofWriter: failed to open AOF file at {:?}: {e}", path);
                return;
            }
        };

        let mut last_fsync = Instant::now();
        let mut batch_buf = Vec::with_capacity(128 * 1024);

        while running.load(Ordering::Relaxed) || !rx.is_empty() {
            // Receive first item with 100ms timeout
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(frame) => {
                    batch_buf.extend_from_slice(&frame);
                    // Drain any additional pending frames
                    while let Ok(f) = rx.try_recv() {
                        batch_buf.extend_from_slice(&f);
                        if batch_buf.len() >= 64 * 1024 {
                            break;
                        }
                    }

                    if let Err(e) = file.write_all(&batch_buf) {
                        log::error!("AofWriter: failed to write batch to {:?}: {e}", path);
                    }
                    batch_buf.clear();

                    if policy == AppendFsync::Always {
                        let _ = file.sync_data();
                    }
                }
                Err(_) => {
                    // Timeout tick: check periodic fsync
                }
            }

            if policy == AppendFsync::EverySec && last_fsync.elapsed() >= Duration::from_secs(1) {
                let _ = file.sync_data();
                last_fsync = Instant::now();
            }
        }

        // Final flush before thread termination
        let _ = file.flush();
        let _ = file.sync_all();
        log::info!("AofWriter: safely stopped, flushed AOF to {:?}", path);
    }
}

impl Drop for AofWriter {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(handle) = self.worker_handle.take() {
            let _ = handle.join();
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
