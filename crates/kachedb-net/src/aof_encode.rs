//! `kachedb-net` — AOF frame encoding and dispatch hook.

/// Magic bytes embedded at the beginning of every `.kaof` frame: "KA" (0x4B, 0x41).
pub const AOF_MAGIC: [u8; 2] = [0x4B, 0x41];

/// Minimum frame overhead:
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
    type Error = ();

    fn try_from(val: u8) -> Result<Self, Self::Error> {
        match val {
            0x01 => Ok(Self::Set),
            0x02 => Ok(Self::Del),
            0x03 => Ok(Self::Expire),
            0x04 => Ok(Self::VAdd),
            0x05 => Ok(Self::VIndexCreate),
            0x06 => Ok(Self::VIndexDrop),
            _ => Err(()),
        }
    }
}

/// Encodes an AOF mutation into a binary `.kaof` frame with CRC32.
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

static AOF_CHANNEL: std::sync::RwLock<Option<crossbeam_channel::Sender<Vec<u8>>>> =
    std::sync::RwLock::new(None);

/// Configures the global AOF channel for streaming mutations to the AOF writer thread.
pub fn set_aof_channel(tx: crossbeam_channel::Sender<Vec<u8>>) {
    *AOF_CHANNEL.write().unwrap() = Some(tx);
}

/// Dispatches an encoded frame to the active AOF writer if configured.
pub fn emit_aof(op: AofOp, key: &[u8], value: &[u8], now_sec: u32) {
    if let Ok(guard) = AOF_CHANNEL.read() {
        if let Some(ref tx) = *guard {
            let frame = encode_frame(op, key, value, now_sec as u64);
            let _ = tx.try_send(frame);
        }
    }
}
