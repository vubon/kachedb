//! `kachedb-net` — AOF frame encoding and dispatch hook.

/// Magic bytes embedded at the beginning of every `.kaof` frame: "KA" (0x4B, 0x41).
pub const AOF_MAGIC: [u8; 2] = [0x4B, 0x41];

/// Minimum frame overhead:
/// Magic (2B) + CmdId (1B) + Timestamp (8B) + KeyLen (2B) + ValLen (4B) + CRC32 (4B) = 21B.
pub const AOF_HEADER_OVERHEAD: usize = 21;

/// Maximum encoded size of one AOF record, including framing.
pub const MAX_AOF_RECORD_SIZE: usize = 64 * 1024 * 1024;

/// Counts the space for a batch with an empty envelope key, a u32 pair
/// count, and u16 key and u32 value lengths before each pair's bytes.
pub(crate) fn mset_fits_aof_record(pairs: &[(&[u8], &[u8])]) -> bool {
    mset_record_len(pairs).is_some()
}

pub(crate) fn mset_record_len(pairs: &[(&[u8], &[u8])]) -> Option<usize> {
    let header = CHECKED_FRAME_OVERHEAD + size_of::<u32>();
    let pair_header = size_of::<u16>() + size_of::<u32>();
    pairs.iter().try_fold(header, |bytes, (key, value)| {
        let bytes = bytes
            .checked_add(pair_header)?
            .checked_add(key.len())?
            .checked_add(value.len())?;
        (bytes <= MAX_AOF_RECORD_SIZE).then_some(bytes)
    })
}

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
    HSet = 0x07,
    HDel = 0x08,
    Put = 0x09,
    MSet = 0x0a,
    Command = 0x0b,
    Generation = 0x0c,
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
            0x07 => Ok(Self::HSet),
            0x08 => Ok(Self::HDel),
            0x09 => Ok(Self::Put),
            0x0a => Ok(Self::MSet),
            0x0b => Ok(Self::Command),
            0x0c => Ok(Self::Generation),
            _ => Err(()),
        }
    }
}

/// Encodes an AOF mutation into a binary `.kaof` frame with CRC32.
pub fn encode_frame(op: AofOp, key: &[u8], value: &[u8], timestamp_sec: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(AOF_HEADER_OVERHEAD + key.len() + value.len());
    append_frame(&mut buf, op, key, value, timestamp_sec);
    buf
}

fn append_frame(buf: &mut Vec<u8>, op: AofOp, key: &[u8], value: &[u8], timestamp_sec: u64) {
    let start = buf.len();
    append_mutation_header(buf, op, key, value.len(), timestamp_sec);
    buf.extend_from_slice(value);
    let checksum = crc32fast::hash(&buf[start + 2..]);
    buf.extend_from_slice(&checksum.to_le_bytes());
}

fn append_mutation_header(
    buf: &mut Vec<u8>,
    op: AofOp,
    key: &[u8],
    value_len: usize,
    timestamp_sec: u64,
) {
    buf.extend_from_slice(&AOF_MAGIC);
    buf.push(op as u8);
    buf.extend_from_slice(&timestamp_sec.to_le_bytes());
    buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&(value_len as u32).to_le_bytes());
}

/// A checked length header lets recovery identify a torn final payload.
pub const CHECKED_FRAME_MAGIC: [u8; 2] = *b"K2";
pub const CHECKED_HEADER_SIZE: usize = 10;
pub const CHECKED_FRAME_OVERHEAD: usize = CHECKED_HEADER_SIZE + AOF_HEADER_OVERHEAD;

/// Encodes a checked envelope followed by the existing CRC-protected record.
pub fn encode_checked_frame(op: AofOp, key: &[u8], value: &[u8], now: u32) -> Vec<u8> {
    encode_checked_frame_with(op, key, value.len(), now, |record| {
        record.extend_from_slice(value)
    })
}

/// Writes a payload of the given length directly into its final framed allocation.
pub(crate) fn encode_checked_frame_with(
    op: AofOp,
    key: &[u8],
    value_len: usize,
    now: u32,
    payload: impl FnOnce(&mut Vec<u8>),
) -> Vec<u8> {
    let inner_len = AOF_HEADER_OVERHEAD + key.len() + value_len;
    let mut record = Vec::with_capacity(CHECKED_HEADER_SIZE + inner_len);
    record.extend_from_slice(&CHECKED_FRAME_MAGIC);
    record.extend_from_slice(&(inner_len as u32).to_le_bytes());
    let header_crc = crc32fast::hash(&record);
    record.extend_from_slice(&header_crc.to_le_bytes());

    append_mutation_header(&mut record, op, key, value_len, u64::from(now));
    payload(&mut record);
    debug_assert_eq!(record.len() + 4, CHECKED_HEADER_SIZE + inner_len);
    let checksum = crc32fast::hash(&record[CHECKED_HEADER_SIZE + 2..]);
    record.extend_from_slice(&checksum.to_le_bytes());
    record
}
