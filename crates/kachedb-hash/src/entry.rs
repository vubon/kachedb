//! `kachedb-hash` — Hash entry descriptor stored within a Swiss Table slot.
//!
//! Each `HashEntry` is 64-byte cache-line aligned so that a single probe group
//! (8 control bytes + 8 entries) occupies a minimal number of cache lines,
//! maximising L1 hit rates during dense-key lookups.
//!
//! The `access_flags` field is the S3-FIFO hit bit (RFC 3): updated atomically
//! on every GET, and cleared by the eviction background pass.

use std::sync::atomic::{AtomicU8, Ordering};

use kachedb_core::SlabBlockId;

/// Atomic S3-FIFO frequency tag embedded in every hash entry.
/// Atomic S3-FIFO frequency tag embedded in every hash entry.
///
/// - `0` — not accessed since last eviction sweep.
/// - `1` — accessed at least once; entry is promotion candidate.
pub const ACCESS_BIT_ACCESSED: u8 = 1;

/// Value type tag indicating standard Redis string payload.
pub const VALUE_TYPE_STRING: u8 = 0;
/// Value type tag indicating Redis Hash structure encoded in a slotted Megaslab frame.
pub const VALUE_TYPE_HASH: u8 = 1;

/// Maximum key byte length stored inline inside a 64-byte `HashEntry` (Small String Optimization).
/// Keys longer than 40 bytes store their 40-byte prefix inline and full bytes in `SwissTable::overflow_keys`.
pub const INLINE_KEY_CAPACITY: usize = 40;

/// Copyable snapshot of key metadata retrieved from a Swiss Table lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableEntry {
    /// Opaque block identifier pointing into `kachedb-core`'s `SlabPool`.
    pub slab_block_id: SlabBlockId,
    /// Byte length of the value stored in the slab slot.
    pub value_len: u32,
    /// Absolute expiration timestamp in epoch seconds (0 = persistent).
    pub expire_at_secs: u32,
    /// Type descriptor for stored value (0 = String, 1 = Hash).
    pub value_type: u8,
}

const _: () = assert!(
    std::mem::size_of::<TableEntry>() <= 24,
    "TableEntry must remain compact (<= 24 bytes)"
);

/// An occupied slot in the KacheDB Swiss Table hash index.
///
/// # Layout (64 bytes, cache-line aligned)
///
/// | Offset | Field            | Size | Description                                     |
/// |--------|------------------|------|-------------------------------------------------|
/// | 0      | `key_hash`       | 8 B  | Full 64-bit key hash (XXH3)                     |
/// | 8      | `slab_block_id`  | 4 B  | Opaque handle into the slab pool                |
/// | 12     | `value_len`      | 4 B  | Byte length of the stored value                 |
/// | 16     | `expire_at_secs` | 4 B  | Absolute expiration epoch seconds               |
/// | 20     | `access_flags`   | 1 B  | S3-FIFO atomic access frequency bit             |
/// | 21     | `value_type`     | 1 B  | Type descriptor (0 = String, 1 = Hash)          |
/// | 22     | `key_len`        | 2 B  | Full byte length of key (0 = anonymous legacy)  |
/// | 24–63  | `inline_key`     | 40 B | Inline SSO key bytes (or 40-byte prefix if >40B)|
#[repr(C, align(64))]
pub struct HashEntry {
    /// Full 64-bit hash of the key used for fingerprint verification.
    pub key_hash: u64,
    /// Opaque block identifier pointing into `kachedb-core`'s `SlabPool`.
    pub slab_block_id: SlabBlockId,
    /// Byte length of the value stored in the slab slot.
    pub value_len: u32,
    /// Absolute expiration timestamp in epoch seconds (0 = persistent / no TTL).
    pub expire_at_secs: u32,
    /// S3-FIFO single-bit frequency counter.
    /// Updated with a lock-free `fetch_or(1, Relaxed)` on every GET.
    pub access_flags: AtomicU8,
    /// Type descriptor for stored value (0 = String, 1 = Hash).
    pub value_type: u8,
    /// Full byte length of the raw key (0 = anonymous legacy key; > 40 = full key in overflow_keys).
    pub key_len: u16,
    /// Inline Small String Optimization (SSO) key buffer (full key if <= 40B, or 40B prefix if > 40B).
    pub inline_key: [u8; INLINE_KEY_CAPACITY],
}

const _: () = assert!(
    std::mem::size_of::<HashEntry>() == 64,
    "HashEntry must be exactly 64 bytes"
);

impl Default for HashEntry {
    #[inline]
    fn default() -> Self {
        Self {
            key_hash: 0,
            slab_block_id: SlabBlockId(0),
            value_len: 0,
            expire_at_secs: 0,
            access_flags: AtomicU8::new(0),
            value_type: VALUE_TYPE_STRING,
            key_len: 0,
            inline_key: [0u8; INLINE_KEY_CAPACITY],
        }
    }
}

impl HashEntry {
    /// Constructs a new persistent `HashEntry` (no TTL, string type) from a key hash and slab descriptor.
    #[inline]
    pub fn new(key_hash: u64, slab_block_id: SlabBlockId, value_len: u32) -> Self {
        Self::with_ttl(key_hash, slab_block_id, value_len, 0)
    }

    /// Constructs a new `HashEntry` (string type) with an explicit expiration timestamp in epoch seconds.
    #[inline]
    pub fn with_ttl(
        key_hash: u64,
        slab_block_id: SlabBlockId,
        value_len: u32,
        expire_at_secs: u32,
    ) -> Self {
        Self::with_type(
            key_hash,
            slab_block_id,
            value_len,
            expire_at_secs,
            VALUE_TYPE_STRING,
        )
    }

    /// Constructs a new `HashEntry` with explicit value type and expiration timestamp.
    #[inline]
    pub fn with_type(
        key_hash: u64,
        slab_block_id: SlabBlockId,
        value_len: u32,
        expire_at_secs: u32,
        value_type: u8,
    ) -> Self {
        Self {
            key_hash,
            slab_block_id,
            value_len,
            expire_at_secs,
            access_flags: AtomicU8::new(0),
            value_type,
            key_len: 0,
            inline_key: [0u8; INLINE_KEY_CAPACITY],
        }
    }

    /// Constructs a new `HashEntry` with explicit value type, expiration timestamp, and raw key bytes.
    #[inline]
    pub fn with_type_and_key(
        key_hash: u64,
        key: &[u8],
        slab_block_id: SlabBlockId,
        value_len: u32,
        expire_at_secs: u32,
        value_type: u8,
    ) -> Self {
        let mut entry = Self::with_type(
            key_hash,
            slab_block_id,
            value_len,
            expire_at_secs,
            value_type,
        );
        entry.set_inline_key(key);
        entry
    }

    /// Populates `key_len` and `inline_key` (up to `INLINE_KEY_CAPACITY` bytes) from `key`.
    #[inline(always)]
    pub fn set_inline_key(&mut self, key: &[u8]) {
        let len = key.len().min(u16::MAX as usize);
        self.key_len = len as u16;
        let copy_len = len.min(INLINE_KEY_CAPACITY);
        self.inline_key[..copy_len].copy_from_slice(&key[..copy_len]);
        if copy_len < INLINE_KEY_CAPACITY {
            self.inline_key[copy_len..].fill(0);
        }
    }

    /// Returns the inline key bytes (up to 40 bytes).
    #[inline(always)]
    pub fn inline_key_prefix(&self) -> &[u8] {
        let len = (self.key_len as usize).min(INLINE_KEY_CAPACITY);
        &self.inline_key[..len]
    }

    /// Returns `true` if this entry has expired relative to `now_secs`.
    #[inline(always)]
    pub fn is_expired(&self, now_secs: u32) -> bool {
        self.expire_at_secs != 0 && self.expire_at_secs <= now_secs
    }

    /// Marks this entry as accessed (S3-FIFO promotion flag).
    ///
    /// **Lock-free fast path**: single `fetch_or` with `Relaxed` ordering —
    /// no pointer mutations, no queue reshuffling, no cache-line bouncing.
    #[inline(always)]
    pub fn mark_accessed(&self) {
        self.access_flags
            .fetch_or(ACCESS_BIT_ACCESSED, Ordering::Relaxed);
    }

    /// Atomically checks and clears the access flag.
    ///
    /// Used by the S3-FIFO eviction pass to decide whether to promote or evict.
    /// Returns `true` if the entry was accessed since the last eviction sweep.
    #[inline(always)]
    pub fn test_and_clear_accessed(&self) -> bool {
        self.access_flags.swap(0, Ordering::Relaxed) > 0
    }

    /// Returns a copyable `TableEntry` snapshot of this entry.
    #[inline(always)]
    pub fn to_snapshot(&self) -> TableEntry {
        TableEntry {
            slab_block_id: self.slab_block_id,
            value_len: self.value_len,
            expire_at_secs: self.expire_at_secs,
            value_type: self.value_type,
        }
    }

    /// Returns `true` if the stored hash matches `candidate_hash`.
    ///
    /// Used after a positive control-byte probe to verify against false positives.
    #[inline(always)]
    pub fn matches(&self, candidate_hash: u64) -> bool {
        self.key_hash == candidate_hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry() -> HashEntry {
        HashEntry::new(0xDEAD_BEEF_CAFE_1234, SlabBlockId(42), 128)
    }

    #[test]
    fn entry_size_is_64_bytes() {
        assert_eq!(std::mem::size_of::<HashEntry>(), 64);
    }

    #[test]
    fn access_flag_roundtrip() {
        let e = make_entry();
        assert!(!e.test_and_clear_accessed()); // initially 0
        e.mark_accessed();
        assert!(e.test_and_clear_accessed()); // now 1
        assert!(!e.test_and_clear_accessed()); // cleared
    }

    #[test]
    fn hash_match() {
        let e = make_entry();
        assert!(e.matches(0xDEAD_BEEF_CAFE_1234));
        assert!(!e.matches(0x0000_0000_0000_0000));
    }
}
