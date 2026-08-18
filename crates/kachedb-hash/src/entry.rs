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
///
/// - `0` — not accessed since last eviction sweep.
/// - `1` — accessed at least once; entry is promotion candidate.
pub const ACCESS_BIT_ACCESSED: u8 = 1;

/// An occupied slot in the KacheDB Swiss Table hash index.
///
/// # Layout (64 bytes, cache-line aligned)
///
/// | Offset | Field            | Size | Description                         |
/// |--------|------------------|------|-------------------------------------|
/// | 0      | `key_hash`       | 8 B  | Full 64-bit key hash (AHash)        |
/// | 8      | `slab_block_id`  | 4 B  | Opaque handle into the slab pool    |
/// | 12     | `value_len`      | 4 B  | Byte length of the stored value     |
/// | 16     | `expire_at_secs` | 4 B  | Absolute expiration epoch seconds   |
/// | 20     | `access_flags`   | 1 B  | S3-FIFO atomic access frequency bit |
/// | 21–63  | `_pad`           | 43 B | Cache-line padding                  |
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
    /// Padding to fill the 64-byte cache line.
    _pad: [u8; 43],
}

const _: () = assert!(
    std::mem::size_of::<HashEntry>() == 64,
    "HashEntry must be exactly 64 bytes"
);

impl HashEntry {
    /// Constructs a new persistent `HashEntry` (no TTL) from a key hash and slab descriptor.
    #[inline]
    pub fn new(key_hash: u64, slab_block_id: SlabBlockId, value_len: u32) -> Self {
        Self::with_ttl(key_hash, slab_block_id, value_len, 0)
    }

    /// Constructs a new `HashEntry` with an explicit expiration timestamp in epoch seconds.
    #[inline]
    pub fn with_ttl(
        key_hash: u64,
        slab_block_id: SlabBlockId,
        value_len: u32,
        expire_at_secs: u32,
    ) -> Self {
        Self {
            key_hash,
            slab_block_id,
            value_len,
            expire_at_secs,
            access_flags: AtomicU8::new(0),
            _pad: [0u8; 43],
        }
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
        self.access_flags.fetch_or(ACCESS_BIT_ACCESSED, Ordering::Relaxed);
    }

    /// Atomically checks and clears the access flag.
    ///
    /// Used by the S3-FIFO eviction pass to decide whether to promote or evict.
    /// Returns `true` if the entry was accessed since the last eviction sweep.
    #[inline(always)]
    pub fn test_and_clear_accessed(&self) -> bool {
        self.access_flags.swap(0, Ordering::Relaxed) > 0
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
