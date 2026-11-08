//! `kachedb-hash` — High-concurrency Sharded Swiss Table.
//!
//! Partitions the 64-bit XXH3 keyspace into 256 independent micro-shards,
//! each guarded by an ultra-low-overhead [`parking_lot::RwLock`].
//!
//! This enables true multi-core parallel scaling across all worker threads
//! with near-zero lock contention and 100% global key visibility.

use parking_lot::RwLock;

use crate::{
    entry::TableEntry,
    glob::CompiledPattern,
    table::{SwissTable, next_bit_reversed_cursor},
};
use kachedb_core::SlabBlockId;

/// Number of striped shards. Must be a power of two.
pub const NUM_SHARDS: usize = 256;
const SHARD_MASK: usize = NUM_SHARDS - 1;

/// Default `COUNT` batch size hint for `SCAN`.
pub const DEFAULT_SCAN_COUNT: usize = 10;
/// Maximum clamped `COUNT` batch size for `SCAN`.
pub const MAX_SCAN_COUNT: usize = 10_000;
/// Maximum home buckets inspected in a single `SCAN` step before returning cursor.
const MAX_BUCKETS_PER_STEP: usize = 4096;
/// Maximum home buckets inspected under a single shard read lock acquisition (~2.5 µs).
const SHARD_BATCH_BUCKETS: usize = 256;

/// Cache-line aligned shard container.
///
/// `#[repr(align(64))]` guarantees each shard begins on a dedicated 64-byte
/// CPU cache line boundary. This completely eliminates false sharing between CPU cores
/// when multiple worker threads concurrently read or write adjacent shards.
#[repr(align(64))]
pub struct Shard {
    table: RwLock<SwissTable>,
}

impl Shard {
    /// Creates a new cache-aligned shard with the specified initial slot capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            table: RwLock::new(SwissTable::with_capacity(capacity)),
        }
    }
}

/// A high-concurrency, lock-striped hash index powered by Swiss Tables.
///
/// Thread-safe for simultaneous multi-reader and multi-writer access from
/// arbitrary worker threads.
pub struct ShardedSwissTable {
    shards: Box<[Shard]>,
}

impl Default for ShardedSwissTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Default initial capacity per shard (1,024 slots × 64 B = 64 KB per shard, 16 MB total across 256 shards).
pub const DEFAULT_PER_SHARD_CAPACITY: usize = 1024;

impl ShardedSwissTable {
    /// Creates a new `ShardedSwissTable` with 256 shards and 1,024 slots per shard (16 MB starting memory).
    pub fn new() -> Self {
        Self::with_total_capacity(DEFAULT_PER_SHARD_CAPACITY * NUM_SHARDS)
    }

    /// Creates a new `ShardedSwissTable` with a total target capacity across all shards.
    pub fn with_total_capacity(total_capacity: usize) -> Self {
        let per_shard_cap = (total_capacity / NUM_SHARDS).max(64);
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(Shard::new(per_shard_cap));
        }
        Self {
            shards: shards.into_boxed_slice(),
        }
    }

    /// Returns the shard index for a given 64-bit hash.
    #[inline(always)]
    fn shard_idx(hash: u64) -> usize {
        // Use upper bits of hash to avoid correlation with H1/H2 in SwissTable
        ((hash >> 32) as usize) & SHARD_MASK
    }

    /// Point lookup with TTL expiry validation.
    ///
    /// Acquires a short-lived read lock on the target cache-aligned shard (~1.5 ns).
    #[inline(always)]
    pub fn lookup_checked(&self, hash: u64, now_sec: u32) -> Option<TableEntry> {
        let idx = Self::shard_idx(hash);
        let shard = self.shards[idx].table.read();
        shard.lookup_checked(hash, now_sec).map(|e| e.to_snapshot())
    }

    /// Point lookup without TTL validation (for raw metadata inspection).
    #[inline(always)]
    pub fn lookup(&self, hash: u64) -> Option<TableEntry> {
        let idx = Self::shard_idx(hash);
        let shard = self.shards[idx].table.read();
        shard.lookup(hash).map(|e| e.to_snapshot())
    }

    /// Inserts or updates a key entry with optional TTL.
    ///
    /// If the key already existed, returns the previous `SlabBlockId` so
    /// the caller can immediately recycle the old slab memory slot.
    #[inline(always)]
    pub fn insert_with_ttl(
        &self,
        hash: u64,
        block_id: SlabBlockId,
        value_len: u32,
        expire_at_secs: u32,
    ) -> Option<SlabBlockId> {
        let idx = Self::shard_idx(hash);
        let mut shard = self.shards[idx].table.write();
        shard
            .insert_with_ttl(hash, block_id, value_len, expire_at_secs)
            .ok()
            .flatten()
    }

    /// Inserts or updates a key entry with raw key bytes and optional TTL.
    #[inline(always)]
    pub fn insert_with_ttl_and_key(
        &self,
        hash: u64,
        key: &[u8],
        block_id: SlabBlockId,
        value_len: u32,
        expire_at_secs: u32,
    ) -> Option<SlabBlockId> {
        let idx = Self::shard_idx(hash);
        let mut shard = self.shards[idx].table.write();
        shard
            .insert_with_ttl_and_key(hash, key, block_id, value_len, expire_at_secs)
            .ok()
            .flatten()
    }

    /// Inserts or updates a key entry without TTL.
    #[inline(always)]
    pub fn insert(&self, hash: u64, block_id: SlabBlockId, value_len: u32) -> Option<SlabBlockId> {
        self.insert_with_ttl(hash, block_id, value_len, 0)
    }

    /// Inserts or updates a key entry with raw key bytes without TTL.
    #[inline(always)]
    pub fn insert_with_key(
        &self,
        hash: u64,
        key: &[u8],
        block_id: SlabBlockId,
        value_len: u32,
    ) -> Option<SlabBlockId> {
        self.insert_with_ttl_and_key(hash, key, block_id, value_len, 0)
    }

    /// Inserts or updates a key entry with explicit expiration timestamp and value type.
    #[inline(always)]
    #[allow(clippy::result_unit_err)]
    pub fn insert_typed(
        &self,
        hash: u64,
        block_id: SlabBlockId,
        value_len: u32,
        expire_at_secs: u32,
        value_type: u8,
    ) -> Result<Option<SlabBlockId>, ()> {
        let idx = Self::shard_idx(hash);
        let mut shard = self.shards[idx].table.write();
        shard.insert_typed(hash, block_id, value_len, expire_at_secs, value_type)
    }

    /// Inserts or updates a key entry with raw key bytes, explicit expiration timestamp, and value type.
    #[inline(always)]
    #[allow(clippy::result_unit_err)]
    pub fn insert_typed_with_key(
        &self,
        hash: u64,
        key: &[u8],
        block_id: SlabBlockId,
        value_len: u32,
        expire_at_secs: u32,
        value_type: u8,
    ) -> Result<Option<SlabBlockId>, ()> {
        let idx = Self::shard_idx(hash);
        let mut shard = self.shards[idx].table.write();
        shard.insert_typed_with_key(hash, key, block_id, value_len, expire_at_secs, value_type)
    }

    /// Executes a closure holding the read lock on the shard responsible for `hash`.
    #[inline(always)]
    pub fn with_shard<R>(&self, hash: u64, f: impl FnOnce(&SwissTable) -> R) -> R {
        let idx = Self::shard_idx(hash);
        let shard = self.shards[idx].table.read();
        f(&shard)
    }

    /// Executes a closure holding the write lock on the shard responsible for `hash`.
    #[inline(always)]
    pub fn with_shard_mut<R>(&self, hash: u64, f: impl FnOnce(&mut SwissTable) -> R) -> R {
        let idx = Self::shard_idx(hash);
        let mut shard = self.shards[idx].table.write();
        f(&mut shard)
    }

    /// Removes an entry by its 64-bit hash.
    ///
    /// Returns the removed `TableEntry` if found.
    #[inline(always)]
    pub fn remove(&self, hash: u64) -> Option<TableEntry> {
        let idx = Self::shard_idx(hash);
        let mut shard = self.shards[idx].table.write();
        shard.remove(hash)
    }

    /// Removes an entry only if its slab block ID matches `expected_block_id`.
    ///
    /// Protects against double-free when S3-FIFO eviction or explicit DEL
    /// occurs before the TimingWheel expiry bucket triggers.
    #[inline(always)]
    pub fn remove_if_matching(
        &self,
        hash: u64,
        expected_block_id: SlabBlockId,
    ) -> Option<TableEntry> {
        let idx = Self::shard_idx(hash);
        let mut shard = self.shards[idx].table.write();
        shard.remove_if_matching(hash, expected_block_id)
    }

    /// Updates the TTL on an existing key in its designated shard.
    #[inline(always)]
    pub fn update_ttl(&self, hash: u64, expire_at_secs: u32, now_sec: u32) -> bool {
        let idx = Self::shard_idx(hash);
        let mut shard = self.shards[idx].table.write();
        shard.update_ttl(hash, expire_at_secs, now_sec)
    }

    /// Retrieves the remaining TTL in seconds (-2 missing/expired, -1 persistent, >=0 seconds).
    #[inline(always)]
    pub fn get_ttl(&self, hash: u64, now_sec: u32) -> i64 {
        let idx = Self::shard_idx(hash);
        let shard = self.shards[idx].table.read();
        shard.get_ttl(hash, now_sec)
    }

    /// Removes TTL, converting key into a persistent key.
    #[inline(always)]
    pub fn persist(&self, hash: u64, now_sec: u32) -> bool {
        let idx = Self::shard_idx(hash);
        let mut shard = self.shards[idx].table.write();
        shard.persist(hash, now_sec)
    }

    /// Returns the total count of live entries across all 256 shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.table.read().len()).sum()
    }

    /// Returns `true` if all shards are empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of shards in this sharded table.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Clears all entries across all shards and returns all removed slab block IDs.
    pub fn clear(&self) -> Vec<SlabBlockId> {
        let mut removed_blocks = Vec::new();
        for shard in self.shards.iter() {
            let mut table = shard.table.write();
            for (_, entry) in table.live_entries() {
                removed_blocks.push(entry.slab_block_id);
            }
            table.clear();
        }
        removed_blocks
    }

    /// Returns a snapshot of all live entries in the specified shard.
    pub fn snapshot_shard(&self, shard_idx: usize) -> Vec<(u64, TableEntry)> {
        if shard_idx < self.shards.len() {
            self.shards[shard_idx].table.read().live_entries()
        } else {
            Vec::new()
        }
    }

    /// Returns a snapshot of all live `(key_hash, key_bytes, TableEntry)` tuples in the specified shard.
    pub fn snapshot_shard_with_keys(&self, shard_idx: usize) -> Vec<(u64, Vec<u8>, TableEntry)> {
        if shard_idx < self.shards.len() {
            self.shards[shard_idx].table.read().live_entries_with_keys()
        } else {
            Vec::new()
        }
    }

    /// Returns the total count of anonymous (`key_len == 0`) live keys across all shards.
    pub fn anonymous_key_count(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.table.read().anonymous_key_count())
            .sum()
    }

    /// Bounded-work stateless cursor scan across the 256 shards.
    ///
    /// - Cursor layout: `((local_cursor as u64) << 8) | (shard_idx as u64)`.
    /// - Acquires each shard's read lock once per batch (up to `SHARD_BATCH_BUCKETS = 256` home buckets).
    /// - Skips empty shards (`shard.is_empty()`) in $O(1)$ without charging `buckets_visited`.
    /// - Invokes `on_key` for each matched live key and returns the next 64-bit cursor (`0` when complete).
    pub fn scan_batch(
        &self,
        cursor: u64,
        pattern: Option<&CompiledPattern<'_>>,
        count: usize,
        type_filter: Option<u8>,
        now_sec: u32,
        mut on_key: impl FnMut(&[u8]),
    ) -> u64 {
        let count = count.clamp(1, MAX_SCAN_COUNT);
        let mut shard_idx = (cursor & 0xFF) as usize;
        let mut local_cursor = (cursor >> 8) as usize;

        if shard_idx >= NUM_SHARDS {
            return 0;
        }

        let mut emitted = 0usize;
        let mut buckets_visited = 0usize;

        while shard_idx < NUM_SHARDS {
            {
                let shard = self.shards[shard_idx].table.read();
                if shard.is_empty() {
                    shard_idx += 1;
                    local_cursor = 0;
                    continue;
                }

                let cap = shard.capacity();
                let mut shard_buckets = 0usize;
                loop {
                    emitted += shard.scan_home_bucket(
                        local_cursor,
                        pattern,
                        type_filter,
                        now_sec,
                        &mut on_key,
                    );
                    local_cursor = next_bit_reversed_cursor(local_cursor, cap);
                    buckets_visited += 1;
                    shard_buckets += 1;

                    if local_cursor == 0 {
                        shard_idx += 1;
                        break;
                    }
                    if emitted >= count
                        || buckets_visited >= MAX_BUCKETS_PER_STEP
                        || shard_buckets >= SHARD_BATCH_BUCKETS
                    {
                        break;
                    }
                }
            }

            if emitted >= count || buckets_visited >= MAX_BUCKETS_PER_STEP {
                if local_cursor == 0 {
                    while shard_idx < NUM_SHARDS && self.shards[shard_idx].table.read().is_empty() {
                        shard_idx += 1;
                    }
                }
                break;
            }
        }

        if shard_idx >= NUM_SHARDS {
            0
        } else {
            ((local_cursor as u64) << 8) | (shard_idx as u64)
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::hash_key;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_sharded_basic_insert_lookup_remove() {
        let table = ShardedSwissTable::new();
        let h = hash_key(b"user:42");
        let block_id = SlabBlockId::new(0, 1);

        // Insert
        let old = table.insert_with_ttl(h, block_id, 128, 0);
        assert_eq!(old, None);
        assert_eq!(table.len(), 1);

        // Lookup
        let entry = table.lookup_checked(h, 0).expect("key should exist");
        assert_eq!(entry.slab_block_id, block_id);
        assert_eq!(entry.value_len, 128);

        // Update with new block ID
        let new_block_id = SlabBlockId::new(0, 2);
        let old2 = table.insert_with_ttl(h, new_block_id, 256, 0);
        assert_eq!(old2, Some(block_id));

        // Lookup updated
        let entry2 = table.lookup_checked(h, 0).expect("key should exist");
        assert_eq!(entry2.slab_block_id, new_block_id);
        assert_eq!(entry2.value_len, 256);

        // Remove
        let removed = table.remove(h).expect("remove should succeed");
        assert_eq!(removed.slab_block_id, new_block_id);
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn test_sharded_multi_threaded_concurrency() {
        let table = Arc::new(ShardedSwissTable::new());
        let num_threads = 8;
        let ops_per_thread = 10_000;
        let mut handles = Vec::new();

        // Spawn writers & readers concurrently
        for t in 0..num_threads {
            let table_clone = Arc::clone(&table);
            handles.push(thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let key = format!("thread_{t}_key_{i}");
                    let h = hash_key(key.as_bytes());
                    let block_id = SlabBlockId::new(t as u16, i as u16);

                    // Insert
                    table_clone.insert_with_ttl(h, block_id, 64, 0);

                    // Immediate read
                    let entry = table_clone.lookup_checked(h, 0).expect("must find key");
                    assert_eq!(entry.slab_block_id, block_id);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(table.len(), num_threads * ops_per_thread);

        // Verify all keys from main thread
        for t in 0..num_threads {
            for i in 0..ops_per_thread {
                let key = format!("thread_{t}_key_{i}");
                let h = hash_key(key.as_bytes());
                assert!(table.lookup_checked(h, 0).is_some());
            }
        }
    }

    #[test]
    fn test_sharded_ttl_operations() {
        let table = ShardedSwissTable::new();
        let h = hash_key(b"session:user_999");
        let block_id = SlabBlockId::new(1, 10);

        table.insert_with_ttl(h, block_id, 128, 500);
        assert_eq!(table.get_ttl(h, 100), 400);

        // Update TTL
        assert!(table.update_ttl(h, 600, 100));
        assert_eq!(table.get_ttl(h, 100), 500);

        // Persist
        assert!(table.persist(h, 100));
        assert_eq!(table.get_ttl(h, 100), -1);
        assert!(!table.persist(h, 100)); // Already persistent

        // Missing
        let missing = hash_key(b"missing");
        assert_eq!(table.get_ttl(missing, 100), -2);
        assert!(!table.update_ttl(missing, 500, 100));
    }

    #[test]
    fn test_sharded_clear() {
        let table = ShardedSwissTable::new();
        assert_eq!(table.len(), 0);
        assert!(table.is_empty());

        for i in 0..100 {
            let k = format!("key_{i}");
            let h = hash_key(k.as_bytes());
            table.insert(h, SlabBlockId::new(1, i as u16), 64);
        }

        assert_eq!(table.len(), 100);
        assert!(!table.is_empty());

        let cleared = table.clear();
        assert_eq!(cleared.len(), 100);
        assert_eq!(table.len(), 0);
        assert!(table.is_empty());
    }

    #[test]
    fn test_sharded_scan_batch_empty_and_populated() {
        use crate::entry::{VALUE_TYPE_HASH, VALUE_TYPE_STRING};
        use std::collections::HashSet;

        let table = ShardedSwissTable::new();

        // 1. Empty DB scan must complete in a single call and return cursor 0
        let mut empty_out = Vec::new();
        let next = table.scan_batch(0, None, 10, None, 100, |k| empty_out.push(k.to_vec()));
        assert_eq!(next, 0);
        assert!(empty_out.is_empty());

        // 2. Populate 200 string keys (including > 40B overflow keys) and 50 hash keys
        let mut expected_all = HashSet::new();
        let mut expected_users = HashSet::new();
        let mut expected_hashes = HashSet::new();

        for i in 0..150 {
            let k = format!("user:{i:04}").into_bytes();
            let h = hash_key(&k);
            table.insert_with_key(h, &k, SlabBlockId::new(0, i as u16), 32);
            expected_all.insert(k.clone());
            expected_users.insert(k);
        }
        for i in 0..50 {
            let k = format!(
                "long:document:namespace:hierarchy:segment:{i:04}:payload_key_exceeding_40_bytes"
            )
            .into_bytes();
            assert!(k.len() > 40);
            let h = hash_key(&k);
            table.insert_with_key(h, &k, SlabBlockId::new(1, i as u16), 64);
            expected_all.insert(k);
        }
        for i in 0..50 {
            let k = format!("hash:profile:{i:04}").into_bytes();
            let h = hash_key(&k);
            table
                .insert_typed_with_key(
                    h,
                    &k,
                    SlabBlockId::new(2, i as u16),
                    128,
                    0,
                    VALUE_TYPE_HASH,
                )
                .unwrap();
            expected_all.insert(k.clone());
            expected_hashes.insert(k);
        }
        // Also insert an already-expired key at expire_at = 50 (now_sec = 100)
        let exp_key = b"user:expired";
        table.insert_with_ttl_and_key(hash_key(exp_key), exp_key, SlabBlockId::new(3, 1), 16, 50);

        // Full scan with COUNT 7
        let mut collected = HashSet::new();
        let mut cursor = 0u64;
        loop {
            cursor = table.scan_batch(cursor, None, 7, None, 100, |k| {
                collected.insert(k.to_vec());
            });
            assert!(cursor < (1u64 << 53), "cursor must remain < 2^53");
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(collected, expected_all);

        // Filtered scan with MATCH user:*
        let pat = CompiledPattern::new(b"user:*");
        let mut matched_users = HashSet::new();
        let mut cursor = 0u64;
        loop {
            cursor = table.scan_batch(cursor, Some(&pat), 15, None, 100, |k| {
                matched_users.insert(k.to_vec());
            });
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(matched_users, expected_users);

        // Filtered scan with TYPE hash
        let mut matched_hashes = HashSet::new();
        let mut cursor = 0u64;
        loop {
            cursor = table.scan_batch(cursor, None, 20, Some(VALUE_TYPE_HASH), 100, |k| {
                matched_hashes.insert(k.to_vec());
            });
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(matched_hashes, expected_hashes);

        // Filtered scan with TYPE string
        let mut matched_strings = HashSet::new();
        let mut cursor = 0u64;
        loop {
            cursor = table.scan_batch(cursor, None, 25, Some(VALUE_TYPE_STRING), 100, |k| {
                matched_strings.insert(k.to_vec());
            });
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(matched_strings.len(), 200);
    }
}
