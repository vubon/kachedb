//! `kachedb-hash` — High-concurrency Sharded Swiss Table.
//!
//! Partitions the 64-bit AHash keyspace into 256 independent micro-shards,
//! each guarded by an ultra-low-overhead [`parking_lot::RwLock`].
//!
//! This enables true multi-core parallel scaling across all worker threads
//! with near-zero lock contention and 100% global key visibility.

use parking_lot::RwLock;

use crate::{entry::TableEntry, table::SwissTable};
use kachedb_core::SlabBlockId;

/// Number of striped shards. Must be a power of two.
pub const NUM_SHARDS: usize = 256;
const SHARD_MASK: usize = NUM_SHARDS - 1;

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

    /// Inserts or updates a key entry without TTL.
    #[inline(always)]
    pub fn insert(&self, hash: u64, block_id: SlabBlockId, value_len: u32) -> Option<SlabBlockId> {
        self.insert_with_ttl(hash, block_id, value_len, 0)
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

    /// Returns a snapshot of all live entries in the specified shard.
    pub fn snapshot_shard(&self, shard_idx: usize) -> Vec<(u64, TableEntry)> {
        if shard_idx < self.shards.len() {
            self.shards[shard_idx].table.read().live_entries()
        } else {
            Vec::new()
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
}
