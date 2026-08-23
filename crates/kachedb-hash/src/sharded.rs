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

/// A high-concurrency, lock-striped hash index powered by Swiss Tables.
///
/// Thread-safe for simultaneous multi-reader and multi-writer access from
/// arbitrary worker threads.
pub struct ShardedSwissTable {
    shards: Box<[RwLock<SwissTable>]>,
}

impl Default for ShardedSwissTable {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardedSwissTable {
    /// Creates a new `ShardedSwissTable` with 256 shards and default per-shard capacity.
    pub fn new() -> Self {
        Self::with_total_capacity(65_536 * NUM_SHARDS)
    }

    /// Creates a new `ShardedSwissTable` with a total target capacity across all shards.
    pub fn with_total_capacity(total_capacity: usize) -> Self {
        let per_shard_cap = (total_capacity / NUM_SHARDS).max(64);
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(RwLock::new(SwissTable::with_capacity(per_shard_cap)));
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
    /// Acquires a short-lived read lock on the target shard (~3 ns).
    #[inline]
    pub fn lookup_checked(&self, hash: u64, now_sec: u32) -> Option<TableEntry> {
        let idx = Self::shard_idx(hash);
        let shard = self.shards[idx].read();
        shard.lookup_checked(hash, now_sec).map(|e| e.to_snapshot())
    }

    /// Point lookup without TTL validation (for raw metadata inspection).
    #[inline]
    pub fn lookup(&self, hash: u64) -> Option<TableEntry> {
        let idx = Self::shard_idx(hash);
        let shard = self.shards[idx].read();
        shard.lookup(hash).map(|e| e.to_snapshot())
    }

    /// Inserts or updates a key entry with optional TTL.
    ///
    /// If the key already existed, returns the previous `SlabBlockId` so
    /// the caller can immediately recycle the old slab memory slot.
    #[inline]
    pub fn insert_with_ttl(
        &self,
        hash: u64,
        block_id: SlabBlockId,
        value_len: u32,
        expire_at_secs: u32,
    ) -> Option<SlabBlockId> {
        let idx = Self::shard_idx(hash);
        let mut shard = self.shards[idx].write();
        shard
            .insert_with_ttl(hash, block_id, value_len, expire_at_secs)
            .ok()
            .flatten()
    }

    /// Removes an entry by its 64-bit hash.
    ///
    /// Returns the removed `TableEntry` if found.
    #[inline]
    pub fn remove(&self, hash: u64) -> Option<TableEntry> {
        let idx = Self::shard_idx(hash);
        let mut shard = self.shards[idx].write();
        shard.remove(hash)
    }

    /// Returns the total count of live entries across all 256 shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    /// Returns `true` if all shards are empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
}
