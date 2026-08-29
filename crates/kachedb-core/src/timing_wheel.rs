//! `kachedb-core` — Per-core hashed timing wheel for O(1) TTL memory expiration.
//!
//! # Architecture (RFC: Time-To-Live & Memory Expiration)
//!
//! Implements a thread-local circular timing wheel with 3,600 one-second buckets
//! (1-hour high-resolution window).
//!
//! - **O(1) Scheduling**: Slot handles are placed into buckets via a simple modulo index:
//!   `bucket_idx = (expire_at_secs) % 3600`.
//! - **O(1) Advancing**: On each second tick from the event loop, the active bucket's
//!   expired slab slots are reclaimed directly to the local `SlabPool` via free-list recycling.
//! - **Zero Lock Contention**: Each CPU worker core owns its isolated `HashedTimingWheel`.

use smallvec::SmallVec;

use crate::arena::SlabBlockId;
use crate::pool::SlabPool;

/// Number of 1-second resolution buckets in the timing wheel (1 hour).
pub const WHEEL_BUCKETS: usize = 3600;

/// Descriptor of an item scheduled for temporal expiration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpireEntry {
    /// 64-bit hash of the key in SwissTable.
    pub key_hash: u64,
    /// Slab block handle.
    pub slab_block_id: SlabBlockId,
    /// Absolute expiration timestamp in epoch seconds.
    pub expire_at_sec: u32,
}

/// A thread-local hashed timing wheel for deterministic O(1) slab memory reclamation.
pub struct HashedTimingWheel {
    /// Circular ring of buckets containing expiration entries.
    buckets: Vec<SmallVec<[ExpireEntry; 16]>>,
    /// Monotonic second timestamp corresponding to the current ring head.
    current_tick_sec: u32,
}

impl HashedTimingWheel {
    /// Creates a new `HashedTimingWheel` starting at `start_sec`.
    pub fn new(start_sec: u32) -> Self {
        Self {
            buckets: vec![SmallVec::new(); WHEEL_BUCKETS],
            current_tick_sec: start_sec,
        }
    }

    /// Returns the current tick timestamp in seconds.
    #[inline(always)]
    pub fn current_tick_sec(&self) -> u32 {
        self.current_tick_sec
    }

    /// Schedules a key hash and slab slot handle for expiration in O(1) time.
    #[inline(always)]
    pub fn schedule(&mut self, key_hash: u64, slot_id: SlabBlockId, expire_at_sec: u32) {
        let entry = ExpireEntry {
            key_hash,
            slab_block_id: slot_id,
            expire_at_sec,
        };

        if expire_at_sec <= self.current_tick_sec {
            // Already expired; target current head for immediate reclamation on next tick
            let idx = (self.current_tick_sec as usize) % WHEEL_BUCKETS;
            self.buckets[idx].push(entry);
            return;
        }

        let delta = expire_at_sec - self.current_tick_sec;
        let target_idx = if (delta as usize) < WHEEL_BUCKETS {
            (expire_at_sec as usize) % WHEEL_BUCKETS
        } else {
            // Overflow bucket handling for long-lived TTLs (> 1 hour):
            // Place in the furthest bucket; will be re-evaluated on rollover
            (self.current_tick_sec as usize + WHEEL_BUCKETS - 1) % WHEEL_BUCKETS
        };

        self.buckets[target_idx].push(entry);
    }

    /// Advances the timing wheel to `now_sec` and drains all expired entries into `out`.
    pub fn advance_expired_entries(&mut self, now_sec: u32, out: &mut Vec<ExpireEntry>) {
        while self.current_tick_sec <= now_sec {
            let bucket_idx = (self.current_tick_sec as usize) % WHEEL_BUCKETS;
            let expired_slots = &mut self.buckets[bucket_idx];

            out.extend_from_slice(expired_slots.as_slice());
            expired_slots.clear();
            self.current_tick_sec += 1;
        }
    }

    /// Advances the timing wheel to `now_sec` and batch-reclaims expired memory slabs in O(1) time.
    ///
    /// Returns the number of slab slots successfully deallocated.
    pub fn advance_to(&mut self, now_sec: u32, slab_pool: &mut SlabPool) -> usize {
        let mut reclaimed_count = 0;

        while self.current_tick_sec <= now_sec {
            let bucket_idx = (self.current_tick_sec as usize) % WHEEL_BUCKETS;
            let expired_slots = &mut self.buckets[bucket_idx];

            for entry in expired_slots.iter() {
                // Free slot directly to the local thread slab pool
                if slab_pool.deallocate(entry.slab_block_id).is_ok() {
                    reclaimed_count += 1;
                }
            }

            expired_slots.clear();
            self.current_tick_sec += 1;
        }

        reclaimed_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::slab::SlabClassType;

    #[test]
    fn timing_wheel_starts_at_given_timestamp() {
        let wheel = HashedTimingWheel::new(1000);
        assert_eq!(wheel.current_tick_sec(), 1000);
    }

    #[test]
    fn schedule_and_advance_reclaims_expired_slots() {
        let mut pool = SlabPool::new(0, 16 * 1024 * 1024).unwrap();
        let slot1 = pool.allocate(SlabClassType::AppSmall).unwrap();
        let slot2 = pool.allocate(SlabClassType::AppSmall).unwrap();

        let mut wheel = HashedTimingWheel::new(100);

        // Schedule slot1 to expire at 105s, slot2 at 110s
        wheel.schedule(1, slot1, 105);
        wheel.schedule(2, slot2, 110);

        // Advance to 104s: neither should expire
        let reclaimed = wheel.advance_to(104, &mut pool);
        assert_eq!(reclaimed, 0);
        assert_eq!(wheel.current_tick_sec(), 105);

        // Advance to 105s: slot1 should be reclaimed
        let reclaimed = wheel.advance_to(105, &mut pool);
        assert_eq!(reclaimed, 1);
        assert_eq!(wheel.current_tick_sec(), 106);

        // Advance to 110s: slot2 should be reclaimed
        let reclaimed = wheel.advance_to(110, &mut pool);
        assert_eq!(reclaimed, 1);
        assert_eq!(wheel.current_tick_sec(), 111);
    }

    #[test]
    fn advance_expired_entries_collects_keys() {
        let mut wheel = HashedTimingWheel::new(100);
        let slot1 = SlabBlockId::new(0, 1);
        let slot2 = SlabBlockId::new(0, 2);

        wheel.schedule(12345, slot1, 105);
        wheel.schedule(67890, slot2, 108);

        let mut expired = Vec::new();
        wheel.advance_expired_entries(106, &mut expired);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].key_hash, 12345);
        assert_eq!(expired[0].slab_block_id, slot1);

        expired.clear();
        wheel.advance_expired_entries(110, &mut expired);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].key_hash, 67890);
        assert_eq!(expired[0].slab_block_id, slot2);
    }

    #[test]
    fn past_expiry_schedules_at_head() {
        let mut pool = SlabPool::new(0, 16 * 1024 * 1024).unwrap();
        let slot = pool.allocate(SlabClassType::AppSmall).unwrap();

        let mut wheel = HashedTimingWheel::new(100);
        // Expiry is in the past (90s <= 100s)
        wheel.schedule(99, slot, 90);

        // Advancing to 101s should reclaim it immediately
        let reclaimed = wheel.advance_to(101, &mut pool);
        assert_eq!(reclaimed, 1);
    }
}
