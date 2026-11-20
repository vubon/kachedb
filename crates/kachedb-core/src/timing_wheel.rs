//! `kachedb-core` — Per-core hashed timing wheel for O(1) TTL memory expiration.
//!
//! Buckets produce expiration events. The table validates and unlinks the
//! matching allocation before its owning pool may reclaim it.

use smallvec::SmallVec;

use crate::arena::SlabBlockId;

/// Number of 1-second resolution buckets in the timing wheel (1 hour).
pub const WHEEL_BUCKETS: usize = 3600;

/// Descriptor of an item scheduled for temporal expiration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpireEntry {
    /// 64-bit hash of the key in SwissTable.
    pub key_hash: u64,
    /// Original key bytes, when scheduled for a named table entry.
    pub key: Option<Box<[u8]>>,
    /// Slab block handle.
    pub slab_block_id: SlabBlockId,
    /// Absolute expiration timestamp in epoch seconds.
    pub expire_at_sec: u32,
    /// Generation counter of the slab block when scheduled.
    pub generation: Option<u64>,
}

/// A thread-local hashed timing wheel for deterministic O(1) slab memory reclamation.
pub struct HashedTimingWheel {
    /// Circular ring of buckets containing expiration entries.
    buckets: Vec<SmallVec<[ExpireEntry; 16]>>,
    /// Monotonic second timestamp corresponding to the current ring head.
    current_tick_sec: u64,
}

impl HashedTimingWheel {
    /// Creates a new `HashedTimingWheel` starting at `start_sec`.
    pub fn new(start_sec: u32) -> Self {
        Self {
            buckets: vec![SmallVec::new(); WHEEL_BUCKETS],
            current_tick_sec: u64::from(start_sec),
        }
    }

    /// Returns the current tick timestamp in seconds.
    #[inline(always)]
    pub fn current_tick_sec(&self) -> u64 {
        self.current_tick_sec
    }

    /// Schedules a key hash and slab slot handle for expiration in O(1) time.
    #[inline(always)]
    pub fn schedule(&mut self, key_hash: u64, slot_id: SlabBlockId, expire_at_sec: u32) {
        let generation = crate::registry::get_slot_generation(slot_id);
        self.schedule_entry(ExpireEntry {
            key_hash,
            key: None,
            slab_block_id: slot_id,
            expire_at_sec,
            generation,
        });
    }

    /// Schedules expiration for the exact key, even if another key shares its digest.
    pub fn schedule_key(
        &mut self,
        key_hash: u64,
        key: &[u8],
        slot_id: SlabBlockId,
        expire_at_sec: u32,
    ) {
        let generation = crate::registry::get_slot_generation(slot_id);
        self.schedule_entry(ExpireEntry {
            key_hash,
            key: Some(key.into()),
            slab_block_id: slot_id,
            expire_at_sec,
            generation,
        });
    }

    fn schedule_entry(&mut self, entry: ExpireEntry) {
        let deadline = u64::from(entry.expire_at_sec);
        let bucket_tick = deadline.clamp(
            self.current_tick_sec,
            self.current_tick_sec + WHEEL_BUCKETS as u64 - 1,
        );
        let idx = (bucket_tick % WHEEL_BUCKETS as u64) as usize;
        self.buckets[idx].push(entry);
    }

    /// Yields only due events. A clock jump visits each bucket at most once.
    pub fn advance_expired_entries(&mut self, now_sec: u32, out: &mut Vec<ExpireEntry>) {
        let now_tick = u64::from(now_sec);
        if now_tick < self.current_tick_sec {
            return;
        }

        let end = (now_tick + 1).min(self.current_tick_sec + WHEEL_BUCKETS as u64);
        let mut future = SmallVec::<[ExpireEntry; 8]>::new();
        for tick in self.current_tick_sec..end {
            let idx = (tick % WHEEL_BUCKETS as u64) as usize;
            for entry in self.buckets[idx].drain(..) {
                if entry.expire_at_sec <= now_sec {
                    out.push(entry);
                } else {
                    future.push(entry);
                }
            }
        }
        self.current_tick_sec = now_tick + 1;
        for entry in future {
            self.schedule_entry(entry);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timing_wheel_starts_at_given_timestamp() {
        let wheel = HashedTimingWheel::new(1000);
        assert_eq!(wheel.current_tick_sec(), 1000);
    }

    #[test]
    fn advance_expired_entries_collects_keys() {
        let mut wheel = HashedTimingWheel::new(100);
        let slot1 = SlabBlockId::new(0, 1);
        let slot2 = SlabBlockId::new(0, 2);

        wheel.schedule_key(12345, b"first", slot1, 105);
        wheel.schedule_key(12345, b"", slot2, 108);

        let mut expired = Vec::new();
        wheel.advance_expired_entries(106, &mut expired);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].key_hash, 12345);
        assert_eq!(expired[0].key.as_deref(), Some(b"first".as_slice()));
        assert_eq!(expired[0].slab_block_id, slot1);

        expired.clear();
        wheel.advance_expired_entries(110, &mut expired);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].key_hash, 12345);
        assert_eq!(expired[0].key.as_deref(), Some(b"".as_slice()));
        assert_eq!(expired[0].slab_block_id, slot2);
    }

    #[test]
    fn past_expiry_schedules_at_head() {
        let mut wheel = HashedTimingWheel::new(100);
        let slot = SlabBlockId::new(0, 1);
        wheel.schedule(99, slot, 90);

        let mut expired = Vec::new();
        wheel.advance_expired_entries(101, &mut expired);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].slab_block_id, slot);
    }

    #[test]
    fn timing_wheel_preserves_future_ttl_across_rollovers() {
        let mut wheel = HashedTimingWheel::new(0);
        let slot = SlabBlockId::new(1, 10);

        // Schedule an entry with 7,200s (2 hours) TTL
        wheel.schedule_key(42, b"long-lived", slot, 7200);

        // Advance to 3,599s (the first cycle overflow bucket)
        let mut expired = Vec::new();
        wheel.advance_expired_entries(3599, &mut expired);
        // Must NOT expire at 3,599s!
        assert!(
            expired.is_empty(),
            "entry must not expire on first rollover"
        );

        // Advance to 7,199s
        wheel.advance_expired_entries(7199, &mut expired);
        assert!(expired.is_empty(), "entry must not expire before 7,200s");

        // Advance to 7,200s: now due!
        wheel.advance_expired_entries(7200, &mut expired);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].key.as_deref(), Some(b"long-lived".as_slice()));
        assert_eq!(expired[0].expire_at_sec, 7200);
    }

    #[test]
    fn timing_wheel_handles_large_forward_clock_jump_with_bounded_buckets() {
        let mut wheel = HashedTimingWheel::new(100);
        let slot_due = SlabBlockId::new(1, 1);
        let slot_future = SlabBlockId::new(1, 2);

        // Schedule one entry due at 200s and one future entry at 200,000s
        wheel.schedule_key(1, b"due", slot_due, 200);
        wheel.schedule_key(2, b"future", slot_future, 200_000);

        // Clock jumps from 100 to 100,000s (> 3,600s jump)
        let mut expired = Vec::new();
        wheel.advance_expired_entries(100_000, &mut expired);

        // Only the due entry should be returned
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].key.as_deref(), Some(b"due".as_slice()));
        assert_eq!(wheel.current_tick_sec(), 100_001);

        // Advance to 200,000s: future entry now expires
        expired.clear();
        wheel.advance_expired_entries(200_000, &mut expired);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].key.as_deref(), Some(b"future".as_slice()));
    }
}
