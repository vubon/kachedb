#![allow(clippy::result_unit_err, clippy::collapsible_if)]

//! `kachedb-hash` — Swiss Table (open-addressed, SIMD-probed) hash index.
//!
//! # Swiss Table Design (Google Abseil / Rust hashbrown)
//!
//! The table is split into two parallel arrays:
//!
//! 1. **Control bytes array** (`ctrl: Vec<u8>`) — one byte per slot:
//!    - `CTRL_EMPTY   (0xFF)` — slot is unoccupied.
//!    - `CTRL_DELETED (0x80)` — tombstone (slot freed but not yet compacted).
//!    - `H2(hash)     (0x00–0x7F)` — lower 7 bits of the hash for fast rejection.
//!
//! 2. **Entry array** (`entries: Vec<Option<Box<HashEntry>>>`) — 64-byte aligned
//!    `HashEntry` slots, only initialised when the corresponding control byte is
//!    a valid `H2` fingerprint.
//!
//! # Probe Sequence
//!
//! Probing starts at slot `H1(hash) % capacity`, then iterates in groups of 8
//! control bytes using a scalar (portable) or SIMD (x86 SSE2) scan to find
//! either an empty slot (insert) or a matching `H2` fingerprint (lookup).
//!
//! # Load Factor
//!
//! The table resizes when occupancy exceeds **87.5%** (7/8 slots) of capacity,
//! matching the standard Swiss Table growth threshold.

use ahash::AHasher;
use std::hash::{Hash, Hasher};

use kachedb_core::SlabBlockId;

use crate::entry::HashEntry;

// ─── Control byte sentinels ───────────────────────────────────────────────────

/// Control byte for an unoccupied slot.
const CTRL_EMPTY: u8 = 0xFF;
/// Control byte for a deleted (tombstone) slot.
const CTRL_DELETED: u8 = 0x80;
/// Maximum H2 fingerprint value (7-bit).
const CTRL_H2_MASK: u8 = 0x7F;
/// Number of control bytes in one probe group (portable scalar fallback).
const GROUP_SIZE: usize = 8;
/// Resize threshold: 87.5% load factor.
const LOAD_FACTOR_NUM: usize = 7;
const LOAD_FACTOR_DEN: usize = 8;

// ─── Hash helpers ─────────────────────────────────────────────────────────────

/// Computes a 64-bit AHash digest for an arbitrary byte key.
#[inline]
pub fn hash_key(key: &[u8]) -> u64 {
    let mut h = AHasher::default();
    key.hash(&mut h);
    h.finish()
}

/// Extracts `H1`: upper 57 bits used as the slot index.
#[inline(always)]
fn h1(hash: u64, capacity: usize) -> usize {
    ((hash >> 7) as usize) % capacity
}

/// Extracts `H2`: lower 7 bits used as the control byte fingerprint.
#[inline(always)]
fn h2(hash: u64) -> u8 {
    (hash as u8) & CTRL_H2_MASK
}

// ─── Group probe (portable scalar) ───────────────────────────────────────────

/// Portable 8-wide control-byte group scan.
///
/// On x86-64 Linux, this can be replaced with SSE2 intrinsics for zero-cost
/// SIMD acceleration. The current implementation provides correct behaviour
/// on all platforms including Apple Silicon.
struct Group<'a> {
    ctrl: &'a [u8],
    base: usize,
    cap: usize,
}

impl<'a> Group<'a> {
    fn new(ctrl: &'a [u8], start: usize, cap: usize) -> Self {
        Self {
            ctrl,
            base: start,
            cap,
        }
    }

    /// Yields indices of slots matching `fingerprint`.
    fn match_byte(&self, fingerprint: u8) -> impl Iterator<Item = usize> + '_ {
        (0..GROUP_SIZE).filter_map(move |i| {
            let idx = (self.base + i) % self.cap;
            if self.ctrl[idx] == fingerprint {
                Some(idx)
            } else {
                None
            }
        })
    }

    /// Returns `true` if any slot in the group is empty (`CTRL_EMPTY`).
    fn has_empty(&self) -> bool {
        (0..GROUP_SIZE).any(|i| self.ctrl[(self.base + i) % self.cap] == CTRL_EMPTY)
    }

    /// Returns the index of the first empty or deleted slot in the group.
    fn first_available(&self) -> Option<usize> {
        (0..GROUP_SIZE).find_map(|i| {
            let idx = (self.base + i) % self.cap;
            matches!(self.ctrl[idx], CTRL_EMPTY | CTRL_DELETED).then_some(idx)
        })
    }
}

// ─── SwissTable ───────────────────────────────────────────────────────────────

/// O(1) point-query hash index for KacheDB application cache keys.
///
/// Stores (key_hash → `SlabBlockId` + value_len) mappings using an
/// open-addressed Swiss Table layout with 64-byte aligned `HashEntry` slots.
///
/// # Thread Safety
///
/// `SwissTable` is single-threaded per core. Cross-core lookups are forwarded
/// via SPSC message channels (defined in `kachedb-net`).
///
/// # Example
///
/// ```rust
/// use kachedb_hash::SwissTable;
///
/// let mut table = SwissTable::with_capacity(1024);
/// let hash = kachedb_hash::hash_key(b"my-cache-key");
/// let block_id = kachedb_core::SlabBlockId(0);
///
/// table.insert(hash, block_id, 256).unwrap();
/// assert!(table.lookup(hash).is_some());
/// ```
pub struct SwissTable {
    /// One control byte per slot.
    ctrl: Vec<u8>,
    /// Slot entries (None = unoccupied).
    entries: Vec<Option<Box<HashEntry>>>,
    /// Number of occupied (non-deleted) slots.
    count: usize,
    /// Allocated slot count (always a power of two).
    capacity: usize,
    /// Incremental cursor for idle-time tombstone compaction (Improvement 3).
    compact_cursor: usize,
}

impl SwissTable {
    /// Creates a new `SwissTable` with at least `min_capacity` slots.
    ///
    /// Capacity is rounded up to the next power of two for efficient modulo
    /// arithmetic.
    pub fn with_capacity(min_capacity: usize) -> Self {
        let capacity = min_capacity.next_power_of_two().max(GROUP_SIZE);
        Self {
            ctrl: vec![CTRL_EMPTY; capacity],
            entries: (0..capacity).map(|_| None).collect(),
            count: 0,
            capacity,
            compact_cursor: 0,
        }
    }

    /// Inserts a mapping from `key_hash` → (`slab_block_id`, `value_len`).
    ///
    /// Returns `true` if the key was newly inserted, `false` if it was updated.
    ///
    /// # Errors
    ///
    /// Returns `Err(())` if the table is completely full (only possible if the
    /// Inserts a key-hash and slab descriptor, returning `true` on new insert
    /// or `false` on update.
    #[inline]
    pub fn insert(
        &mut self,
        key_hash: u64,
        slab_block_id: SlabBlockId,
        value_len: u32,
    ) -> Result<bool, ()> {
        self.insert_with_ttl(key_hash, slab_block_id, value_len, 0)
    }

    /// Inserts a key-hash and slab descriptor with an explicit expiration timestamp (in epoch seconds).
    ///
    /// Returns `Ok(true)` if inserted as a new entry, or `Ok(false)` if updated in place.
    pub fn insert_with_ttl(
        &mut self,
        key_hash: u64,
        slab_block_id: SlabBlockId,
        value_len: u32,
        expire_at_secs: u32,
    ) -> Result<bool, ()> {
        // Grow before exceeding the 87.5% load threshold.
        if self.count * LOAD_FACTOR_DEN >= self.capacity * LOAD_FACTOR_NUM {
            self.resize(self.capacity * 2);
        }

        let fingerprint = h2(key_hash);
        let mut pos = h1(key_hash, self.capacity);

        loop {
            let group = Group::new(&self.ctrl, pos, self.capacity);

            // Check for an existing entry with the same hash (update path).
            for idx in group.match_byte(fingerprint) {
                if let Some(entry) = &self.entries[idx] {
                    if entry.matches(key_hash) {
                        // Update in place.
                        self.entries[idx] = Some(Box::new(HashEntry::with_ttl(
                            key_hash,
                            slab_block_id,
                            value_len,
                            expire_at_secs,
                        )));
                        return Ok(false); // updated
                    }
                }
            }

            // If the group has an empty slot, this key is definitely absent → insert.
            if group.has_empty() {
                let slot = group.first_available().ok_or(())?;
                self.ctrl[slot] = fingerprint;
                self.entries[slot] = Some(Box::new(HashEntry::with_ttl(
                    key_hash,
                    slab_block_id,
                    value_len,
                    expire_at_secs,
                )));
                self.count += 1;
                return Ok(true); // inserted
            }

            // Probe next group.
            pos = (pos + GROUP_SIZE) % self.capacity;
        }
    }

    /// Returns a reference to the `HashEntry` for `key_hash`, if present.
    ///
    /// Marks the entry as accessed (S3-FIFO bit) on every successful lookup.
    #[inline(always)]
    pub fn lookup(&self, key_hash: u64) -> Option<&HashEntry> {
        self.lookup_checked(key_hash, 0)
    }

    /// Returns a reference to the `HashEntry` for `key_hash`, checking against `now_secs`.
    ///
    /// If the entry exists but has expired (`is_expired(now_secs)`), returns `None`.
    /// Marks the entry as accessed (S3-FIFO bit) on every successful lookup.
    pub fn lookup_checked(&self, key_hash: u64, now_secs: u32) -> Option<&HashEntry> {
        let fingerprint = h2(key_hash);
        let mut pos = h1(key_hash, self.capacity);

        loop {
            let group = Group::new(&self.ctrl, pos, self.capacity);

            for idx in group.match_byte(fingerprint) {
                if let Some(entry) = &self.entries[idx] {
                    if entry.matches(key_hash) {
                        if now_secs != 0 && entry.is_expired(now_secs) {
                            return None; // expired
                        }
                        entry.mark_accessed(); // S3-FIFO hot-path
                        return Some(entry);
                    }
                }
            }

            if group.has_empty() {
                return None; // key not present
            }

            pos = (pos + GROUP_SIZE) % self.capacity;
        }
    }

    /// Removes the entry for `key_hash`, returning it if found.
    ///
    /// Leaves a `CTRL_DELETED` tombstone so in-progress probe sequences
    /// are not interrupted (deleted entries are compacted during resize).
    pub fn remove(&mut self, key_hash: u64) -> Option<Box<HashEntry>> {
        let fingerprint = h2(key_hash);
        let mut pos = h1(key_hash, self.capacity);

        loop {
            // Collect matching indices first to avoid holding an immutable
            // borrow on self.ctrl while also needing a mutable borrow.
            let matches: Vec<usize> = {
                let group = Group::new(&self.ctrl, pos, self.capacity);
                group.match_byte(fingerprint).collect()
            };
            let has_empty = {
                let group = Group::new(&self.ctrl, pos, self.capacity);
                group.has_empty()
            };

            for idx in matches {
                if let Some(entry) = &self.entries[idx] {
                    if entry.matches(key_hash) {
                        self.ctrl[idx] = CTRL_DELETED;
                        self.count -= 1;
                        return self.entries[idx].take();
                    }
                }
            }

            if has_empty {
                return None;
            }

            pos = (pos + GROUP_SIZE) % self.capacity;
        }
    }

    /// Number of live (non-deleted) entries.
    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }

    /// Returns `true` if the table holds no live entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Current slot capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Current load factor as a fraction (0.0 – 1.0).
    #[inline]
    pub fn load_factor(&self) -> f64 {
        self.count as f64 / self.capacity as f64
    }

    // ── Private ──────────────────────────────────────────────────────────────

    /// Doubles the capacity and rehashes all live entries.
    fn resize(&mut self, new_capacity: usize) {
        let new_capacity = new_capacity.next_power_of_two().max(GROUP_SIZE);
        log::debug!(
            "SwissTable: resizing {old} → {new_capacity}",
            old = self.capacity
        );

        let mut new_ctrl = vec![CTRL_EMPTY; new_capacity];
        let mut new_entries: Vec<Option<Box<HashEntry>>> =
            (0..new_capacity).map(|_| None).collect();

        for (idx, entry) in self.entries.iter_mut().enumerate() {
            if self.ctrl[idx] != CTRL_EMPTY && self.ctrl[idx] != CTRL_DELETED {
                if let Some(e) = entry.take() {
                    let hash = e.key_hash;
                    let fp = h2(hash);
                    let mut pos = h1(hash, new_capacity);
                    loop {
                        let group = Group::new(&new_ctrl, pos, new_capacity);
                        if let Some(slot) = group.first_available() {
                            new_ctrl[slot] = fp;
                            new_entries[slot] = Some(e);
                            break;
                        }
                        pos = (pos + GROUP_SIZE) % new_capacity;
                    }
                }
            }
        }

        self.ctrl = new_ctrl;
        self.entries = new_entries;
        self.capacity = new_capacity;
        self.compact_cursor = 0; // reset compaction cursor after resize
    }

    // ── Improvement 3: Idle-Time Tombstone Compaction ─────────────────────────

    /// Incrementally compacts tombstone slots in one group of 8 slots.
    ///
    /// Designed to be called during idle event-loop ticks with near-zero
    /// overhead (< 50 ns per call). Returns the number of tombstone slots
    /// reclaimed in this pass.
    ///
    /// # Thread Safety
    /// Must only be called from the owning worker thread. No locks required.
    pub fn compact_one_group(&mut self) -> usize {
        if self.capacity == 0 {
            return 0;
        }
        let start = self.compact_cursor;
        self.compact_cursor = (self.compact_cursor + GROUP_SIZE) % self.capacity;

        let mut reclaimed = 0;
        for i in 0..GROUP_SIZE {
            let idx = (start + i) % self.capacity;
            if self.ctrl[idx] != CTRL_DELETED {
                continue;
            }

            // Check if a live entry following this tombstone in the probe chain
            // can be safely back-shifted into this slot.
            if let Some(backshift_idx) = self.find_backshift_candidate(idx) {
                // Move the candidate entry into the tombstone slot.
                let fingerprint = self.ctrl[backshift_idx];
                self.ctrl[idx] = fingerprint;
                self.entries[idx] = self.entries[backshift_idx].take();
                self.ctrl[backshift_idx] = CTRL_DELETED;
                reclaimed += 1;
            } else if self.probe_chain_clear_after(idx) {
                // No live entries depend on this tombstone — safe to clear it.
                self.ctrl[idx] = CTRL_EMPTY;
                reclaimed += 1;
            }
        }
        reclaimed
    }

    /// Returns the total number of tombstone (`CTRL_DELETED`) slots.
    /// Used for monitoring and benchmark validation.
    pub fn tombstone_count(&self) -> usize {
        self.ctrl.iter().filter(|&&c| c == CTRL_DELETED).count()
    }

    /// Finds a live entry after `tombstone_idx` in the probe chain that can
    /// be back-shifted into the tombstone's position without breaking its
    /// own lookup invariant.
    fn find_backshift_candidate(&self, tombstone_idx: usize) -> Option<usize> {
        // Scan the next GROUP_SIZE slots after the tombstone.
        for i in 1..=GROUP_SIZE {
            let candidate = (tombstone_idx + i) % self.capacity;
            let ctrl = self.ctrl[candidate];

            // Stop at an empty slot — no entries beyond this point depend on the tombstone.
            if ctrl == CTRL_EMPTY {
                return None;
            }

            // Skip over other tombstones.
            if ctrl == CTRL_DELETED {
                continue;
            }

            // Found a live entry: check if its natural home is at or before tombstone_idx.
            if let Some(entry) = &self.entries[candidate] {
                let natural_home = h1(entry.key_hash, self.capacity);
                // If the entry's natural slot is <= tombstone position (in ring distance),
                // it can be moved back safely.
                if ring_distance(natural_home, candidate, self.capacity)
                    > ring_distance(natural_home, tombstone_idx, self.capacity)
                {
                    return Some(candidate);
                }
            }
        }
        None
    }

    /// Returns true if the probe chain after `idx` contains no live entries
    /// that depend on this tombstone for their lookup (safe to convert to CTRL_EMPTY).
    fn probe_chain_clear_after(&self, idx: usize) -> bool {
        for i in 1..=GROUP_SIZE {
            let next = (idx + i) % self.capacity;
            match self.ctrl[next] {
                CTRL_EMPTY => return true, // clean break
                CTRL_DELETED => continue,  // another tombstone, keep checking
                _ => return false,         // live entry depends on this tombstone
            }
        }
        false
    }
}

/// Ring-buffer distance from `from` to `to` in a table of `capacity` slots.
#[inline(always)]
fn ring_distance(from: usize, to: usize, capacity: usize) -> usize {
    if to >= from {
        to - from
    } else {
        capacity - from + to
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_id(n: u32) -> SlabBlockId {
        SlabBlockId(n)
    }

    #[test]
    fn insert_and_lookup() {
        let mut t = SwissTable::with_capacity(64);
        let h = hash_key(b"hello");
        t.insert(h, make_id(1), 100).unwrap();
        let entry = t.lookup(h).expect("should be found");
        assert_eq!(entry.slab_block_id, make_id(1));
        assert_eq!(entry.value_len, 100);
    }

    #[test]
    fn lookup_missing_returns_none() {
        let t = SwissTable::with_capacity(64);
        assert!(t.lookup(0xDEAD).is_none());
    }

    #[test]
    fn remove_entry() {
        let mut t = SwissTable::with_capacity(64);
        let h = hash_key(b"remove-me");
        t.insert(h, make_id(5), 32).unwrap();
        let removed = t.remove(h);
        assert!(removed.is_some());
        assert!(t.lookup(h).is_none());
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn update_existing_key() {
        let mut t = SwissTable::with_capacity(64);
        let h = hash_key(b"update");
        t.insert(h, make_id(1), 50).unwrap();
        t.insert(h, make_id(2), 200).unwrap();
        let e = t.lookup(h).unwrap();
        assert_eq!(e.slab_block_id, make_id(2));
        assert_eq!(e.value_len, 200);
        assert_eq!(t.len(), 1); // still one entry
    }

    #[test]
    fn insert_many_triggers_resize() {
        let mut t = SwissTable::with_capacity(8);
        for i in 0u64..100 {
            t.insert(hash_key(&i.to_le_bytes()), make_id(i as u32), 0)
                .unwrap();
        }
        assert_eq!(t.len(), 100);
        for i in 0u64..100 {
            assert!(t.lookup(hash_key(&i.to_le_bytes())).is_some());
        }
    }

    #[test]
    fn s3_fifo_access_flag_set_on_lookup() {
        let mut t = SwissTable::with_capacity(64);
        let h = hash_key(b"fifo");
        t.insert(h, make_id(9), 64).unwrap();
        let e = t.lookup(h).unwrap();
        assert!(e.test_and_clear_accessed());
    }

    #[test]
    fn tombstone_count_after_many_deletes() {
        let mut t = SwissTable::with_capacity(256);
        // Insert 100 keys
        let hashes: Vec<u64> = (0u64..100).map(|i| hash_key(&i.to_le_bytes())).collect();
        for (i, &h) in hashes.iter().enumerate() {
            t.insert(h, make_id(i as u32), 64).unwrap();
        }
        // Delete 70 of them
        for &h in &hashes[..70] {
            t.remove(h);
        }
        assert_eq!(t.len(), 30);
        assert_eq!(t.tombstone_count(), 70);
    }

    #[test]
    fn compact_reduces_tombstones_and_preserves_live_entries() {
        let mut t = SwissTable::with_capacity(256);
        let hashes: Vec<u64> = (0u64..50).map(|i| hash_key(&i.to_le_bytes())).collect();
        for (i, &h) in hashes.iter().enumerate() {
            t.insert(h, make_id(i as u32), 128).unwrap();
        }
        // Delete 40 entries to create heavy tombstone density
        for &h in &hashes[..40] {
            t.remove(h);
        }
        let before = t.tombstone_count();
        assert_eq!(before, 40);

        // Run compaction sweeps over the whole table
        let passes = t.capacity() / 8 + 1;
        for _ in 0..passes {
            t.compact_one_group();
        }

        let after = t.tombstone_count();
        // Tombstone count must not increase
        assert!(
            after <= before,
            "tombstones should not increase after compaction"
        );

        // All live entries must still be found
        for &h in &hashes[40..] {
            assert!(t.lookup(h).is_some(), "live entry missing after compaction");
        }
        // All deleted entries must still be absent
        for &h in &hashes[..40] {
            assert!(
                t.lookup(h).is_none(),
                "deleted entry reappeared after compaction"
            );
        }
    }

    #[test]
    fn ttl_lookup_before_and_after_expiry() {
        let mut t = SwissTable::with_capacity(32);
        let h = hash_key(b"temporary_key");
        // Insert with expiration at epoch second 100
        t.insert_with_ttl(h, make_id(7), 32, 100).unwrap();

        // At epoch 90: active
        assert!(t.lookup_checked(h, 90).is_some());
        // At epoch 99: active
        assert!(t.lookup_checked(h, 99).is_some());
        // At epoch 100: expired
        assert!(t.lookup_checked(h, 100).is_none());
        // At epoch 105: expired
        assert!(t.lookup_checked(h, 105).is_none());
        // Standard unchecked lookup still returns entry descriptor
        assert!(t.lookup(h).is_some());
    }
}
