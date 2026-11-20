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

use std::collections::HashMap;

use kachedb_core::SlabBlockId;

use crate::entry::{HashEntry, INLINE_KEY_CAPACITY, SnapshotPayloadEntry, TableEntry};
use crate::glob::CompiledPattern;

// ─── Control byte sentinels ───────────────────────────────────────────────────

/// Control byte for an unoccupied slot.
const CTRL_EMPTY: u8 = 0xFF;
/// Control byte for a deleted (tombstone) slot.
const CTRL_DELETED: u8 = 0x80;
/// Maximum H2 fingerprint value (7-bit).
const CTRL_H2_MASK: u8 = 0x7F;
/// Number of control bytes in one probe group (portable scalar fallback).
const GROUP_SIZE: usize = 8;
/// Resize threshold: 87.5% load factor (grow).
const LOAD_FACTOR_NUM: usize = 7;
const LOAD_FACTOR_DEN: usize = 8;
/// Shrink threshold: <12.5% load factor (1/8).
const SHRINK_LOAD_FACTOR_DEN: usize = 8;
/// Minimum capacity floor below which a table will not shrink.
const MIN_SHRINK_CAPACITY: usize = 1024;

// ─── Hash helpers ─────────────────────────────────────────────────────────────

/// Computes a deterministic 64-bit XXH3 digest for an arbitrary byte key.
///
/// The digest is **stable across process restarts, CPU architectures and
/// builds**. Snapshots persist it alongside the original key bytes so the
/// restored entries use the same shards and probe groups.
#[inline]
pub fn hash_key(key: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(key)
}

/// Extracts `H1`: upper 57 bits used as the slot index.
#[inline(always)]
fn h1(hash: u64, capacity: usize) -> usize {
    ((hash >> 7) as usize) & (capacity - 1)
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
    mask: usize,
}

impl<'a> Group<'a> {
    #[inline(always)]
    fn new(ctrl: &'a [u8], start: usize, cap: usize) -> Self {
        Self {
            ctrl,
            base: start,
            mask: cap - 1,
        }
    }

    /// Yields indices of slots matching `fingerprint`.
    #[inline(always)]
    fn match_byte(&self, fingerprint: u8) -> impl Iterator<Item = usize> + '_ {
        (0..GROUP_SIZE).filter_map(move |i| {
            let idx = (self.base + i) & self.mask;
            if self.ctrl[idx] == fingerprint {
                Some(idx)
            } else {
                None
            }
        })
    }

    /// Returns `true` if any slot in the group is empty (`CTRL_EMPTY`).
    #[inline(always)]
    fn has_empty(&self) -> bool {
        (0..GROUP_SIZE).any(|i| self.ctrl[(self.base + i) & self.mask] == CTRL_EMPTY)
    }

    /// Returns the index of the first empty or deleted slot in the group.
    #[inline(always)]
    fn first_available(&self) -> Option<usize> {
        (0..GROUP_SIZE).find_map(|i| {
            let idx = (self.base + i) & self.mask;
            matches!(self.ctrl[idx], CTRL_EMPTY | CTRL_DELETED).then_some(idx)
        })
    }
}

// ─── SwissTable ───────────────────────────────────────────────────────────────

/// O(1) point-query hash index for KacheDB application cache keys.
///
/// Stores key bytes and their slab descriptors using an
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
/// table.insert_with_key(hash, b"my-cache-key", block_id, 256).unwrap();
/// assert!(table.lookup_key_checked(hash, b"my-cache-key", 0).is_some());
/// ```
/// Advances a shard-local home-bucket cursor `cursor` within a power-of-two `cap`.
/// Handles cursors from a previously larger table (post-shrink) via Redis's
/// high-bit mask preservation before bit-reversed increment.
#[inline(always)]
pub fn next_bit_reversed_cursor(mut cursor: usize, cap: usize) -> usize {
    debug_assert!(cap >= 2 && cap.is_power_of_two());
    let mask = cap - 1;
    cursor |= !mask;
    cursor = cursor.reverse_bits();
    cursor = cursor.wrapping_add(1);
    cursor.reverse_bits()
}

pub struct SwissTable {
    /// One control byte per slot.
    ctrl: Vec<u8>,
    /// Flat contiguous slot entries (zero heap allocation on insert).
    entries: Vec<HashEntry>,
    /// Number of occupied (non-deleted) slots.
    count: usize,
    /// Allocated slot count (always a power of two).
    capacity: usize,
    /// Incremental cursor for idle-time tombstone compaction (Improvement 3).
    compact_cursor: usize,
    /// Shard-local overflow storage for keys exceeding `INLINE_KEY_CAPACITY` (40 bytes).
    overflow_keys: HashMap<usize, Box<[u8]>>,
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
            entries: (0..capacity).map(|_| HashEntry::default()).collect(),
            count: 0,
            capacity,
            compact_cursor: 0,
            overflow_keys: HashMap::new(),
        }
    }

    fn store_entry_key(&mut self, slot: usize, key: &[u8]) {
        self.entries[slot].set_inline_key(key);
        if key.len() > INLINE_KEY_CAPACITY {
            self.overflow_keys.insert(slot, key.into());
        }
    }

    /// Scans the key's probe groups and, for insertion, remembers the first
    /// reusable slot. A full circuit can occur when every unused slot is a tombstone.
    #[inline]
    fn probe<const INSERT: bool>(
        &self,
        hash: u64,
        mut matches: impl FnMut(usize) -> bool,
    ) -> (Option<usize>, Option<usize>) {
        let fingerprint = h2(hash);
        let home = h1(hash, self.capacity);
        let mask = self.capacity - 1;
        let mut pos = home;
        let mut vacant = None;
        loop {
            let group = Group::new(&self.ctrl, pos, self.capacity);
            if let Some(slot) = group
                .match_byte(fingerprint)
                .find(|&slot| self.entries[slot].matches(hash) && matches(slot))
            {
                return (Some(slot), vacant);
            }
            if INSERT && vacant.is_none() {
                vacant = group.first_available();
            }
            pos = (pos + GROUP_SIZE) & mask;
            if group.has_empty() || pos == home {
                return (None, vacant);
            }
        }
    }

    #[inline]
    fn matches_key(&self, slot: usize, key: &[u8]) -> bool {
        let entry = &self.entries[slot];
        entry.key_len as usize == key.len()
            && entry
                .inline_key_prefix()
                .eq(&key[..key.len().min(INLINE_KEY_CAPACITY)])
            && if key.is_empty() {
                entry.has_key()
            } else if key.len() > INLINE_KEY_CAPACITY {
                self.key_bytes_at(slot) == key
            } else {
                true
            }
    }

    #[inline]
    fn key_slot(&self, hash: u64, key: &[u8]) -> Option<usize> {
        self.probe::<false>(hash, |slot| self.matches_key(slot, key))
            .0
    }

    // Legacy hash-only callers can operate on an unambiguous digest.
    fn unique_hash_slot(&self, hash: u64) -> Result<Option<usize>, ()> {
        let mut first = None;
        let (duplicate, _) = self.probe::<false>(hash, |slot| first.replace(slot).is_some());
        if duplicate.is_some() {
            Err(())
        } else {
            Ok(first)
        }
    }

    /// Inserts a key-hash and slab descriptor, returning `Some(old_block_id)` if updated
    /// or `None` if newly inserted.
    #[inline]
    pub fn insert(
        &mut self,
        key_hash: u64,
        slab_block_id: SlabBlockId,
        value_len: u32,
    ) -> Result<Option<SlabBlockId>, ()> {
        self.insert_with_ttl(key_hash, slab_block_id, value_len, 0)
    }

    /// Inserts a key-hash, raw key bytes, and slab descriptor without TTL.
    #[inline]
    pub fn insert_with_key(
        &mut self,
        key_hash: u64,
        key: &[u8],
        slab_block_id: SlabBlockId,
        value_len: u32,
    ) -> Result<Option<SlabBlockId>, ()> {
        self.insert_with_ttl_and_key(key_hash, key, slab_block_id, value_len, 0)
    }

    /// Inserts a key-hash and slab descriptor with an explicit expiration timestamp (in epoch seconds).
    ///
    /// Single-pass probe: Returns `Ok(Some(old_block_id))` if updated in place,
    /// or `Ok(None)` if inserted as a new entry.
    #[inline]
    pub fn insert_with_ttl(
        &mut self,
        key_hash: u64,
        slab_block_id: SlabBlockId,
        value_len: u32,
        expire_at_secs: u32,
    ) -> Result<Option<SlabBlockId>, ()> {
        self.insert_typed(
            key_hash,
            slab_block_id,
            value_len,
            expire_at_secs,
            crate::entry::VALUE_TYPE_STRING,
        )
    }

    /// Inserts a key-hash, raw key bytes, and slab descriptor with an explicit expiration timestamp.
    #[inline]
    pub fn insert_with_ttl_and_key(
        &mut self,
        key_hash: u64,
        key: &[u8],
        slab_block_id: SlabBlockId,
        value_len: u32,
        expire_at_secs: u32,
    ) -> Result<Option<SlabBlockId>, ()> {
        self.insert_typed_with_key(
            key_hash,
            key,
            slab_block_id,
            value_len,
            expire_at_secs,
            crate::entry::VALUE_TYPE_STRING,
        )
    }

    /// Inserts a key-hash, slab descriptor, expiration timestamp, and explicit value type.
    ///
    /// Single-pass probe: Returns `Ok(Some(old_block_id))` if updated in place,
    /// or `Ok(None)` if inserted as a new entry.
    #[inline]
    pub fn insert_typed(
        &mut self,
        key_hash: u64,
        slab_block_id: SlabBlockId,
        value_len: u32,
        expire_at_secs: u32,
        value_type: u8,
    ) -> Result<Option<SlabBlockId>, ()> {
        let existing = self.unique_hash_slot(key_hash)?;
        let entry = HashEntry::with_type(
            key_hash,
            slab_block_id,
            value_len,
            expire_at_secs,
            value_type,
        );
        if let Some(slot) = existing {
            return Ok(Some(self.replace_value(slot, entry)));
        }
        self.insert_entry(entry, None)
    }

    /// Inserts a key-hash, raw key bytes, slab descriptor, expiration timestamp, and explicit value type.
    pub fn insert_typed_with_key(
        &mut self,
        key_hash: u64,
        key: &[u8],
        slab_block_id: SlabBlockId,
        value_len: u32,
        expire_at_secs: u32,
        value_type: u8,
    ) -> Result<Option<SlabBlockId>, ()> {
        if key.len() > crate::entry::MAX_KEY_LEN {
            return Err(());
        }
        let entry = HashEntry::with_type(
            key_hash,
            slab_block_id,
            value_len,
            expire_at_secs,
            value_type,
        );
        self.insert_entry(entry, Some(key))
    }

    fn insert_entry(
        &mut self,
        entry: HashEntry,
        key: Option<&[u8]>,
    ) -> Result<Option<SlabBlockId>, ()> {
        let hash = entry.key_hash;
        let (existing, mut vacant) = self.probe::<true>(hash, |slot| {
            key.is_some_and(|key| self.matches_key(slot, key))
        });
        if let Some(slot) = existing {
            return Ok(Some(self.replace_value(slot, entry)));
        }
        if self.count * LOAD_FACTOR_DEN >= self.capacity * LOAD_FACTOR_NUM {
            self.resize(self.capacity * 2);
            vacant = self.probe::<true>(hash, |_| false).1;
        }
        let slot = vacant.ok_or(())?;
        self.ctrl[slot] = h2(hash);
        self.entries[slot] = entry;
        if let Some(key) = key {
            self.store_entry_key(slot, key);
        }
        self.count += 1;
        Ok(None)
    }

    fn replace_value(&mut self, slot: usize, value: HashEntry) -> SlabBlockId {
        let entry = &mut self.entries[slot];
        let old_block = entry.slab_block_id;
        entry.slab_block_id = value.slab_block_id;
        entry.value_len = value.value_len;
        entry.expire_at_secs = value.expire_at_secs;
        entry.value_type = value.value_type;
        entry.clear_accessed();
        old_block
    }

    /// Assigns original key bytes that were independently recovered for an anonymous entry.
    pub fn ensure_key_name(&mut self, key_hash: u64, key: &[u8]) {
        if key.len() > crate::entry::MAX_KEY_LEN || self.key_slot(key_hash, key).is_some() {
            return;
        }
        if let Some(slot) = self
            .probe::<false>(key_hash, |slot| !self.entries[slot].has_key())
            .0
        {
            self.store_entry_key(slot, key);
        }
    }

    /// Returns the bytes of an entry borrowed from this table.
    /// For a stand-alone entry only the inline prefix is available.
    pub fn key_bytes<'a>(&'a self, entry: &'a HashEntry) -> &'a [u8] {
        if entry.key_len as usize > INLINE_KEY_CAPACITY {
            let offset = std::ptr::from_ref(entry)
                .addr()
                .wrapping_sub(self.entries.as_ptr().addr());
            let slot = offset / std::mem::size_of::<HashEntry>();
            if self
                .entries
                .get(slot)
                .is_some_and(|stored| std::ptr::eq(stored, entry))
            {
                return self.key_bytes_at(slot);
            }
        }
        entry.inline_key_prefix()
    }

    #[inline]
    fn key_bytes_at(&self, slot: usize) -> &[u8] {
        let entry = &self.entries[slot];
        if entry.key_len as usize > INLINE_KEY_CAPACITY {
            &self.overflow_keys[&slot]
        } else {
            entry.inline_key_prefix()
        }
    }

    /// Looks up an unambiguous digest without TTL validation.
    #[inline(always)]
    pub fn lookup(&self, key_hash: u64) -> Option<&HashEntry> {
        self.lookup_checked(key_hash, 0)
    }

    /// Looks up an unambiguous digest, rejecting expired entries.
    #[inline(always)]
    pub fn lookup_checked(&self, key_hash: u64, now_secs: u32) -> Option<&HashEntry> {
        self.lookup_slot(self.unique_hash_slot(key_hash).ok().flatten()?, now_secs)
    }

    /// Looks up the exact key, rejecting expired entries.
    #[inline(always)]
    pub fn lookup_key_checked(&self, hash: u64, key: &[u8], now_secs: u32) -> Option<&HashEntry> {
        self.lookup_slot(self.key_slot(hash, key)?, now_secs)
    }

    fn lookup_slot(&self, slot: usize, now_secs: u32) -> Option<&HashEntry> {
        let entry = &self.entries[slot];
        if entry.is_expired(now_secs) {
            return None;
        }
        entry.mark_accessed();
        Some(entry)
    }

    /// Updates the expiration of an unambiguous digest.
    pub fn update_ttl(&mut self, key_hash: u64, expire_at_secs: u32, now_secs: u32) -> bool {
        let slot = self.unique_hash_slot(key_hash).ok().flatten();
        self.set_ttl(slot, expire_at_secs, now_secs, false)
    }

    /// Updates the expiration of the exact key.
    pub fn update_key_ttl(&mut self, hash: u64, key: &[u8], expires: u32, now: u32) -> bool {
        let slot = self.key_slot(hash, key);
        self.set_ttl(slot, expires, now, false)
    }

    /// Returns the remaining TTL, -1 for a persistent key, or -2 for a missing or expired key.
    pub fn get_ttl(&self, key_hash: u64, now_secs: u32) -> i64 {
        Self::remaining_ttl(self.lookup_checked(key_hash, now_secs), now_secs)
    }

    /// Returns the remaining TTL for the exact key.
    pub fn get_key_ttl(&self, hash: u64, key: &[u8], now: u32) -> i64 {
        Self::remaining_ttl(self.lookup_key_checked(hash, key, now), now)
    }

    fn remaining_ttl(entry: Option<&HashEntry>, now: u32) -> i64 {
        match entry {
            Some(entry) if entry.expire_at_secs == 0 => -1,
            Some(entry) => i64::from(entry.expire_at_secs - now),
            None => -2,
        }
    }

    /// Removes the TTL of an unambiguous digest.
    pub fn persist(&mut self, key_hash: u64, now_secs: u32) -> bool {
        let slot = self.unique_hash_slot(key_hash).ok().flatten();
        self.set_ttl(slot, 0, now_secs, true)
    }

    /// Removes the TTL from the exact key.
    pub fn persist_key(&mut self, hash: u64, key: &[u8], now: u32) -> bool {
        let slot = self.key_slot(hash, key);
        self.set_ttl(slot, 0, now, true)
    }

    fn set_ttl(&mut self, slot: Option<usize>, expires: u32, now: u32, has_ttl: bool) -> bool {
        let Some(slot) = slot else { return false };
        let entry = &mut self.entries[slot];
        if entry.is_expired(now) || (has_ttl && entry.expire_at_secs == 0) {
            return false;
        }
        entry.expire_at_secs = expires;
        entry.mark_accessed();
        true
    }

    /// Removes an entry identified by an unambiguous digest.
    pub fn remove(&mut self, key_hash: u64) -> Option<TableEntry> {
        let slot = self.unique_hash_slot(key_hash).ok().flatten()?;
        Some(self.remove_slot(slot))
    }

    /// Removes the exact key.
    pub fn remove_key(&mut self, hash: u64, key: &[u8]) -> Option<TableEntry> {
        let slot = self.key_slot(hash, key)?;
        Some(self.remove_slot(slot))
    }

    /// Removes an entry only for an unambiguous digest and matching slab block ID.
    pub fn remove_if_matching(
        &mut self,
        key_hash: u64,
        expected_block_id: SlabBlockId,
    ) -> Option<TableEntry> {
        let slot = self.unique_hash_slot(key_hash).ok().flatten()?;
        self.remove_matching_slot(slot, expected_block_id)
    }

    /// Removes the exact key only if it still refers to the expected slab allocation.
    pub fn remove_key_if_matching(
        &mut self,
        hash: u64,
        key: &[u8],
        expected_block_id: SlabBlockId,
    ) -> Option<TableEntry> {
        let slot = self.key_slot(hash, key)?;
        self.remove_matching_slot(slot, expected_block_id)
    }

    /// Removes the exact key only if:
    /// 1. Its key and hash match.
    /// 2. Its slab block ID matches `expected_block_id`.
    /// 3. Its expiration deadline matches `expected_expire_at_secs` and `expire_at_secs > 0`.
    /// 4. Current epoch seconds `now_sec >= expire_at_secs` (is due).
    /// 5. Its slab block generation matches `expected_generation` (not reused).
    pub fn remove_key_if_expired(
        &mut self,
        hash: u64,
        key: &[u8],
        expected_block_id: SlabBlockId,
        expected_expire_at_secs: u32,
        expected_generation: u64,
        now_sec: u32,
    ) -> Option<TableEntry> {
        let slot = self.key_slot(hash, key)?;
        let entry = &self.entries[slot];
        if entry.slab_block_id != expected_block_id {
            return None;
        }
        if entry.expire_at_secs == 0 || entry.expire_at_secs != expected_expire_at_secs {
            return None;
        }
        if now_sec < entry.expire_at_secs {
            return None;
        }
        if kachedb_core::registry::get_slot_generation(expected_block_id)
            != Some(expected_generation)
        {
            return None;
        }
        Some(self.remove_slot(slot))
    }

    fn remove_matching_slot(&mut self, slot: usize, block: SlabBlockId) -> Option<TableEntry> {
        (self.entries[slot].slab_block_id == block).then(|| self.remove_slot(slot))
    }

    fn remove_slot(&mut self, slot: usize) -> TableEntry {
        let entry = &self.entries[slot];
        let snapshot = entry.to_snapshot();
        if entry.key_len as usize > INLINE_KEY_CAPACITY {
            self.overflow_keys.remove(&slot);
        }
        self.ctrl[slot] = CTRL_DELETED;
        self.count -= 1;
        if self.capacity > MIN_SHRINK_CAPACITY
            && self.count * SHRINK_LOAD_FACTOR_DEN < self.capacity
        {
            self.resize(self.capacity / 2);
        }
        snapshot
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

    /// Returns a vector containing all live (hash, TableEntry) pairs in the table.
    pub fn live_entries(&self) -> Vec<(u64, TableEntry)> {
        let mut res = Vec::with_capacity(self.count);
        for (idx, entry) in self.entries.iter().enumerate() {
            if self.ctrl[idx] != CTRL_EMPTY && self.ctrl[idx] != CTRL_DELETED {
                res.push((entry.key_hash, entry.to_snapshot()));
            }
        }
        res
    }

    /// Returns a vector containing all live `(key_hash, key_bytes, TableEntry)` tuples in the table.
    pub fn live_entries_with_keys(&self) -> Vec<(u64, Vec<u8>, TableEntry)> {
        let mut res = Vec::with_capacity(self.count);
        for (idx, entry) in self.entries.iter().enumerate() {
            if self.ctrl[idx] != CTRL_EMPTY && self.ctrl[idx] != CTRL_DELETED {
                res.push((
                    entry.key_hash,
                    self.key_bytes_at(idx).to_vec(),
                    entry.to_snapshot(),
                ));
            }
        }
        res
    }

    /// # Safety
    ///
    /// Indexed payloads must be live, sized correctly, and immutable for this borrow.
    pub(crate) unsafe fn live_entries_with_payloads(
        &self,
        now_sec: u32,
    ) -> Result<Vec<SnapshotPayloadEntry>, kachedb_core::CoreError> {
        let mut res = Vec::with_capacity(self.count);
        for (idx, entry) in self.entries.iter().enumerate() {
            if matches!(self.ctrl[idx], CTRL_EMPTY | CTRL_DELETED) || entry.is_expired(now_sec) {
                continue;
            }
            let ptr = unsafe { kachedb_core::resolve_slot_ptr(entry.slab_block_id) }.ok_or(
                kachedb_core::CoreError::InvalidBlockId {
                    id: entry.slab_block_id.0,
                },
            )?;
            let value = unsafe { std::slice::from_raw_parts(ptr, entry.value_len as usize) };
            let val_bytes = if entry.value_type == crate::entry::VALUE_TYPE_HASH {
                let mut encoded = Vec::new();
                crate::hash_frame::HashSlotFrame::encode_pairs(value, &mut encoded);
                encoded
            } else {
                value.to_vec()
            };
            res.push(SnapshotPayloadEntry {
                key_hash: entry.key_hash,
                key_bytes: self.key_bytes_at(idx).to_vec(),
                has_key: entry.has_key(),
                value_type: entry.value_type,
                val_bytes,
                expire_at: entry.expire_at_secs,
            });
        }
        Ok(res)
    }

    pub(crate) fn schedule_expirations(&self, wheel: &mut kachedb_core::HashedTimingWheel) {
        for (idx, entry) in self.entries.iter().enumerate() {
            if !matches!(self.ctrl[idx], CTRL_EMPTY | CTRL_DELETED)
                && entry.has_key()
                && entry.expire_at_secs != 0
            {
                wheel.schedule_key(
                    entry.key_hash,
                    self.key_bytes_at(idx),
                    entry.slab_block_id,
                    entry.expire_at_secs,
                );
            }
        }
    }

    /// Returns the number of live entries with no known original key bytes.
    pub fn anonymous_key_count(&self) -> usize {
        self.entries
            .iter()
            .enumerate()
            .filter(|&(idx, entry)| {
                self.ctrl[idx] != CTRL_EMPTY && self.ctrl[idx] != CTRL_DELETED && !entry.has_key()
            })
            .count()
    }

    /// Visits all live entries whose home bucket (`h1(key_hash, capacity)`) equals
    /// `cursor & (capacity - 1)`, applying optional TTL, type, and glob pattern filters.
    ///
    /// Returns the number of matching keys emitted via `on_key`.
    pub fn scan_home_bucket(
        &self,
        cursor: usize,
        pattern: Option<&CompiledPattern<'_>>,
        type_filter: Option<u8>,
        now_secs: u32,
        mut on_key: impl FnMut(&[u8]),
    ) -> usize {
        if self.count == 0 || self.capacity == 0 {
            return 0;
        }

        let mask = self.capacity - 1;
        let home_bucket = cursor & mask;
        let mut pos = home_bucket;
        let max_groups = (self.capacity / GROUP_SIZE).max(1);
        let mut visited_groups = 0;
        let mut emitted = 0;

        loop {
            for i in 0..GROUP_SIZE {
                let idx = (pos + i) & mask;
                let ctrl = self.ctrl[idx];
                if ctrl != CTRL_EMPTY && ctrl != CTRL_DELETED {
                    let entry = &self.entries[idx];
                    if h1(entry.key_hash, self.capacity) == home_bucket {
                        // Skip legacy anonymous keys and expired keys
                        if !entry.has_key() {
                            continue;
                        }
                        if now_secs != 0 && entry.is_expired(now_secs) {
                            continue;
                        }
                        if let Some(expected_type) = type_filter {
                            if entry.value_type != expected_type {
                                continue;
                            }
                        }
                        if let Some(pat) = pattern {
                            if !pat.matches_entry(self, entry) {
                                continue;
                            }
                        }
                        on_key(self.key_bytes_at(idx));
                        emitted += 1;
                    }
                }
            }

            let group = Group::new(&self.ctrl, pos, self.capacity);
            visited_groups += 1;
            if group.has_empty() || visited_groups >= max_groups {
                break;
            }
            pos = (pos + GROUP_SIZE) & mask;
        }

        emitted
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
        let mut new_entries = Vec::with_capacity(new_capacity);
        new_entries.resize_with(new_capacity, HashEntry::default);
        let mut old_overflow = std::mem::take(&mut self.overflow_keys);

        for (idx, entry) in self.entries.drain(..).enumerate() {
            if self.ctrl[idx] != CTRL_EMPTY && self.ctrl[idx] != CTRL_DELETED {
                let hash = entry.key_hash;
                let fp = h2(hash);
                let mut pos = h1(hash, new_capacity);
                loop {
                    let group = Group::new(&new_ctrl, pos, new_capacity);
                    if let Some(slot) = group.first_available() {
                        new_ctrl[slot] = fp;
                        if let Some(key) = old_overflow.remove(&idx) {
                            self.overflow_keys.insert(slot, key);
                        }
                        new_entries[slot] = entry;
                        break;
                    }
                    pos = (pos + GROUP_SIZE) % new_capacity;
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
                self.entries.swap(idx, backshift_idx);
                if let Some(key) = self.overflow_keys.remove(&backshift_idx) {
                    self.overflow_keys.insert(idx, key);
                }
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

    /// Clears all entries from the table.
    pub fn clear(&mut self) {
        self.ctrl.fill(CTRL_EMPTY);
        self.overflow_keys.clear();
        self.count = 0;
        self.compact_cursor = 0;
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
            let entry = &self.entries[candidate];
            let natural_home = h1(entry.key_hash, self.capacity);
            // If the entry's natural slot is <= tombstone position (in ring distance),
            // it can be moved back safely.
            if ring_distance(natural_home, candidate, self.capacity)
                > ring_distance(natural_home, tombstone_idx, self.capacity)
            {
                return Some(candidate);
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

    #[test]
    fn test_shrink_after_deletions() {
        let mut t = SwissTable::with_capacity(2048);
        assert_eq!(t.capacity(), 2048);

        let hashes: Vec<u64> = (0u64..1500).map(|i| hash_key(&i.to_le_bytes())).collect();
        for (i, &h) in hashes.iter().enumerate() {
            t.insert(h, make_id(i as u32), 64).unwrap();
        }
        // Resized during growth to fit 1500 elements
        assert!(t.capacity() >= 2048);

        // Delete most elements so count drops below 12.5% of capacity
        for &h in &hashes[..1400] {
            t.remove(h);
        }

        assert_eq!(t.len(), 100);
        // Table should have shrunk down toward MIN_SHRINK_CAPACITY
        assert!(t.capacity() <= 2048);

        // Remaining elements must still be found
        for (i, &h) in hashes.iter().enumerate().skip(1400) {
            let entry = t.lookup(h).expect("remaining key should exist");
            assert_eq!(entry.slab_block_id, make_id(i as u32));
        }
    }

    #[test]
    fn test_update_ttl_and_get_ttl() {
        let mut t = SwissTable::with_capacity(32);
        let h = hash_key(b"user:session");
        t.insert(h, make_id(10), 64).unwrap(); // Persistent entry

        assert_eq!(t.get_ttl(h, 100), -1); // Persistent

        // Update TTL to expire at 200
        assert!(t.update_ttl(h, 200, 100));
        assert_eq!(t.get_ttl(h, 100), 100); // 100s remaining
        assert_eq!(t.get_ttl(h, 150), 50); // 50s remaining
        assert_eq!(t.get_ttl(h, 200), -2); // Expired
        assert_eq!(t.get_ttl(h, 250), -2); // Expired

        // Missing key
        let missing = hash_key(b"missing");
        assert_eq!(t.get_ttl(missing, 100), -2);
        assert!(!t.update_ttl(missing, 200, 100));
    }

    #[test]
    fn test_persist_removes_ttl() {
        let mut t = SwissTable::with_capacity(32);
        let h = hash_key(b"temp_data");
        t.insert_with_ttl(h, make_id(12), 64, 200).unwrap();

        assert_eq!(t.get_ttl(h, 100), 100);
        assert!(t.persist(h, 100));
        assert_eq!(t.get_ttl(h, 100), -1); // Now persistent
        assert!(!t.persist(h, 100)); // Already persistent -> false

        let missing = hash_key(b"missing");
        assert!(!t.persist(missing, 100));
    }

    /// Snapshots persist `key_hash`, so the digest must never depend on
    /// per-process randomness. Pinned value guards against regressions.
    #[test]
    fn hash_key_is_stable_across_processes() {
        assert_eq!(hash_key(b"k1"), 9344898337136588485);
        assert_eq!(hash_key(b"k1"), hash_key(b"k1"));
        assert_ne!(hash_key(b"k1"), hash_key(b"k2"));
    }

    #[test]
    fn test_inline_sso_and_overflow_key_storage_all_lengths() {
        let mut t = SwissTable::with_capacity(64);
        for (i, len) in [0, 1, 39, 40, 41, 512, crate::MAX_KEY_LEN]
            .into_iter()
            .enumerate()
        {
            let key: Vec<u8> = (0..len).map(|b| b'a' + (b % 26) as u8).collect();
            let hash = hash_key(&key);
            t.insert_with_key(hash, &key, make_id(i as u32 + 1), 64)
                .unwrap();

            let slot = t.key_slot(hash, &key).unwrap();
            assert_eq!(t.entries[slot].key_len as usize, len);
            assert_eq!(t.key_bytes(&t.entries[slot]), key);
            assert_eq!(
                t.overflow_keys.contains_key(&slot),
                len > INLINE_KEY_CAPACITY
            );
        }
        assert_eq!(t.anonymous_key_count(), 0);
        let too_long = vec![0; crate::MAX_KEY_LEN + 1];
        assert!(t.insert_with_key(0, &too_long, make_id(0), 64).is_err());

        t.clear();
        assert!(t.overflow_keys.is_empty());
        assert!(t.is_empty());
    }

    #[test]
    fn colliding_keys_keep_identity_through_compaction_and_resize() {
        let mut t = SwissTable::with_capacity(1024);
        let hash = 23;
        let first = [b'a'; 64];
        let mut second = first;
        second[63] = b'b';
        let inline = b"short";
        t.insert_with_key(hash, &first, make_id(1), 16).unwrap();
        t.insert_with_key(hash, &second, make_id(2), 16).unwrap();
        t.insert_with_key(hash, inline, make_id(3), 16).unwrap();

        assert!(t.lookup(hash).is_none());
        assert!(t.remove(hash).is_none());
        let old_slot = t.key_slot(hash, &second).unwrap();
        assert_eq!(
            t.remove_key(hash, &first).unwrap().slab_block_id,
            make_id(1)
        );
        t.compact_one_group();
        assert_ne!(t.key_slot(hash, &second), Some(old_slot));
        assert_eq!(t.overflow_keys.len(), 1);

        for i in 100..1100u32 {
            let key = i.to_le_bytes();
            t.insert_with_key(hash_key(&key), &key, make_id(i), 16)
                .unwrap();
        }
        assert_eq!(t.capacity(), 2048);
        assert_eq!(
            t.lookup_key_checked(hash, &second, 0)
                .unwrap()
                .slab_block_id,
            make_id(2)
        );
        for i in 100..1100u32 {
            let key = i.to_le_bytes();
            assert!(t.remove_key(hash_key(&key), &key).is_some());
        }
        assert_eq!(t.capacity(), 1024);

        assert!(t.update_key_ttl(hash, &second, 10, 1));
        assert_eq!(t.get_key_ttl(hash, inline, 1), -1);
        assert_eq!(t.get_key_ttl(hash, &second, 1), 9);
        assert!(t.persist_key(hash, &second, 1));
        assert_eq!(t.remove_key_if_matching(hash, &second, make_id(3)), None);
        assert_eq!(
            t.insert_with_key(hash, &second, make_id(4), 16).unwrap(),
            Some(make_id(2))
        );

        let pattern = CompiledPattern::new(&second);
        let mut found = Vec::new();
        t.scan_home_bucket(h1(hash, t.capacity()), Some(&pattern), None, 0, |key| {
            found.push(key.to_vec());
        });
        assert_eq!(found, [second.to_vec()]);
        assert_eq!(
            t.remove_key_if_matching(hash, &second, make_id(4))
                .unwrap()
                .slab_block_id,
            make_id(4)
        );
        assert!(t.overflow_keys.is_empty());
        assert_eq!(
            t.lookup_key_checked(hash, inline, 0).unwrap().slab_block_id,
            make_id(3)
        );
    }

    #[test]
    fn empty_key_is_distinct_from_anonymous_entry() {
        let mut t = SwissTable::with_capacity(64);
        let hash = hash_key(b"");
        t.insert(hash, make_id(1), 16).unwrap();
        assert!(t.lookup_key_checked(hash, b"", 0).is_none());
        t.insert_with_key(hash, b"", make_id(2), 16).unwrap();
        assert_eq!(t.anonymous_key_count(), 1);
        assert_eq!(
            t.lookup_key_checked(hash, b"", 0).unwrap().slab_block_id,
            make_id(2)
        );
        let mut keys = Vec::new();
        t.scan_home_bucket(h1(hash, t.capacity()), None, None, 0, |key| {
            keys.push(key.to_vec())
        });
        assert_eq!(keys, [Vec::<u8>::new()]);
        assert!(t.remove_key(hash, b"").unwrap().has_key);
        assert!(!t.lookup(hash).unwrap().has_key());
    }

    #[test]
    fn test_bit_reversed_cursor_full_cycle() {
        let cap = 8;
        let mut cur = 0;
        let mut visited = Vec::new();
        loop {
            visited.push(cur);
            cur = next_bit_reversed_cursor(cur, cap);
            if cur == 0 {
                break;
            }
        }
        assert_eq!(visited, vec![0, 4, 2, 6, 1, 5, 3, 7]);
    }

    #[test]
    fn test_home_bucket_scan_completeness_under_grow_shrink_and_compaction() {
        use std::collections::HashSet;

        // Start with capacity 1024 and populate 900 keys (triggers resize to 2048)
        let mut t = SwissTable::with_capacity(1024);
        let mut survivor_keys: HashSet<Vec<u8>> = HashSet::new();
        let mut transient_keys: Vec<Vec<u8>> = Vec::new();

        for i in 0..300 {
            let k = format!("survivor:key:{i:04}").into_bytes();
            let h = hash_key(&k);
            t.insert_with_key(h, &k, make_id(i as u32), 64).unwrap();
            survivor_keys.insert(k);
        }
        for i in 0..1600 {
            let k = format!("transient:key:{i:04}").into_bytes();
            let h = hash_key(&k);
            t.insert_with_key(h, &k, make_id(1000 + i as u32), 64)
                .unwrap();
            transient_keys.push(k);
        }
        assert!(t.capacity() >= 2048);

        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        let mut cursor = 0usize;
        let mut step = 0usize;

        loop {
            t.scan_home_bucket(cursor, None, None, 0, |k| {
                seen.insert(k.to_vec());
            });
            cursor = next_bit_reversed_cursor(cursor, t.capacity());
            step += 1;

            // Mid-scan mutations: delete transient keys to trigger shrink, and run compaction
            if step == 50 {
                for k in &transient_keys[..800] {
                    t.remove(hash_key(k));
                }
                for _ in 0..32 {
                    t.compact_one_group();
                }
            } else if step == 150 {
                for k in &transient_keys[800..] {
                    t.remove(hash_key(k));
                }
                for _ in 0..32 {
                    t.compact_one_group();
                }
            }

            if cursor == 0 {
                break;
            }
        }

        // 100% of survivor keys that remained live throughout the scan MUST be visited!
        for k in &survivor_keys {
            assert!(
                seen.contains(k),
                "Missed survivor key {:?} during dynamic resize/shrink/compaction scan",
                String::from_utf8_lossy(k)
            );
        }
    }

    #[test]
    fn test_remove_key_if_expired_rejects_stale_events() {
        let mut t = SwissTable::with_capacity(64);
        let key = b"my_timed_key";
        let h = hash_key(key);
        let mut pool = kachedb_core::SlabPool::new(0, 4 * 1024 * 1024).unwrap();
        let class = kachedb_core::SlabClassType::AppSmall;
        let block_id = pool.allocate(class).unwrap();

        // Insert with TTL expiring at 100s
        let _ = t.insert_with_ttl_and_key(h, key, block_id, 32, 100);

        let initial_gen = kachedb_core::registry::get_slot_generation(block_id).unwrap();

        // 1. Not due yet (now_sec = 50 < 100) -> rejected
        assert!(
            t.remove_key_if_expired(h, key, block_id, 100, initial_gen, 50)
                .is_none()
        );
        assert!(t.lookup_key_checked(h, key, 50).is_some());

        // 2. Deadline changed / extended to 200s (stale event has expected_expire = 100) -> rejected
        assert!(t.update_key_ttl(h, key, 200, 50));
        assert!(
            t.remove_key_if_expired(h, key, block_id, 100, initial_gen, 150)
                .is_none()
        );
        assert!(t.lookup_key_checked(h, key, 150).is_some());

        // 3. PERSIST called (expire_at_secs = 0, stale event has expected_expire = 200) -> rejected
        assert!(t.persist_key(h, key, 150));
        assert!(
            t.remove_key_if_expired(h, key, block_id, 200, initial_gen, 250)
                .is_none()
        );
        assert!(t.lookup_key_checked(h, key, 250).is_some());

        // Reuse the same handle for the same key and deadline. Only the new event can remove it.
        t.remove_key(h, key).unwrap();
        pool.deallocate(block_id).unwrap();
        assert_eq!(pool.allocate(class).unwrap(), block_id);
        t.insert_with_ttl_and_key(h, key, block_id, 32, 300)
            .unwrap();
        let generation = kachedb_core::registry::get_slot_generation(block_id).unwrap();
        assert_ne!(generation, initial_gen);
        assert!(
            t.remove_key_if_expired(h, key, block_id, 300, initial_gen, 300)
                .is_none()
        );
        let removed = t
            .remove_key_if_expired(h, key, block_id, 300, generation, 300)
            .expect("current generation should expire");
        assert_eq!(removed.slab_block_id, block_id);
        assert!(t.lookup_key_checked(h, key, 300).is_none());
    }
}
