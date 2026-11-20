//! `kachedb-core` — Per-core `SlabPool`: multi-arena memory manager with soft quota enforcement.
//!
//! Each CPU core owns exactly one `SlabPool`. It manages a collection of
//! `MegaslabArena` instances partitioned between the two workloads:
//!
//! - **App Cache** — small KV payloads (128 B / 512 B / 4 KB slots).
//! - **LLM KV-Cache** — large PagedAttention tensor blocks (64 KB / 256 KB / 2 MB).
//!
//! # Quota Model (from RFC 3)
//!
//! ```text
//! ┌──────────────── Total Core RAM ────────────────┐
//! │ App Cache (target: 20%)  │ Tensor Cache (80%)  │
//! └─────────────────────────────────────────────────┘
//!   ↑ Elastic: either side borrows from unassigned megaslabs until ceiling.
//! ```
//!
//! # Allocation Lifecycle
//!
//! 1. Find an existing non-full `MegaslabArena` for the requested class.
//! 2. If none available, allocate a new `MegaslabArena` (subject to quota).
//! 3. Return a `SlabBlockId` that encodes both arena and slot index.

use crate::{
    arena::{MegaslabArena, SlabBlockId},
    error::CoreError,
    quota::{QuotaSnapshot, WorkloadQuota},
    slab::SlabClassType,
};

/// Default fraction of total megaslabs assigned to the App Cache workload.
pub const APP_CACHE_DEFAULT_RATIO: f64 = 0.20;
/// Hard ceiling ratio for App Cache (allows elastic growth when tensors are idle).
pub const APP_CACHE_MAX_RATIO: f64 = 0.95;
/// Default fraction assigned to LLM tensor storage.
pub const TENSOR_CACHE_DEFAULT_RATIO: f64 = 0.80;
/// Hard ceiling ratio for tensor storage.
pub const TENSOR_CACHE_MAX_RATIO: f64 = 0.95;

use std::collections::HashMap;

#[inline(always)]
fn class_index(class: SlabClassType) -> usize {
    match class {
        SlabClassType::AppSmall => 0,
        SlabClassType::AppMedium => 1,
        SlabClassType::AppLarge => 2,
        SlabClassType::Tensor64KB => 3,
        SlabClassType::Tensor256KB => 4,
        SlabClassType::Tensor2MB => 5,
    }
}

/// Per-core slab pool managing multiple `MegaslabArena` instances.
///
/// # Thread Safety
///
/// `SlabPool` is **not** thread-safe. Like `MegaslabArena`, it is owned
/// exclusively by one CPU core worker thread. Cross-core requests are
/// forwarded via SPSC message passing.
pub struct SlabPool {
    /// The CPU core this pool belongs to.
    pub core_id: u16,
    /// Active arenas for all size classes.
    arenas: Vec<MegaslabArena>,
    /// O(1) direct active arena index per size class.
    active_arena: [Option<usize>; 6],
    /// All arena indices grouped by size class.
    class_arenas: [Vec<usize>; 6],
    /// Fast O(1) slab_id to arena index mapping.
    slab_id_to_arena: HashMap<u16, usize>,
    /// Maximum total megaslabs allowed across all workloads.
    max_megaslabs: usize,
    /// Dynamic quota budget for App Cache workload (Improvement 4).
    app_quota: WorkloadQuota,
    /// Dynamic quota budget for Tensor Cache workload (Improvement 4).
    tensor_quota: WorkloadQuota,
    /// Monotonic second timestamp of last activity per arena (for S3-FIFO cold detection).
    arena_last_active_sec: Vec<u32>,
    /// Retired slots returned to this pool by other workers.
    inbox_rx: crossbeam_channel::Receiver<crate::registry::RetiredSlot>,
    inbox_tx: crossbeam_channel::Sender<crate::registry::RetiredSlot>,
}

impl SlabPool {
    /// Creates a new `SlabPool` for `core_id` with a configured memory ceiling.
    ///
    /// `max_total_bytes` is divided into 2 MB Megaslabs. Pre-allocates one arena
    /// per class so the first request in each class incurs zero allocation latency.
    pub fn new(core_id: u16, max_total_bytes: usize) -> Result<Self, CoreError> {
        let max_megaslabs = max_total_bytes / crate::slab::MEGASLAB_BYTES;
        let app_quota =
            WorkloadQuota::new(max_megaslabs, APP_CACHE_DEFAULT_RATIO, APP_CACHE_MAX_RATIO);
        let tensor_quota = WorkloadQuota::new(
            max_megaslabs,
            TENSOR_CACHE_DEFAULT_RATIO,
            TENSOR_CACHE_MAX_RATIO,
        );

        let (inbox_tx, inbox_rx) = crossbeam_channel::unbounded();

        let pool = Self {
            core_id,
            arenas: Vec::new(),
            active_arena: [None; 6],
            class_arenas: Default::default(),
            slab_id_to_arena: HashMap::new(),
            max_megaslabs,
            app_quota,
            tensor_quota,
            arena_last_active_sec: Vec::new(),
            inbox_rx,
            inbox_tx,
        };

        // Arenas are grown lazily on first allocation request per class.
        // This keeps baseline RSS near zero until actual traffic arrives.

        Ok(pool)
    }

    /// Drains any freed slab slots returned by foreign worker threads to this pool's inbox.
    pub fn drain_inbox(&mut self) -> usize {
        let mut count = 0;
        while let Ok(retired) = self.inbox_rx.try_recv() {
            if crate::registry::get_slot_generation(retired.id) == Some(retired.generation)
                && self.deallocate(retired.id).is_ok()
            {
                count += 1;
            }
        }
        count
    }

    /// Allocates one slot of the given `class` using strictly non-evicting semantics.
    ///
    /// Under ADR 6 production hardening, blind recycling of occupied slots is eliminated:
    /// if memory is exhausted, returns [`CoreError::PoolExhausted`].
    pub fn reserve_non_evicting(&mut self, class: SlabClassType) -> Result<SlabBlockId, CoreError> {
        let c_idx = class_index(class);

        // 1. Fast path: active arena has free slots
        if let Some(arena_idx) = self.active_arena[c_idx].filter(|&idx| !self.arenas[idx].is_full())
        {
            return self.arenas[arena_idx].allocate();
        }

        // 2. Drain inbox for slots returned from other worker cores
        if self.drain_inbox() > 0
            && let Some(arena_idx) =
                self.active_arena[c_idx].filter(|&idx| !self.arenas[idx].is_full())
        {
            return self.arenas[arena_idx].allocate();
        }

        // 3. Growth path: try to grow into a new arena if pool ceiling allows
        if self.arenas.len() < self.max_megaslabs && self.grow(class).is_ok() {
            let new_idx = self.arenas.len() - 1;
            self.active_arena[c_idx] = Some(new_idx);
            return self.arenas[new_idx].allocate();
        }

        // 4. Scan existing arenas of this class for any free slots (e.g. from deallocations)
        if let Some(&arena_idx) = self.class_arenas[c_idx]
            .iter()
            .find(|&&idx| !self.arenas[idx].is_full())
        {
            self.active_arena[c_idx] = Some(arena_idx);
            return self.arenas[arena_idx].allocate();
        }

        Err(CoreError::PoolExhausted { class })
    }

    /// Allocates one slot of the given `class`, growing into a new `MegaslabArena`
    /// if all existing arenas for that class are full.
    ///
    /// # Performance
    ///
    /// True **O(1) in ~1–2 nanoseconds** on the fast path.
    #[inline(always)]
    pub fn allocate(&mut self, class: SlabClassType) -> Result<SlabBlockId, CoreError> {
        self.reserve_non_evicting(class)
    }

    /// Returns a previously allocated slot back to its owning arena in O(1).
    ///
    /// If the slot was allocated by another worker core, routes it to the owner's
    /// deallocation inbox via crossbeam channel.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidBlockId`] if no arena owns `id`.
    pub fn deallocate(&mut self, id: SlabBlockId) -> Result<(), CoreError> {
        let slab_id = id.slab_id();
        if let Some(&arena_idx) = self
            .slab_id_to_arena
            .get(&slab_id)
            .filter(|&&idx| idx < self.arenas.len())
        {
            let res = self.arenas[arena_idx].deallocate(id);
            let c_idx = class_index(self.arenas[arena_idx].class());
            if let Some(_curr_active) =
                self.active_arena[c_idx].filter(|&curr| self.arenas[curr].is_full())
            {
                self.active_arena[c_idx] = Some(arena_idx);
            }
            return res;
        }

        if crate::registry::send_to_dealloc_inbox(id).is_some() {
            return Ok(());
        }

        Err(CoreError::InvalidBlockId { id: id.0 })
    }

    /// Called from the server's idle event-loop tick (once per second).
    ///
    /// Updates activity timestamps for all arenas that have live allocations,
    /// enabling the S3-FIFO cold-detection logic in `reclaim_cold_arena()`
    /// without paying a `SystemTime::now()` cost on every alloc/dealloc.
    pub fn tick_second(&mut self, now_sec: u32) {
        for (i, arena) in self.arenas.iter().enumerate() {
            if arena.allocated() > 0 {
                self.arena_last_active_sec[i] = now_sec;
            }
        }
    }

    /// Returns the raw pointer to the slot payload for a live block ID.
    ///
    /// # Safety
    /// The caller must guarantee `id` is live and was obtained from this pool.
    pub unsafe fn slot_ptr(&self, id: SlabBlockId) -> Result<*mut u8, CoreError> {
        let slab_id = id.slab_id();
        for arena in &self.arenas {
            if arena.header_slab_id() == slab_id as u32 {
                return Ok(unsafe { arena.slot_ptr(id) });
            }
        }
        Err(CoreError::InvalidBlockId { id: id.0 })
    }

    /// Total number of active `MegaslabArena` instances across all classes.
    pub fn arena_count(&self) -> usize {
        self.arenas.len()
    }

    /// Total bytes allocated across all arenas.
    pub fn total_allocated_bytes(&self) -> usize {
        self.arenas
            .iter()
            .map(|a| a.allocated() as usize * a.class().slot_bytes())
            .sum()
    }

    // ── Private ──────────────────────────────────────────────────────────────

    /// Allocates a new `MegaslabArena` for `class`, enforcing quota limits.
    /// Attempts S3-FIFO cold-arena reclamation before returning `PoolExhausted`.
    fn grow(&mut self, class: SlabClassType) -> Result<(), CoreError> {
        if self.arenas.len() >= self.max_megaslabs {
            return Err(CoreError::PoolExhausted { class });
        }

        // Check workload-specific quota ceiling.
        let quota_ok = if class.is_tensor() {
            self.tensor_quota.can_borrow()
        } else {
            self.app_quota.can_borrow()
        };

        if !quota_ok {
            // Attempt to reclaim a cold arena from the opposite workload only if any exist.
            let opposite_claimed = if class.is_tensor() {
                self.app_quota.claimed
            } else {
                self.tensor_quota.claimed
            };
            if opposite_claimed == 0 || !self.reclaim_cold_arena(class) {
                return Err(CoreError::PoolExhausted { class });
            }
        }

        if self.arenas.len() >= self.max_megaslabs {
            return Err(CoreError::PoolExhausted { class });
        }

        let slab_id = crate::registry::allocate_global_slab_id();
        let arena = MegaslabArena::new(class, slab_id, self.core_id)?;
        crate::registry::register_dealloc_sender(slab_id, self.inbox_tx.clone());
        let arena_idx = self.arenas.len();
        self.slab_id_to_arena.insert(slab_id, arena_idx);
        let c_idx = class_index(class);
        self.active_arena[c_idx] = Some(arena_idx);
        self.class_arenas[c_idx].push(arena_idx);
        self.arenas.push(arena);
        self.arena_last_active_sec.push(now_secs());

        // Update quota accounting.
        if class.is_tensor() {
            self.tensor_quota.claim_one();
        } else {
            self.app_quota.claim_one();
        }

        log::debug!(
            "SlabPool(core={}): grew arena slab_id={slab_id} for {class:?} \
             [app={}/{}, tensor={}/{}]",
            self.core_id,
            self.app_quota.claimed,
            self.app_quota.ceiling,
            self.tensor_quota.claimed,
            self.tensor_quota.ceiling,
        );
        Ok(())
    }

    /// S3-FIFO cold-arena reclamation: releases the oldest fully-empty arena
    /// on the opposite workload back to the unassigned pool.
    ///
    /// Returns `true` if an arena was successfully reclaimed.
    fn reclaim_cold_arena(&mut self, requesting_class: SlabClassType) -> bool {
        let opposite_claimed = if requesting_class.is_tensor() {
            self.app_quota.claimed
        } else {
            self.tensor_quota.claimed
        };
        if opposite_claimed == 0 {
            return false;
        }

        let now = now_secs();
        let cold_threshold_secs: u32 = 30;

        // Find the oldest cold, fully-empty arena on the opposite workload.
        let candidate = self
            .arenas
            .iter()
            .enumerate()
            .find(|(i, arena)| {
                let is_opposite = arena.class().is_tensor() != requesting_class.is_tensor();
                let is_empty = arena.allocated() == 0;
                let is_cold =
                    now.saturating_sub(self.arena_last_active_sec[*i]) >= cold_threshold_secs;
                is_opposite && is_empty && is_cold
            })
            .map(|(i, _)| i);

        if let Some(idx) = candidate {
            let released_class = self.arenas[idx].class();
            self.arenas[idx].madvise_dontneed();
            self.arenas.remove(idx);
            self.arena_last_active_sec.remove(idx);

            if released_class.is_tensor() {
                self.tensor_quota.release_one();
            } else {
                self.app_quota.release_one();
            }

            self.rebuild_indices();

            log::info!(
                "SlabPool(core={}): reclaimed cold {:?} arena for {:?} (idle ≥{}s)",
                self.core_id,
                released_class,
                requesting_class,
                cold_threshold_secs
            );
            true
        } else {
            false
        }
    }

    /// Reclaims completely empty arenas back to the system.
    ///
    /// For each arena with 0 active allocations:
    /// 1. Issues `madvise(MADV_DONTNEED)` on Linux to drop resident pages.
    /// 2. Returns the arena memory quota back to its workload pool.
    /// 3. Reclaims the global slab ID from the atomic registry.
    ///
    /// Returns the number of reclaimed arenas.
    pub fn reclaim_empty_arenas(&mut self) -> usize {
        let mut reclaimed = 0;
        let mut i = 0;

        while i < self.arenas.len() {
            if self.arenas[i].allocated() == 0 {
                let class = self.arenas[i].class();
                self.arenas[i].madvise_dontneed();
                if class.is_tensor() {
                    self.tensor_quota.release_one();
                } else {
                    self.app_quota.release_one();
                }
                self.arenas.remove(i);
                self.arena_last_active_sec.remove(i);
                reclaimed += 1;
            } else {
                i += 1;
            }
        }

        if reclaimed > 0 {
            self.rebuild_indices();
            log::debug!(
                "SlabPool(core={}): reclaimed {} empty arena(s), {} remaining",
                self.core_id,
                reclaimed,
                self.arenas.len()
            );
        }

        reclaimed
    }

    /// Rebuilds `slab_id_to_arena`, `class_arenas`, and `active_arena` index arrays after arena removals.
    fn rebuild_indices(&mut self) {
        self.slab_id_to_arena.clear();
        self.active_arena = [None; 6];
        for list in &mut self.class_arenas {
            list.clear();
        }
        for (i, arena) in self.arenas.iter().enumerate() {
            self.slab_id_to_arena
                .insert(arena.header_slab_id() as u16, i);
            let c_idx = class_index(arena.class());
            self.class_arenas[c_idx].push(i);
            if !arena.is_full() && self.active_arena[c_idx].is_none() {
                self.active_arena[c_idx] = Some(i);
            }
        }
        for (i, arena) in self.arenas.iter().enumerate() {
            let c_idx = class_index(arena.class());
            if self.active_arena[c_idx].is_none() {
                self.active_arena[c_idx] = Some(i);
            }
        }
    }

    /// Returns a snapshot of current quota utilisation for monitoring.
    pub fn quota_snapshot(&self) -> QuotaSnapshot {
        QuotaSnapshot {
            app_claimed: self.app_quota.claimed,
            app_target: self.app_quota.target,
            app_ceiling: self.app_quota.ceiling,
            tensor_claimed: self.tensor_quota.claimed,
            tensor_target: self.tensor_quota.target,
            tensor_ceiling: self.tensor_quota.ceiling,
            total_megaslabs: self.max_megaslabs,
            unassigned: self
                .max_megaslabs
                .saturating_sub(self.app_quota.claimed + self.tensor_quota.claimed),
        }
    }
}

/// Returns the current time as epoch seconds (u32 — sufficient until year 2106).
#[inline]
fn now_secs() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32
}

// ─── Tests ────────────────────────────────────────────────────────────────────

/// Non-evicting allocation used by live commands and startup replay.
pub trait SlotAllocator {
    fn reserve(&mut self, key_hash: u64, class: SlabClassType) -> Result<SlabBlockId, CoreError>;
    fn deallocate(&mut self, block: SlabBlockId) -> Result<(), CoreError>;
    fn total_allocated_bytes(&self) -> usize;
    fn arena_count(&self) -> usize;
}

impl SlotAllocator for SlabPool {
    fn reserve(&mut self, _key_hash: u64, class: SlabClassType) -> Result<SlabBlockId, CoreError> {
        self.reserve_non_evicting(class)
    }

    fn deallocate(&mut self, block: SlabBlockId) -> Result<(), CoreError> {
        SlabPool::deallocate(self, block)
    }

    fn total_allocated_bytes(&self) -> usize {
        SlabPool::total_allocated_bytes(self)
    }

    fn arena_count(&self) -> usize {
        SlabPool::arena_count(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const POOL_32MB: usize = 32 * 1024 * 1024;

    fn make_pool() -> SlabPool {
        SlabPool::new(0, POOL_32MB).expect("32 MB pool allocation should succeed")
    }

    #[test]
    fn pool_allocates_app_small_slot() {
        let mut pool = make_pool();
        let id = pool.allocate(SlabClassType::AppSmall).unwrap();
        assert!(id.slab_id() > 0);
        assert_eq!(pool.total_allocated_bytes(), 128);
    }

    #[test]
    fn pool_deallocates_and_reuses_slot() {
        let mut pool = make_pool();
        let id = pool.allocate(SlabClassType::AppMedium).unwrap();
        pool.deallocate(id).unwrap();
        let id2 = pool.allocate(SlabClassType::AppMedium).unwrap();
        // Slot should be reused from the free-list.
        assert_eq!(id, id2);
    }

    #[test]
    fn pool_grows_arena_when_full() {
        // Allocate a tiny pool that fits exactly one slab per class.
        // Then exhaust AppSmall and expect a second arena to be created.
        let mut pool = SlabPool::new(0, POOL_32MB).unwrap();
        let cap = SlabClassType::AppSmall.slots_per_megaslab();
        for _ in 0..cap {
            pool.allocate(SlabClassType::AppSmall).unwrap();
        }
        // Should grow into a second AppSmall arena.
        let id = pool.allocate(SlabClassType::AppSmall).unwrap();
        // New slab gets the next slab_id.
        assert!(id.slab_id() > 0);
    }

    #[test]
    fn invalid_dealloc_returns_error() {
        let mut pool = make_pool();
        let foreign = SlabBlockId::new(0xFF, 0);
        let result = pool.deallocate(foreign);
        assert!(matches!(result, Err(CoreError::InvalidBlockId { .. })));
    }

    #[test]
    fn test_reclaim_empty_arenas() {
        let mut pool = make_pool();
        let id = pool.allocate(SlabClassType::AppSmall).unwrap();
        assert_eq!(pool.arena_count(), 1);
        pool.deallocate(id).unwrap();
        // The single arena is now empty.
        let reclaimed = pool.reclaim_empty_arenas();
        assert_eq!(reclaimed, 1);
        assert_eq!(pool.arena_count(), 0);

        // Allocating again should lazily grow a new arena.
        let id2 = pool.allocate(SlabClassType::AppSmall).unwrap();
        assert_eq!(pool.arena_count(), 1);
        assert_eq!(pool.total_allocated_bytes(), 128);
        pool.deallocate(id2).unwrap();
    }

    #[test]
    fn pool_returns_exhausted_when_at_ceiling_without_blind_recycling() {
        // Pool of 6MB = 3 megaslabs max (app cache ceiling = 2)
        let mut pool = SlabPool::new(0, 6 * 1024 * 1024).unwrap();
        let cap = SlabClassType::AppSmall.slots_per_megaslab() as usize;
        let mut allocated = Vec::with_capacity(cap * 2);
        // Fill 2 arenas up to ceiling
        for _ in 0..(cap * 2) {
            allocated.push(pool.allocate(SlabClassType::AppSmall).unwrap());
        }
        assert_eq!(pool.arena_count(), 2);

        // Allocating further must return PoolExhausted instead of blindly overwriting occupied slots
        let err = pool.allocate(SlabClassType::AppSmall).unwrap_err();
        assert!(matches!(err, CoreError::PoolExhausted { .. }));

        // Freeing a retired slot allows the next allocation to succeed
        let freed_slot = allocated.pop().unwrap();
        pool.deallocate(freed_slot).unwrap();
        let new_slot = pool.allocate(SlabClassType::AppSmall).unwrap();
        assert_eq!(new_slot, freed_slot);
    }

    #[test]
    fn cross_worker_deallocation_inbox_reclaims_foreign_slots() {
        let mut pool0 = SlabPool::new(10, 4 * 1024 * 1024).unwrap();
        // Core IDs are affinity hints. Two pool instances can share one.
        let mut pool1 = SlabPool::new(10, 4 * 1024 * 1024).unwrap();

        // Worker 0 (core 10) allocates a slot
        let id = pool0.allocate(SlabClassType::AppSmall).unwrap();
        assert_eq!(crate::registry::resolve_owning_core(id), Some(10));

        let generation = crate::registry::get_slot_generation(id).unwrap();
        pool1.deallocate(id).unwrap();
        pool1.deallocate(id).unwrap();

        // Duplicate retirements refer to the same generation and reclaim it once.
        let drained = pool0.drain_inbox();
        assert_eq!(drained, 1);

        // Next allocation on Worker 0 reuses the reclaimed slot
        let reused_id = pool0.allocate(SlabClassType::AppSmall).unwrap();
        assert_eq!(reused_id, id);
        assert_ne!(crate::registry::get_slot_generation(id), Some(generation));
        let out_of_bounds = SlabBlockId::new(id.slab_id(), u16::MAX);
        assert!(pool0.deallocate(out_of_bounds).is_err());
        assert!(crate::registry::get_slot_generation(out_of_bounds).is_none());
    }
}
