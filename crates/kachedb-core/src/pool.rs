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
pub const APP_CACHE_MAX_RATIO: f64 = 0.50;
/// Default fraction assigned to LLM tensor storage.
pub const TENSOR_CACHE_DEFAULT_RATIO: f64 = 0.80;
/// Hard ceiling ratio for tensor storage.
pub const TENSOR_CACHE_MAX_RATIO: f64 = 0.95;

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
    /// Monotonic counter for generating unique `slab_id` values.
    next_slab_id: u16,
    /// Maximum total megaslabs allowed across all workloads.
    max_megaslabs: usize,
    /// Dynamic quota budget for App Cache workload (Improvement 4).
    app_quota: WorkloadQuota,
    /// Dynamic quota budget for Tensor Cache workload (Improvement 4).
    tensor_quota: WorkloadQuota,
    /// Monotonic second timestamp of last activity per arena (for S3-FIFO cold detection).
    arena_last_active_sec: Vec<u32>,
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
        let mut pool = Self {
            core_id,
            arenas: Vec::new(),
            next_slab_id: 0,
            max_megaslabs,
            app_quota,
            tensor_quota,
            arena_last_active_sec: Vec::new(),
        };

        // Pre-warm one arena for each app-cache class.
        for class in [
            SlabClassType::AppSmall,
            SlabClassType::AppMedium,
            SlabClassType::AppLarge,
        ] {
            pool.grow(class)?;
        }

        Ok(pool)
    }

    /// Allocates one slot of the given `class`, growing into a new `MegaslabArena`
    /// if all existing arenas for that class are full.
    ///
    /// # Performance
    ///
    /// On the fast path (non-full arena exists), this is O(1) with no OS calls.
    ///
    /// # Errors
    ///
    /// - [`CoreError::PoolExhausted`] if the quota ceiling has been reached.
    /// - [`CoreError::OsAllocFailed`] if the OS cannot satisfy a new 2 MB allocation.
    pub fn allocate(&mut self, class: SlabClassType) -> Result<SlabBlockId, CoreError> {
        // Find first non-full arena serving this class.
        for i in 0..self.arenas.len() {
            if self.arenas[i].class() == class && !self.arenas[i].is_full() {
                return self.arenas[i].allocate();
            }
        }
        // No capacity — try to grow.
        self.grow(class)?;
        // Retry once: the freshly grown arena is guaranteed non-full.
        self.arenas
            .last_mut()
            .expect("grow always pushes an arena")
            .allocate()
    }

    /// Returns a previously allocated slot back to its owning arena.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidBlockId`] if no arena owns `id`.
    pub fn deallocate(&mut self, id: SlabBlockId) -> Result<(), CoreError> {
        let slab_id = id.slab_id();
        for arena in self.arenas.iter_mut() {
            if arena.header_slab_id() == slab_id as u32 {
                return arena.deallocate(id);
            }
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

    /// Returns a raw mutable pointer to the payload bytes of `id`.
    ///
    /// # Safety
    ///
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
        // Check workload-specific quota ceiling.
        let quota_ok = if class.is_tensor() {
            self.tensor_quota.can_borrow()
        } else {
            self.app_quota.can_borrow()
        };

        if !quota_ok {
            // Attempt to reclaim a cold arena from the opposite workload.
            if !self.reclaim_cold_arena(class) {
                return Err(CoreError::PoolExhausted { class });
            }
        }

        if self.arenas.len() >= self.max_megaslabs {
            return Err(CoreError::PoolExhausted { class });
        }

        let slab_id = self.next_slab_id;
        self.next_slab_id = self.next_slab_id.wrapping_add(1);
        let arena = MegaslabArena::new(class, slab_id, self.core_id)?;
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
            self.arenas.remove(idx);
            self.arena_last_active_sec.remove(idx);

            if released_class.is_tensor() {
                self.tensor_quota.release_one();
            } else {
                self.app_quota.release_one();
            }

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
        assert_eq!(id.slab_id(), 0); // first arena has slab_id=0
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
}
