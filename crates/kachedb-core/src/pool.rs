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
}

impl SlabPool {
    /// Creates a new `SlabPool` for `core_id` with a configured memory ceiling.
    ///
    /// `max_total_bytes` is divided into 2 MB Megaslabs. Pre-allocates one arena
    /// per class so the first request in each class incurs zero allocation latency.
    pub fn new(core_id: u16, max_total_bytes: usize) -> Result<Self, CoreError> {
        let max_megaslabs = max_total_bytes / crate::slab::MEGASLAB_BYTES;
        let mut pool = Self {
            core_id,
            arenas: Vec::new(),
            next_slab_id: 0,
            max_megaslabs,
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
        for arena in self.arenas.iter_mut().filter(|a| a.class() == class) {
            if !arena.is_full() {
                return arena.allocate();
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
        for arena in &mut self.arenas {
            // The slab_id is encoded in the upper 16 bits of the block id.
            if arena.header_slab_id() == slab_id as u32 {
                return arena.deallocate(id);
            }
        }
        Err(CoreError::InvalidBlockId { id: id.0 })
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
        self.arenas.iter().map(|a| {
            a.allocated() as usize * a.class().slot_bytes()
        }).sum()
    }

    // ── Private ──────────────────────────────────────────────────────────────

    /// Allocates a new `MegaslabArena` for `class` and pushes it onto `self.arenas`.
    fn grow(&mut self, class: SlabClassType) -> Result<(), CoreError> {
        if self.arenas.len() >= self.max_megaslabs {
            return Err(CoreError::PoolExhausted { class });
        }
        let slab_id = self.next_slab_id;
        self.next_slab_id = self.next_slab_id.wrapping_add(1);
        let arena = MegaslabArena::new(class, slab_id, self.core_id)?;
        self.arenas.push(arena);
        log::debug!(
            "SlabPool(core={}): grew arena slab_id={slab_id} for {class:?}",
            self.core_id
        );
        Ok(())
    }
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
