//! `kachedb-core` — Megaslab arena: 2 MB aligned page allocation and slot management.
//!
//! Each `MegaslabArena` owns exactly one 2 MB OS-allocated memory region.
//! The first 64 bytes are occupied by a `MegaslabHeader`; the remaining bytes
//! are subdivided into fixed-size slots according to the assigned `SlabClassType`.
//!
//! # Allocation Strategy
//!
//! Free slots are tracked with a bump pointer (`next_free_slot`) combined with
//! a compact free-list (`free_slots: Vec<u32>`) for O(1) reclaimed-slot reuse.
//!
//! - **Alloc hot-path:** pop from `free_slots` if non-empty; else bump.
//! - **Dealloc hot-path:** push slot index onto `free_slots`.
//!
//! This guarantees zero dynamic `malloc`/`free` during request execution after
//! the initial arena bootstrap.

use std::{
    alloc::{self, Layout},
    ptr::NonNull,
    sync::atomic::{AtomicU32, Ordering},
};

use crate::{
    error::CoreError,
    slab::{CACHE_LINE_BYTES, MEGASLAB_BYTES, MEGASLAB_HEADER_BYTES, SlabClassType},
};

// ─── Magic constant ───────────────────────────────────────────────────────────

/// Magic number embedded in every `MegaslabHeader` for integrity checks.
/// Encodes `KACH` in little-endian ASCII: 0x4B_41_43_48.
const MEGASLAB_MAGIC: u32 = 0x4B41_4348;

// ─── MegaslabHeader ───────────────────────────────────────────────────────────

/// 64-byte header placed at the base of every 2 MB Megaslab allocation.
///
/// The header occupies exactly one CPU cache line (`CACHE_LINE_BYTES`).
/// It stores enough metadata to identify the slab's owner, class, and
/// occupancy without requiring pointer arithmetic or external data structures.
///
/// # Layout
///
/// ```text
/// Offset 0  : magic          (u32)  — integrity sentinel 0x4B414348 ("KACH")
/// Offset 4  : slab_id        (u32)  — unique monotonic ID
/// Offset 8  : class_type     (u8)   — `SlabClassType` discriminant
/// Offset 9  : _pad1          (3 B)
/// Offset 12 : total_slots    (u32)  — immutable slot capacity
/// Offset 16 : allocated_slots(u32)  — current live allocations (atomic)
/// Offset 20 : owning_core    (u16)  — CPU core that owns this slab
/// Offset 22 : _pad2          (42 B) — padding to fill 64 bytes
/// ```
#[repr(C, align(64))]
pub struct MegaslabHeader {
    /// Integrity sentinel (`KACH` = `0x4B41_4348`).
    pub magic: u32,
    /// Unique monotonic identifier assigned at allocation time.
    pub slab_id: u32,
    /// Discriminant of `SlabClassType` this slab is bound to.
    pub class_type: u8,
    _pad1: [u8; 3],
    /// Total number of slots (immutable after construction).
    pub total_slots: u32,
    /// Atomic live-allocation counter.
    pub allocated_slots: AtomicU32,
    /// The CPU core index that owns and manages this slab.
    pub owning_core: u16,
    /// Pad to 64 bytes.
    _pad2: [u8; 42],
}

const _: () = assert!(
    std::mem::size_of::<MegaslabHeader>() == CACHE_LINE_BYTES,
    "MegaslabHeader must be exactly 64 bytes"
);

// ─── SlabBlockId ──────────────────────────────────────────────────────────────

/// An opaque handle identifying a single allocated slot within an arena.
///
/// The upper 16 bits encode the `slab_id`; the lower 16 bits encode the
/// slot index within that slab. This packing makes the ID self-contained
/// and avoids secondary look-ups for simple validity checks.
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────┐
/// │  bits 31..16: slab_id (u16)   │  bits 15..0: slot_index (u16)  │
/// └─────────────────────────────────────────────────────────────────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SlabBlockId(pub u32);

impl SlabBlockId {
    /// Constructs a new `SlabBlockId` from a slab and slot index.
    #[inline(always)]
    pub fn new(slab_id: u16, slot_index: u16) -> Self {
        Self(((slab_id as u32) << 16) | (slot_index as u32))
    }

    /// Extracts the slab identifier.
    #[inline(always)]
    pub fn slab_id(self) -> u16 {
        (self.0 >> 16) as u16
    }

    /// Extracts the slot index within its slab.
    #[inline(always)]
    pub fn slot_index(self) -> u16 {
        (self.0 & 0xFFFF) as u16
    }
}

// ─── MegaslabArena ────────────────────────────────────────────────────────────

/// Manages a single 2 MB OS-allocated memory arena subdivided into
/// fixed-size slots of one `SlabClassType`.
///
/// # Thread Safety
///
/// `MegaslabArena` is **not** thread-safe by design. Each CPU core owns its
/// arena(s) exclusively; cross-core access is mediated through the SPSC message
/// passing layer defined in `kachedb-net`.
pub struct MegaslabArena {
    /// Raw pointer to the 2 MB aligned allocation.
    base: NonNull<u8>,
    /// The slab class this arena serves.
    class: SlabClassType,
    /// Monotonic identifier for this arena instance.
    slab_id: u16,
    /// Next uninitialized slot index (bump pointer).
    next_free_slot: u32,
    /// Total slot capacity for this class.
    capacity: u32,
    /// Reclaimed slot indices available for immediate reuse.
    free_slots: Vec<u32>,
}

// SAFETY: The memory region is exclusively owned by one thread/core.
unsafe impl Send for MegaslabArena {}

impl MegaslabArena {
    /// Allocates a 2 MB OS page and initializes the `MegaslabHeader`.
    ///
    /// Uses `alloc::alloc` with a `MEGASLAB_BYTES`-aligned `Layout`, which
    /// on Linux production systems maps to `mmap` with `MAP_POPULATE` set
    /// for Transparent Huge Page eligibility.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::OsAllocFailed`] if the OS allocation fails.
    pub fn new(class: SlabClassType, slab_id: u16, owning_core: u16) -> Result<Self, CoreError> {
        // SAFETY: MEGASLAB_BYTES is non-zero and a power of two — valid Layout.
        let layout = Layout::from_size_align(MEGASLAB_BYTES, MEGASLAB_BYTES)
            .expect("MEGASLAB_BYTES is a valid alignment");

        let base = unsafe {
            let ptr = alloc::alloc_zeroed(layout);
            NonNull::new(ptr).ok_or_else(|| CoreError::OsAllocFailed {
                reason: "alloc_zeroed returned null".into(),
            })?
        };

        let capacity = class.slots_per_megaslab();

        // Write the header into the first 64 bytes of the allocation.
        // SAFETY: `base` is valid, aligned, and exclusively owned.
        unsafe {
            let header = base.as_ptr() as *mut MegaslabHeader;
            (*header).magic = MEGASLAB_MAGIC;
            (*header).slab_id = slab_id as u32;
            (*header).class_type = class as u8;
            (*header).total_slots = capacity;
            (*header).owning_core = owning_core;
            // `allocated_slots` is zero-initialized by `alloc_zeroed`.
        }

        crate::registry::register_arena(slab_id, base.as_ptr(), class.slot_bytes());

        log::debug!(
            "MegaslabArena: allocated slab_id={slab_id} class={class:?} \
             capacity={capacity} core={owning_core}"
        );

        Ok(Self {
            base,
            class,
            slab_id,
            next_free_slot: 0,
            capacity,
            free_slots: Vec::new(),
        })
    }

    /// Allocates one slot and returns its `SlabBlockId`.
    ///
    /// 1. If the free-list has reclaimed slots, reuse the most recent one.
    /// 2. Otherwise, advance the bump pointer.
    ///
    /// **Hot path: ~10–20 ns on a warm L1 cache.**
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::PoolExhausted`] when all slots are occupied.
    #[inline]
    pub fn allocate(&mut self) -> Result<SlabBlockId, CoreError> {
        let slot_index = if let Some(idx) = self.free_slots.pop() {
            idx
        } else if self.next_free_slot < self.capacity {
            let idx = self.next_free_slot;
            self.next_free_slot += 1;
            idx
        } else {
            return Err(CoreError::PoolExhausted { class: self.class });
        };

        // Increment the atomic counter for observability.
        self.header_mut()
            .allocated_slots
            .fetch_add(1, Ordering::Relaxed);

        Ok(SlabBlockId::new(self.slab_id, slot_index as u16))
    }

    /// Returns a slot back to this arena for immediate reuse.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidBlockId`] if the `id` does not belong to
    /// this arena.
    #[inline]
    pub fn deallocate(&mut self, id: SlabBlockId) -> Result<(), CoreError> {
        if id.slab_id() != self.slab_id {
            return Err(CoreError::InvalidBlockId { id: id.0 });
        }
        self.free_slots.push(id.slot_index() as u32);
        self.header_mut()
            .allocated_slots
            .fetch_sub(1, Ordering::Relaxed);
        Ok(())
    }

    /// Returns a raw pointer to the start of a slot's payload region.
    ///
    /// # Safety
    ///
    /// Caller must ensure `id` was obtained from this arena and is currently
    /// live (i.e., has not been deallocated).
    #[inline]
    pub unsafe fn slot_ptr(&self, id: SlabBlockId) -> *mut u8 {
        let offset = MEGASLAB_HEADER_BYTES + (id.slot_index() as usize) * self.class.slot_bytes();
        unsafe { self.base.as_ptr().add(offset) }
    }

    /// Returns a byte slice covering the payload of an allocated slot.
    ///
    /// # Safety
    ///
    /// See [`slot_ptr`](Self::slot_ptr).
    #[inline]
    pub unsafe fn slot_bytes(&self, id: SlabBlockId) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.slot_ptr(id), self.class.slot_bytes()) }
    }

    /// Returns `true` if all slots are currently allocated.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.next_free_slot >= self.capacity && self.free_slots.is_empty()
    }

    /// Returns `true` if no slots are currently allocated.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.header().allocated_slots.load(Ordering::Relaxed) == 0
    }

    /// Number of slots currently allocated.
    #[inline]
    pub fn allocated(&self) -> u32 {
        self.header().allocated_slots.load(Ordering::Relaxed)
    }

    /// Total slot capacity.
    #[inline]
    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    /// The `SlabClassType` this arena serves.
    #[inline]
    pub fn class(&self) -> SlabClassType {
        self.class
    }

    /// Returns the `slab_id` embedded in this arena's `MegaslabHeader`.
    /// Used by `SlabPool` to route `deallocate` calls to the correct arena.
    #[inline]
    pub fn header_slab_id(&self) -> u32 {
        self.header().slab_id
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    fn header(&self) -> &MegaslabHeader {
        // SAFETY: The first 64 bytes are always a valid `MegaslabHeader`.
        unsafe { &*(self.base.as_ptr() as *const MegaslabHeader) }
    }

    fn header_mut(&mut self) -> &mut MegaslabHeader {
        // SAFETY: Exclusive ownership — no concurrent mutations.
        unsafe { &mut *(self.base.as_ptr() as *mut MegaslabHeader) }
    }
}

impl Drop for MegaslabArena {
    fn drop(&mut self) {
        crate::registry::unregister_arena(self.slab_id);
        // SAFETY: `base` was allocated with the same layout; exclusive ownership.
        let layout =
            Layout::from_size_align(MEGASLAB_BYTES, MEGASLAB_BYTES).expect("valid layout at drop");
        unsafe { alloc::dealloc(self.base.as_ptr(), layout) };
        log::debug!("MegaslabArena: freed slab_id={}", self.slab_id);
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_arena(class: SlabClassType) -> MegaslabArena {
        MegaslabArena::new(class, 1, 0).expect("allocation should succeed")
    }

    #[test]
    fn header_magic_is_correct() {
        let arena = make_arena(SlabClassType::AppSmall);
        assert_eq!(arena.header().magic, MEGASLAB_MAGIC);
    }

    #[test]
    fn allocate_and_deallocate_single_slot() {
        let mut arena = make_arena(SlabClassType::AppSmall);
        let id = arena.allocate().expect("first alloc succeeds");
        assert_eq!(arena.allocated(), 1);
        arena.deallocate(id).expect("dealloc succeeds");
        assert_eq!(arena.allocated(), 0);
    }

    #[test]
    fn fill_arena_then_overflow() {
        let mut arena = make_arena(SlabClassType::AppSmall);
        let cap = arena.capacity();
        for _ in 0..cap {
            arena.allocate().expect("should succeed until full");
        }
        assert!(arena.is_full());
        let result = arena.allocate();
        assert!(matches!(result, Err(CoreError::PoolExhausted { .. })));
    }

    #[test]
    fn reclaim_then_reallocate() {
        let mut arena = make_arena(SlabClassType::AppMedium);
        let id1 = arena.allocate().unwrap();
        let id2 = arena.allocate().unwrap();
        arena.deallocate(id1).unwrap();
        // The reclaimed id1 slot should be reused next.
        let id3 = arena.allocate().unwrap();
        assert_eq!(id3, id1);
        let _ = id2;
    }

    #[test]
    fn slot_ptr_within_arena_bounds() {
        let mut arena = make_arena(SlabClassType::AppLarge);
        let id = arena.allocate().unwrap();
        let base_addr = arena.base.as_ptr() as usize;
        let ptr = unsafe { arena.slot_ptr(id) } as usize;
        let end = base_addr + MEGASLAB_BYTES;
        assert!(ptr >= base_addr + MEGASLAB_HEADER_BYTES);
        assert!(ptr + SlabClassType::AppLarge.slot_bytes() <= end);
    }

    #[test]
    fn invalid_block_id_rejected() {
        let mut arena = make_arena(SlabClassType::AppSmall);
        // slab_id=2 doesn't match slab_id=1
        let foreign_id = SlabBlockId::new(2, 0);
        let result = arena.deallocate(foreign_id);
        assert!(matches!(result, Err(CoreError::InvalidBlockId { .. })));
    }
}
