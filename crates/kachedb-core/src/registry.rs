//! `kachedb-core` — Lock-free Global Slab Registry for cross-core zero-copy reads.
//!
//! Maps each globally unique `slab_id` (u16) to its 2 MB memory base pointer and
//! slot size. This allows any worker thread to resolve a [`SlabBlockId`] into a
//! valid raw memory pointer in a single CPU dereference (~1.2 ns) with zero locks.

use std::{
    ptr::null_mut,
    sync::atomic::{AtomicPtr, AtomicU16, AtomicU32, Ordering},
};

use crate::{arena::SlabBlockId, slab::MEGASLAB_HEADER_BYTES};

/// Maximum supported 2 MB megaslabs in the process (16,384 * 2 MB = 32 GB RAM).
pub const MAX_GLOBAL_SLABS: usize = 16_384;

static NEXT_GLOBAL_SLAB_ID: AtomicU16 = AtomicU16::new(1);

struct ArenaEntry {
    base_ptr: AtomicPtr<u8>,
    slot_bytes: AtomicU32,
}

impl ArenaEntry {
    const fn new() -> Self {
        Self {
            base_ptr: AtomicPtr::new(null_mut()),
            slot_bytes: AtomicU32::new(0),
        }
    }
}

// Fixed-size global registry array initialized at compile-time
static GLOBAL_ARENAS: [ArenaEntry; MAX_GLOBAL_SLABS] = {
    // Const initialization of 16k elements
    const INIT: ArenaEntry = ArenaEntry::new();
    [INIT; MAX_GLOBAL_SLABS]
};

/// Atomically allocates a new globally unique `slab_id`.
#[inline]
pub fn allocate_global_slab_id() -> u16 {
    let id = NEXT_GLOBAL_SLAB_ID.fetch_add(1, Ordering::Relaxed);
    if id as usize >= MAX_GLOBAL_SLABS {
        panic!("Exceeded maximum global slab arenas ({MAX_GLOBAL_SLABS})");
    }
    id
}

/// Registers an active `MegaslabArena` in the global lock-free table.
#[inline]
pub fn register_arena(slab_id: u16, base_ptr: *mut u8, slot_bytes: usize) {
    let idx = slab_id as usize;
    if idx < MAX_GLOBAL_SLABS {
        GLOBAL_ARENAS[idx]
            .base_ptr
            .store(base_ptr, Ordering::Release);
        GLOBAL_ARENAS[idx]
            .slot_bytes
            .store(slot_bytes as u32, Ordering::Release);
    }
}

/// Unregisters an arena from the global table upon deallocation.
#[inline]
pub fn unregister_arena(slab_id: u16) {
    let idx = slab_id as usize;
    if idx < MAX_GLOBAL_SLABS {
        GLOBAL_ARENAS[idx]
            .base_ptr
            .store(null_mut(), Ordering::Release);
        GLOBAL_ARENAS[idx].slot_bytes.store(0, Ordering::Release);
    }
}

/// Resolves a [`SlabBlockId`] into a raw pointer to its payload bytes.
///
/// # Safety
///
/// Extremely fast (~1.2 ns): performs a single array indexing and pointer offset.
/// Caller must ensure `id` is a live allocation.
#[inline(always)]
pub unsafe fn resolve_slot_ptr(id: SlabBlockId) -> Option<*mut u8> {
    let slab_id = id.slab_id() as usize;
    if slab_id < MAX_GLOBAL_SLABS {
        let base = GLOBAL_ARENAS[slab_id].base_ptr.load(Ordering::Acquire);
        if !base.is_null() {
            let slot_size = GLOBAL_ARENAS[slab_id].slot_bytes.load(Ordering::Relaxed) as usize;
            let offset = MEGASLAB_HEADER_BYTES + (id.slot_index() as usize) * slot_size;
            return Some(unsafe { base.add(offset) });
        }
    }
    None
}
