//! Global slab registry for cross-worker payload access and slot retirement.

use std::{
    ptr::null_mut,
    sync::{
        Arc,
        atomic::{AtomicPtr, AtomicU32, AtomicU64, Ordering},
    },
};

use crossbeam_channel::Sender;
use parking_lot::RwLock;

use crate::{arena::SlabBlockId, slab::MEGASLAB_HEADER_BYTES};

/// Maximum supported slab identifiers in the process.
pub const MAX_GLOBAL_SLABS: usize = 16_384;

static NEXT_GLOBAL_SLAB_ID: AtomicU32 = AtomicU32::new(1);

pub(crate) struct RetiredSlot {
    pub id: SlabBlockId,
    pub generation: u64,
}

struct ArenaOwner {
    core: u16,
    generations: Arc<[AtomicU64]>,
    inbox: Option<Sender<RetiredSlot>>,
}

impl ArenaOwner {
    fn live_generation(&self, index: usize) -> Option<u64> {
        let generation = self.generations.get(index)?.load(Ordering::Relaxed);
        (generation & 1 != 0).then_some(generation)
    }
}

struct ArenaEntry {
    base_ptr: AtomicPtr<u8>,
    slot_bytes: AtomicU32,
    owner: RwLock<Option<ArenaOwner>>,
}

impl ArenaEntry {
    const fn new() -> Self {
        Self {
            base_ptr: AtomicPtr::new(null_mut()),
            slot_bytes: AtomicU32::new(0),
            owner: RwLock::new(None),
        }
    }
}

static GLOBAL_ARENAS: [ArenaEntry; MAX_GLOBAL_SLABS] =
    [const { ArenaEntry::new() }; MAX_GLOBAL_SLABS];

/// Atomically allocates a globally unique slab identifier.
pub fn allocate_global_slab_id() -> u16 {
    let id = NEXT_GLOBAL_SLAB_ID.fetch_add(1, Ordering::Relaxed);
    assert!(
        (id as usize) < MAX_GLOBAL_SLABS,
        "Exceeded maximum global slab arenas ({MAX_GLOBAL_SLABS})"
    );
    id as u16
}

pub(crate) fn register_arena(
    slab_id: u16,
    base_ptr: *mut u8,
    slot_bytes: usize,
    core: u16,
    generations: Arc<[AtomicU64]>,
) {
    if let Some(arena) = GLOBAL_ARENAS.get(slab_id as usize) {
        let mut owner = arena.owner.write();
        assert!(owner.is_none(), "slab {slab_id} already registered");
        *owner = Some(ArenaOwner {
            core,
            generations,
            inbox: None,
        });
        arena.slot_bytes.store(slot_bytes as u32, Ordering::Relaxed);
        arena.base_ptr.store(base_ptr, Ordering::Release);
    }
}

pub(crate) fn register_dealloc_sender(slab_id: u16, sender: Sender<RetiredSlot>) {
    let mut owner = GLOBAL_ARENAS[slab_id as usize].owner.write();
    owner.as_mut().expect("registered arena").inbox = Some(sender);
}

pub(crate) fn unregister_arena(slab_id: u16) {
    if let Some(arena) = GLOBAL_ARENAS.get(slab_id as usize) {
        let mut owner = arena.owner.write();
        arena.base_ptr.store(null_mut(), Ordering::Release);
        *owner = None;
    }
}

pub(crate) fn send_to_dealloc_inbox(id: SlabBlockId) -> Option<()> {
    let owner = GLOBAL_ARENAS.get(id.slab_id() as usize)?.owner.read();
    let owner = owner.as_ref()?;
    let generation = owner.live_generation(id.slot_index() as usize)?;
    owner
        .inbox
        .as_ref()?
        .send(RetiredSlot { id, generation })
        .ok()
}

/// Returns the registered arena's owning core.
pub fn resolve_owning_core(id: SlabBlockId) -> Option<u16> {
    let owner = GLOBAL_ARENAS.get(id.slab_id() as usize)?.owner.read();
    let owner = owner.as_ref()?;
    owner.generations.get(id.slot_index() as usize)?;
    Some(owner.core)
}

/// Returns the current generation of a live, in-bounds slot.
pub fn get_slot_generation(id: SlabBlockId) -> Option<u64> {
    let owner = GLOBAL_ARENAS.get(id.slab_id() as usize)?.owner.read();
    owner.as_ref()?.live_generation(id.slot_index() as usize)
}

/// Resolves a live slot to its payload pointer.
///
/// # Safety
///
/// The caller must keep the allocation alive and prevent concurrent conflicting
/// access for the duration of any access through the returned pointer.
#[inline(always)]
pub unsafe fn resolve_slot_ptr(id: SlabBlockId) -> Option<*mut u8> {
    let arena = GLOBAL_ARENAS.get(id.slab_id() as usize)?;
    let base = arena.base_ptr.load(Ordering::Acquire);
    if base.is_null() {
        return None;
    }
    let slot_size = arena.slot_bytes.load(Ordering::Relaxed) as usize;
    let offset = MEGASLAB_HEADER_BYTES + (id.slot_index() as usize) * slot_size;
    Some(unsafe { base.add(offset) })
}
