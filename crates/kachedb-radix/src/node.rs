//! `kachedb-radix` — Radix tree node definition and token block primitives.
//!
//! # Token Block Chunking
//!
//! LLM token sequences are grouped into fixed-size **blocks** of
//! [`TOKENS_PER_BLOCK`] (`u32`) tokens, mirroring the PagedAttention block
//! layout used by vLLM and SGLang inference engines.
//!
//! ```text
//! Input tokens: [t0, t1, …, t_L]
//!
//! Chunked into blocks of 16:
//! Block 0: [t0  … t15 ]  → RadixNode A  (slab_block_id = Some(#102))
//! Block 1: [t16 … t31 ]  → RadixNode B  (slab_block_id = Some(#103))
//! Block 2: [t32 … t47 ]  → RadixNode C  (slab_block_id = Some(#104))
//! ```
//!
//! This reduces lookup complexity from O(L) individual token hops to
//! O(L / B) block hops, where B is the block size (16).
//!
//! # Why `u32`?
//!
//! Modern LLM vocabularies exceed the `u16` limit of 65,536:
//! - Llama 3/4: 128,256 tokens
//! - Gemma 2/3: 256,000 tokens
//! - Qwen 2.5 / DeepSeek V3: 152,064 tokens
//!
//! `kachedb-radix` uses `u32` natively to avoid truncation and runtime casting.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use smallvec::SmallVec;

use kachedb_core::SlabBlockId;

/// Number of `u32` token IDs stored in one radix tree edge (block size).
/// Matches the PagedAttention default block size used by vLLM.
pub const TOKENS_PER_BLOCK: usize = 16;

/// Compact inline token storage: stores up to 16 `u32` tokens without heap
/// allocation. Spills to the heap for larger sequences (rare edge case).
pub type TokenBlock = SmallVec<[u32; TOKENS_PER_BLOCK]>;

// ─── RadixNode ────────────────────────────────────────────────────────────────

/// A single node in the KacheDB LLM token prefix tree.
///
/// Each node represents one **block** of token IDs (up to 16 `u32` values)
/// along the path from the root to a leaf. The node optionally holds a pointer
/// into the `kachedb-core` `SlabPool` where the corresponding KV-cache tensor
/// block is stored.
///
/// # Ownership & Eviction Safety
///
/// - `ref_count > 0`: The node is **pinned** by one or more active GPU
///   inference workers. It must not be evicted.
/// - `ref_count == 0` && `child_count == 0`: The node is an evictable **leaf**.
///   It is placed into the eviction LRU queue by [`crate::tree::RadixTree`].
/// - `child_count > 0`: The node is a shared **branch** (system prompt,
///   few-shot prefix, etc.). Eviction is deferred until all children are
///   pruned first (bottom-up cascade).
pub struct RadixNode {
    /// Token slice forming the compressed edge label leading to this node.
    /// Holds exactly `min(TOKENS_PER_BLOCK, remaining_tokens)` elements.
    pub tokens: TokenBlock,

    /// Opaque handle into the `kachedb-core` `SlabPool` where this node's
    /// KV-cache tensor payload lives. `None` for the virtual root node.
    pub slab_block_id: Option<SlabBlockId>,

    /// Reference count of active GPU workers currently reading this block.
    /// Atomic to allow concurrent pin/unpin from the inference engine thread.
    pub ref_count: AtomicU32,

    /// Monotonic nanosecond timestamp of the last access.
    /// Updated on every lookup hit via [`RadixNode::touch`].
    pub last_accessed_ns: AtomicU64,

    /// Number of direct child branches. Maintained by the tree on
    /// insert / evict. A node with `child_count == 0` is a true leaf.
    pub child_count: AtomicU32,

    /// Child edges, each keyed by the **first token** of the child's
    /// `TokenBlock` for O(branching_factor) linear search.
    /// Typical branching factor is very small (2–5) so linear scan beats
    /// hash overhead at this scale.
    pub children: Vec<Box<RadixNode>>,
}

impl Clone for RadixNode {
    /// Deep-clones a `RadixNode` for copy-on-write RCU snapshot creation.
    /// Atomic counters are reset on clone — the snapshot is an independent
    /// structural copy, not a live reference mirror.
    fn clone(&self) -> Self {
        Self {
            tokens: self.tokens.clone(),
            slab_block_id: self.slab_block_id,
            ref_count: AtomicU32::new(self.ref_count.load(Ordering::Acquire)),
            last_accessed_ns: AtomicU64::new(self.last_accessed_ns.load(Ordering::Relaxed)),
            child_count: AtomicU32::new(self.child_count.load(Ordering::Acquire)),
            children: self.children.clone(),
        }
    }
}

impl RadixNode {
    /// Constructs a new internal node with the given token block and slab pointer.
    pub fn new(tokens: TokenBlock, slab_block_id: Option<SlabBlockId>) -> Box<Self> {
        Box::new(Self {
            tokens,
            slab_block_id,
            ref_count: AtomicU32::new(0),
            last_accessed_ns: AtomicU64::new(0),
            child_count: AtomicU32::new(0),
            children: Vec::new(),
        })
    }

    /// Constructs the virtual root node (empty token block, no slab pointer).
    pub fn root() -> Box<Self> {
        Self::new(TokenBlock::new(), None)
    }

    /// Returns `true` if this node is eligible for eviction:
    /// - Not referenced by any active worker (`ref_count == 0`).
    /// - Has no active children (`child_count == 0`).
    #[inline(always)]
    pub fn is_evictable(&self) -> bool {
        self.ref_count.load(Ordering::Acquire) == 0 && self.child_count.load(Ordering::Acquire) == 0
    }

    /// Updates the last-access timestamp for LRU tie-breaking.
    ///
    /// Uses `Relaxed` ordering — staleness of a few nanoseconds is acceptable
    /// for LRU approximation and avoids unnecessary memory barriers.
    #[inline(always)]
    pub fn touch(&self, timestamp_ns: u64) {
        self.last_accessed_ns.store(timestamp_ns, Ordering::Relaxed);
    }

    /// Pins this node by incrementing `ref_count`.
    ///
    /// Must be matched with a corresponding [`RadixNode::unpin`] call
    /// when the GPU worker finishes reading the block.
    #[inline(always)]
    pub fn pin(&self) {
        self.ref_count.fetch_add(1, Ordering::AcqRel);
    }

    /// Unpins this node by decrementing `ref_count`.
    ///
    /// Returns the new `ref_count` value (0 means eviction is now eligible).
    #[inline(always)]
    pub fn unpin(&self) -> u32 {
        self.ref_count
            .fetch_sub(1, Ordering::AcqRel)
            .saturating_sub(1)
    }

    /// Returns the first token of this node's edge label.
    /// Used as the lookup key when searching `parent.children`.
    #[inline(always)]
    pub fn first_token(&self) -> Option<u32> {
        self.tokens.first().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_node_has_empty_tokens() {
        let root = RadixNode::root();
        assert!(root.tokens.is_empty());
        assert!(root.slab_block_id.is_none());
    }

    #[test]
    fn new_node_is_evictable() {
        let tokens: TokenBlock = (0u32..16).collect();
        let node = RadixNode::new(tokens, Some(SlabBlockId(42)));
        assert!(node.is_evictable());
    }

    #[test]
    fn pin_prevents_eviction() {
        let node = RadixNode::new(SmallVec::new(), None);
        node.pin();
        assert!(!node.is_evictable());
        node.unpin();
        assert!(node.is_evictable());
    }

    #[test]
    fn touch_updates_timestamp() {
        let node = RadixNode::new(SmallVec::new(), None);
        node.touch(12345);
        assert_eq!(node.last_accessed_ns.load(Ordering::Relaxed), 12345);
    }
}
