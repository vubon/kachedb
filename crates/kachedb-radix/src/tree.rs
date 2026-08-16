//! `kachedb-radix` — Radix prefix tree for LLM token sequences.
//!
//! # Architecture (from RFC 2)
//!
//! ```text
//!                         [ Root ]
//!                            |
//!          Edge: [t0..t15] (System Prompt)
//!                            |
//!                    [ Node A: Slab #102 ]
//!                    /                   \
//!   Edge: [t16..t31] (User A)     Edge: [t16..t31] (User B)
//!           |                               |
//!   [ Node B: Slab #103 ]         [ Node C: Slab #104 ]
//! ```
//!
//! Nodes sharing a common prefix (e.g., a system prompt) are **never
//! duplicated**: all conversation threads that share the same system prompt
//! converge at the same `Node A`, saving the GPU from re-computing those
//! attention layers.
//!
//! # Lookup Lifecycle
//!
//! 1. GPU inference worker submits `tokens: &[u32]`.
//! 2. `RadixTree::lookup` walks the tree, collecting matched `SlabBlockId`s.
//! 3. Matched blocks are pinned (`ref_count++`) and returned to the caller.
//! 4. Unmatched tail tokens trigger new `SlabPool::allocate()` calls.
//! 5. New nodes are inserted via `RadixTree::insert`.
//! 6. When GPU finishes, caller calls `RadixTree::unpin` to decrement counts.
//! 7. Eviction is triggered by `RadixTree::evict_lru` when memory pressure hits.

use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use kachedb_core::SlabBlockId;

use crate::{
    error::RadixError,
    node::{RadixNode, TokenBlock, TOKENS_PER_BLOCK},
};

// ─── LookupResult ─────────────────────────────────────────────────────────────

/// Result of a prefix lookup in the radix tree.
#[derive(Debug)]
pub struct LookupResult {
    /// Number of token IDs successfully matched from the start of the query.
    pub matched_tokens: usize,
    /// Ordered list of slab block IDs for each matched block along the path.
    /// Blocks are pinned (ref_count incremented) and must be unpinned by the caller.
    pub slab_block_ids: Vec<SlabBlockId>,
}

impl LookupResult {
    /// Returns `true` if the entire query was matched (complete cache hit).
    #[inline]
    pub fn is_full_hit(&self, query_len: usize) -> bool {
        self.matched_tokens == query_len
    }

    /// Number of unmatched tokens that require GPU prefill computation.
    #[inline]
    pub fn tail_len(&self, query_len: usize) -> usize {
        query_len.saturating_sub(self.matched_tokens)
    }
}

// ─── RadixTree ────────────────────────────────────────────────────────────────

/// Token prefix tree for KacheDB LLM KV-cache management.
///
/// Manages a hierarchical index over `&[u32]` token sequences. Matching
/// against this tree determines which attention layers can be skipped by
/// reusing cached KV tensors.
///
/// # Thread Safety
///
/// `RadixTree` is **single-threaded per core** (consistent with the
/// shared-nothing design). Cross-core prefix matches use the replicated
/// radix index model described in RFC 1.
pub struct RadixTree {
    /// Virtual root node (empty token block, no slab pointer).
    root: Box<RadixNode>,
    /// Total number of nodes currently in the tree (including root).
    node_count: usize,
}

impl RadixTree {
    /// Creates a new empty `RadixTree` with a virtual root node.
    pub fn new() -> Self {
        Self {
            root: RadixNode::root(),
            node_count: 1,
        }
    }

    /// Returns the current number of nodes in the tree.
    #[inline]
    pub fn node_count(&self) -> usize {
        self.node_count
    }

    // ── Lookup ───────────────────────────────────────────────────────────────

    /// Finds the longest matching prefix of `tokens` in the tree.
    ///
    /// Walks the tree block by block (16 tokens per hop), collecting slab block
    /// IDs for each matched node. Matched nodes are **pinned** (`ref_count++`)
    /// and their timestamps updated.
    ///
    /// The caller **must** call [`unpin`](Self::unpin) with the returned IDs
    /// when the GPU worker completes its use of the cached tensors.
    ///
    /// # Errors
    ///
    /// Returns [`RadixError::EmptySequence`] if `tokens` is empty.
    pub fn lookup(&self, tokens: &[u32]) -> Result<LookupResult, RadixError> {
        if tokens.is_empty() {
            return Err(RadixError::EmptySequence);
        }

        let now_ns = now_ns();
        let mut matched_tokens = 0usize;
        let mut slab_block_ids = Vec::new();
        let mut current = &*self.root;

        while matched_tokens < tokens.len() {
            let remaining = &tokens[matched_tokens..];
            let chunk: TokenBlock = remaining
                .iter()
                .take(TOKENS_PER_BLOCK)
                .copied()
                .collect();

            match find_child(current, chunk[0]) {
                None => break, // no matching child — prefix ends here
                Some(child) => {
                    // Verify the full block matches.
                    if !child.tokens.iter().zip(chunk.iter()).all(|(a, b)| a == b)
                        || child.tokens.len() != chunk.len()
                    {
                        break; // partial block mismatch
                    }

                    // Pin and record the matched block.
                    child.pin();
                    child.touch(now_ns);
                    if let Some(id) = child.slab_block_id {
                        slab_block_ids.push(id);
                    }
                    matched_tokens += child.tokens.len();
                    current = child;
                }
            }
        }

        Ok(LookupResult { matched_tokens, slab_block_ids })
    }

    // ── Insert ───────────────────────────────────────────────────────────────

    /// Inserts a full token sequence into the tree, creating new nodes for
    /// unmatched tail blocks.
    ///
    /// Each new node is associated with one `SlabBlockId` provided in `slab_ids`.
    /// `slab_ids[i]` corresponds to block `i` of the token sequence.
    ///
    /// # Errors
    ///
    /// Returns [`RadixError::EmptySequence`] if `tokens` is empty.
    ///
    /// # Panics
    ///
    /// Panics if `slab_ids` does not cover all new (unmatched) blocks.
    pub fn insert(
        &mut self,
        tokens: &[u32],
        slab_ids: &[SlabBlockId],
    ) -> Result<usize, RadixError> {
        if tokens.is_empty() {
            return Err(RadixError::EmptySequence);
        }

        let now_ns = now_ns();
        let mut pos = 0usize; // token cursor
        let mut slab_cursor = 0usize; // index into slab_ids for new nodes
        let mut current: *mut RadixNode = &mut *self.root;
        let mut nodes_inserted = 0usize;

        while pos < tokens.len() {
            let remaining = &tokens[pos..];
            let chunk: TokenBlock = remaining
                .iter()
                .take(TOKENS_PER_BLOCK)
                .copied()
                .collect();

            // SAFETY: `current` always points into the tree owned by `self`.
            let node = unsafe { &mut *current };

            match find_child_mut(node, chunk[0]) {
                Some(child_idx) => {
                    // Existing child — verify full match and advance.
                    let child = &mut node.children[child_idx];
                    if child.tokens == chunk {
                        child.touch(now_ns);
                        pos += child.tokens.len();
                        current = &mut **child;
                    } else {
                        // Partial match: existing node diverges mid-block.
                        // For Phase 1 simplicity, we stop here.
                        // Full edge-splitting will be added in Phase 2.
                        break;
                    }
                }
                None => {
                    // No matching child — create a new node.
                    let slab_id = slab_ids.get(slab_cursor).copied();
                    slab_cursor += 1;

                    let new_node = RadixNode::new(chunk.clone(), slab_id);
                    new_node.touch(now_ns);

                    node.child_count.fetch_add(1, Ordering::Relaxed);
                    node.children.push(new_node);

                    pos += chunk.len();
                    nodes_inserted += 1;
                    self.node_count += 1;

                    // Advance current to the newly inserted child.
                    current = &mut **node.children.last_mut().unwrap();
                }
            }
        }

        Ok(nodes_inserted)
    }

    // ── Unpin ────────────────────────────────────────────────────────────────

    /// Decrements `ref_count` for all nodes whose slab block IDs are listed.
    ///
    /// Called by the GPU inference worker after it is done with the cached
    /// tensors. Once `ref_count` reaches 0 (and `child_count == 0`), the node
    /// becomes eligible for [`evict_lru`](Self::evict_lru).
    pub fn unpin(&self, slab_ids: &[SlabBlockId]) {
        Self::unpin_recursive(&self.root, slab_ids);
    }

    fn unpin_recursive(node: &RadixNode, targets: &[SlabBlockId]) {
        if let Some(id) = node.slab_block_id {
            if targets.contains(&id) {
                node.unpin();
            }
        }
        for child in &node.children {
            Self::unpin_recursive(child, targets);
        }
    }

    // ── Eviction ─────────────────────────────────────────────────────────────

    /// Evicts the single least-recently-used leaf node eligible for removal.
    ///
    /// **Algorithm (RFC 2 §2 — Hierarchical Leaf-First Eviction):**
    /// 1. Collect all nodes where `is_evictable() == true`.
    /// 2. Select the one with the smallest `last_accessed_ns` (oldest access).
    /// 3. Remove it from its parent's `children` list.
    /// 4. Decrement parent's `child_count`; if parent becomes evictable, it is
    ///    now a candidate for the next eviction round (bottom-up cascade).
    ///
    /// Returns the `SlabBlockId` of the evicted node so the caller can release
    /// the slab slot back to the `SlabPool`.
    ///
    /// # Errors
    ///
    /// Returns [`RadixError::NoEvictableLeaf`] if all nodes are pinned or
    /// have active children.
    pub fn evict_lru(&mut self) -> Result<Option<SlabBlockId>, RadixError> {
        // Collect (last_accessed_ns, path to evictable leaf).
        // We find the parent of the target so we can remove it.
        let result = find_lru_leaf_parent(&mut self.root);

        match result {
            None => Err(RadixError::NoEvictableLeaf),
            Some((parent_ptr, child_idx, slab_id)) => {
                // SAFETY: `parent_ptr` is valid for the lifetime of `self`.
                let parent = unsafe { &mut *parent_ptr };
                parent.children.swap_remove(child_idx);
                parent.child_count.fetch_sub(1, Ordering::Relaxed);
                self.node_count -= 1;

                log::debug!(
                    "RadixTree: evicted leaf node, slab_id={slab_id:?}, \
                     tree_nodes={}",
                    self.node_count
                );

                Ok(slab_id)
            }
        }
    }

    /// Returns the number of evictable leaf nodes currently in the tree.
    pub fn evictable_count(&self) -> usize {
        count_evictable(&self.root)
    }
}

impl Default for RadixTree {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Private Helpers ──────────────────────────────────────────────────────────

/// Finds an immutable child whose first token matches `first_token`.
fn find_child(node: &RadixNode, first_token: u32) -> Option<&RadixNode> {
    node.children
        .iter()
        .find(|c| c.first_token() == Some(first_token))
        .map(|c| c.as_ref())
}

/// Finds the index of a mutable child whose first token matches `first_token`.
fn find_child_mut(node: &RadixNode, first_token: u32) -> Option<usize> {
    node.children
        .iter()
        .position(|c| c.first_token() == Some(first_token))
}

/// Recursively counts evictable nodes.
fn count_evictable(node: &RadixNode) -> usize {
    let self_evictable = if node.slab_block_id.is_some() && node.is_evictable() {
        1
    } else {
        0
    };
    self_evictable + node.children.iter().map(|c| count_evictable(c)).sum::<usize>()
}

/// Returns `(parent_ptr, child_index, slab_id)` for the LRU evictable leaf.
fn find_lru_leaf_parent(
    node: &mut RadixNode,
) -> Option<(*mut RadixNode, usize, Option<SlabBlockId>)> {
    let mut best: Option<(u64, *mut RadixNode, usize, Option<SlabBlockId>)> = None;

    let node_ptr: *mut RadixNode = node;
    for (idx, child) in node.children.iter_mut().enumerate() {
        if child.is_evictable() && child.slab_block_id.is_some() {
            let ts = child.last_accessed_ns.load(std::sync::atomic::Ordering::Relaxed);
            if best.as_ref().map_or(true, |(best_ts, ..)| ts < *best_ts) {
                best = Some((ts, node_ptr, idx, child.slab_block_id));
            }
        }
        // Recurse into non-evictable children (they may have evictable descendants).
        if let Some(candidate) = find_lru_leaf_parent(child) {
            let ts = unsafe { &*candidate.0 }.children[candidate.1]
                .last_accessed_ns
                .load(std::sync::atomic::Ordering::Relaxed);
            if best.as_ref().map_or(true, |(best_ts, ..)| ts < *best_ts) {
                best = Some((ts, candidate.0, candidate.1, candidate.2));
            }
        }
    }

    best.map(|(_, parent, idx, slab)| (parent, idx, slab))
}

/// Returns a nanosecond timestamp from the system clock.
#[inline]
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(start: u32, count: u32) -> Vec<u32> {
        (start..start + count).collect()
    }

    fn slab(n: u32) -> SlabBlockId {
        SlabBlockId(n)
    }

    #[test]
    fn lookup_empty_sequence_errors() {
        let tree = RadixTree::new();
        assert!(matches!(tree.lookup(&[]), Err(RadixError::EmptySequence)));
    }

    #[test]
    fn insert_and_lookup_single_block() {
        let mut tree = RadixTree::new();
        let tokens = ids(0, 16);
        tree.insert(&tokens, &[slab(1)]).unwrap();

        let result = tree.lookup(&tokens).unwrap();
        assert_eq!(result.matched_tokens, 16);
        assert_eq!(result.slab_block_ids, vec![slab(1)]);
    }

    #[test]
    fn lookup_miss_returns_zero() {
        let tree = RadixTree::new();
        let result = tree.lookup(&ids(0, 16)).unwrap();
        assert_eq!(result.matched_tokens, 0);
        assert!(result.slab_block_ids.is_empty());
    }

    #[test]
    fn shared_prefix_single_branch() {
        let mut tree = RadixTree::new();
        // Shared prefix: first 16 tokens identical.
        let prefix = ids(0, 16);
        let tail_a: Vec<u32> = (16..32).collect();
        let tail_b: Vec<u32> = (100..116).collect();

        let seq_a: Vec<u32> = prefix.iter().chain(tail_a.iter()).copied().collect();
        let seq_b: Vec<u32> = prefix.iter().chain(tail_b.iter()).copied().collect();

        tree.insert(&seq_a, &[slab(10), slab(11)]).unwrap();
        tree.insert(&seq_b, &[slab(10), slab(12)]).unwrap();

        // Both sequences share the same slab(10) for the prefix block.
        assert_eq!(tree.node_count(), 4); // root + prefix_node + two tail nodes

        let res_a = tree.lookup(&seq_a).unwrap();
        assert_eq!(res_a.matched_tokens, 32);

        let res_b = tree.lookup(&seq_b).unwrap();
        assert_eq!(res_b.matched_tokens, 32);
    }

    #[test]
    fn partial_lookup_returns_matched_prefix() {
        let mut tree = RadixTree::new();
        let block0 = ids(0, 16);
        let block1 = ids(16, 16);
        let all: Vec<u32> = block0.iter().chain(block1.iter()).copied().collect();

        tree.insert(&all, &[slab(1), slab(2)]).unwrap();

        // Query only the first block — should get a partial hit.
        let result = tree.lookup(&block0).unwrap();
        assert_eq!(result.matched_tokens, 16);
        assert_eq!(result.slab_block_ids, vec![slab(1)]);
    }

    #[test]
    fn full_hit_detection() {
        let mut tree = RadixTree::new();
        let tokens = ids(0, 16);
        tree.insert(&tokens, &[slab(5)]).unwrap();
        let result = tree.lookup(&tokens).unwrap();
        assert!(result.is_full_hit(16));
        assert_eq!(result.tail_len(16), 0);
    }

    #[test]
    fn evict_lru_removes_leaf() {
        let mut tree = RadixTree::new();
        let tokens = ids(0, 16);
        tree.insert(&tokens, &[slab(99)]).unwrap();

        assert_eq!(tree.evictable_count(), 1);

        // Lookup to set a timestamp (required for LRU).
        let res = tree.lookup(&tokens).unwrap();
        // Unpin so the node becomes evictable again.
        tree.unpin(&res.slab_block_ids);

        let evicted = tree.evict_lru().unwrap();
        assert_eq!(evicted, Some(slab(99)));
        assert_eq!(tree.node_count(), 1); // only root remains
    }

    #[test]
    fn no_evictable_leaf_when_pinned() {
        let mut tree = RadixTree::new();
        let tokens = ids(0, 16);
        tree.insert(&tokens, &[slab(1)]).unwrap();
        // Lookup pins the node.
        let _res = tree.lookup(&tokens).unwrap();
        // Not unpinned — should be ineligible.
        assert_eq!(tree.evictable_count(), 0);
        let err = tree.evict_lru();
        assert!(matches!(err, Err(RadixError::NoEvictableLeaf)));
    }
}
