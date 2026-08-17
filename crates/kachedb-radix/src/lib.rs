//! `kachedb-radix` — `&[u32]` token prefix tree for LLM KV-cache offloading.
//!
//! This crate implements the **Radix Prefix Tree** index from the KacheDB
//! architecture blueprint:
//!
//! > _"Stores prompt token sequences hierarchically. Incoming inference requests
//! > traverse the prefix tree to find the longest matching token chain, returning
//! > pre-allocated block addresses immediately."_
//!
//! # Design Highlights (RFC 2)
//!
//! - **Native `u32` tokens**: supports all modern LLM vocabularies (128K–256K).
//! - **Block-chunked edges**: 16 tokens per node hop → O(L/16) traversal vs O(L).
//! - **Hierarchical bottom-up LRU eviction**: tail blocks evicted before roots,
//!   preserving shared system prompts and few-shot prefixes.
//! - **Reference counting**: GPU inference workers pin active nodes, preventing
//!   eviction of blocks under active use.
//!
//! # Quick Start
//!
//! ```rust
//! use kachedb_radix::RadixTree;
//! use kachedb_core::SlabBlockId;
//!
//! let mut tree = RadixTree::new();
//!
//! // Insert a 32-token sequence (2 blocks of 16) backed by two slab slots.
//! let tokens: Vec<u32> = (0u32..32).collect();
//! tree.insert(&tokens, &[SlabBlockId(1), SlabBlockId(2)]).unwrap();
//!
//! // Lookup — returns matched block count and pinned slab IDs.
//! let result = tree.lookup(&tokens).unwrap();
//! assert_eq!(result.matched_tokens, 32);
//!
//! // Unpin when the GPU worker is done with the cached tensors.
//! tree.unpin(&result.slab_block_ids);
//! ```

pub mod epoch;
pub mod error;
pub mod node;
pub mod tree;

pub use epoch::EpochTree;
pub use error::RadixError;
pub use node::{RadixNode, TokenBlock, TOKENS_PER_BLOCK};
pub use tree::{LookupResult, RadixTree};
