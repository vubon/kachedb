//! `kachedb-radix` — Error types for the radix prefix tree.

use thiserror::Error;

/// Errors returned by the `kachedb-radix` subsystem.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum RadixError {
    /// A token sequence of length zero was provided, which is invalid.
    #[error("token sequence must be non-empty")]
    EmptySequence,

    /// Attempted to decrement the reference count of a node that is already zero.
    #[error("reference count underflow on node at token offset {offset}")]
    RefCountUnderflow { offset: usize },

    /// No evictable leaf nodes exist (all nodes are either pinned or have children).
    #[error("no evictable leaf nodes available")]
    NoEvictableLeaf,
}
