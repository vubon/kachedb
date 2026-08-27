//! `kachedb-vector` — Error types.

use thiserror::Error;

/// Vector search and index operation errors.
#[derive(Debug, Error, PartialEq)]
pub enum VectorError {
    #[error("Vector dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },

    #[error(
        "Invalid raw vector byte length: expected {expected_bytes} bytes for {dim} f32 elements, got {actual_bytes} bytes"
    )]
    InvalidVectorBytes {
        dim: usize,
        expected_bytes: usize,
        actual_bytes: usize,
    },

    #[error("Vector index '{0}' not found")]
    IndexNotFound(String),

    #[error("Invalid similarity threshold {0}: must be between -1.0 and 1.0")]
    InvalidThreshold(f32),

    #[error("Invalid top-k {0}: must be greater than 0")]
    InvalidTopK(usize),

    #[error("Vector key empty")]
    EmptyKey,
}
