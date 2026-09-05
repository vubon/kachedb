//! `kachedb-vector` — Hardware-accelerated SIMD vector search and in-memory semantic cache engine.
//!
//! Provides:
//! - **SIMD Kernels**: ARM NEON (`aarch64`) and AVX2/FMA (`x86_64`) vector dot product, $L_2$ norm, and cosine similarity.
//! - **VectorIndex**: Thread-safe, contiguous vector slab storage with TTL expiration, $L_2$ pre-normalization, and top-k search.
//! - **VectorIndexRegistry**: Multi-tenant named vector index registry.

pub mod error;
pub mod hnsw;
pub mod index;
pub mod quantizer;
pub mod simd;

pub use error::VectorError;
pub use hnsw::{HnswIndex, VectorMetric};
pub use index::{
    VectorEntry, VectorIndex, VectorIndexRegistry, VectorIndexStats, VectorSearchResult,
};
pub use quantizer::{QuantizationMode, Sq8Quantizer};
pub use simd::{
    cosine_similarity, cosine_similarity_normalized, dot_product, dot_product_scalar,
    l2_distance_squared, l2_norm, l2_normalize,
};

#[cfg(target_arch = "aarch64")]
pub use simd::dot_product_neon;

#[cfg(target_arch = "x86_64")]
pub use simd::dot_product_avx2;
