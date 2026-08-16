//! `kachedb-proto-tensor` — Tensor block descriptors and PagedAttention layout.
//!
//! Defines the in-memory binary format used by KacheDB to describe and expose
//! LLM KV-cache tensor blocks to Python inference engines without serialisation.
//!
//! # Zero-Copy Integration
//!
//! Each slab block starts with a [`TensorBlockDescriptor`] header (64 bytes),
//! followed immediately by the raw key+value tensor payload. Python workers:
//!
//! 1. Attach to `/dev/shm/kachedb_{core_id}` via `mmap`.
//! 2. Read the 64-byte descriptor to recover shape / dtype.
//! 3. Call `torch.frombuffer(raw_bytes, dtype=...)` to get a zero-copy tensor.
//! 4. Optionally register with CUDA via `cudaHostRegister()` for async PCIe DMA.

pub mod descriptor;

pub use descriptor::{TENSOR_DESCRIPTOR_MAGIC, TensorBlockDescriptor, TensorDType};
