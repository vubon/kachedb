//! `kachedb-proto-tensor` — `TensorDType`, `TensorBlockDescriptor`, and payload sizing.
//!
//! The 64-byte `TensorBlockDescriptor` header is placed at the base of every
//! KV-cache slab block. It encodes enough metadata for Python workers to wrap
//! the payload as a `torch.Tensor` via `torch.frombuffer()` with zero copies.
//!
//! # Memory Layout
//!
//! ```text
//! ┌──────────────────────────────────── 64 Bytes (1 Cache Line) ─────────────────────────────┐
//! │ magic(4) │ layer_id(2) │ num_layers(2) │ block_size(2) │ num_heads(2) │ head_dim(2) │ .. │
//! │ dtype(1) │ _pad(7)     │ seq_prefix_hash(8)            │ payload_bytes(4) │ _pad(28)  │   │
//! └───────────────────────────────────────────────────────────────────────────────────────────┘
//! ┌──────────────────────────────── RAW TENSOR PAYLOAD ──────────────────────────────────────┐
//! │  Key   tensor: [num_layers, num_heads, block_size, head_dim] × dtype.element_size_bytes  │
//! │  Value tensor: [num_layers, num_heads, block_size, head_dim] × dtype.element_size_bytes  │
//! └───────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use kachedb_core::CACHE_LINE_BYTES;

// ─── TensorDType ──────────────────────────────────────────────────────────────

/// Supported numeric precision types for LLM KV-cache tensors.
///
/// Matches the dtypes supported by vLLM PagedAttention and PyTorch.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TensorDType {
    /// 32-bit IEEE 754 float (4 bytes per element).
    FP32 = 0,
    /// 16-bit IEEE 754 half-precision float (2 bytes per element).
    FP16 = 1,
    /// 16-bit brain float (2 bytes per element). Default for LLaMA/Gemma.
    BF16 = 2,
    /// 8-bit float, E4M3 encoding (1 byte per element). FP8 quantisation.
    FP8E4M3 = 3,
    /// 8-bit float, E5M2 encoding (1 byte per element).
    FP8E5M2 = 4,
    /// 8-bit signed integer (1 byte per element). INT8 quantisation.
    INT8 = 5,
    /// 4-bit integer, packed 2 per byte (0.5 bytes per element).
    /// `element_size_bytes()` returns 1 (one byte stores 2 INT4 values).
    INT4 = 6,
}

impl TensorDType {
    /// Returns the storage size in bytes for one element of this dtype.
    ///
    /// For `INT4`, returns 1 because two values are packed per byte.
    #[inline(always)]
    pub const fn element_size_bytes(self) -> usize {
        match self {
            Self::FP32 => 4,
            Self::FP16 | Self::BF16 => 2,
            Self::FP8E4M3 | Self::FP8E5M2 | Self::INT8 => 1,
            Self::INT4 => 1, // 2 elements packed per byte
        }
    }
}

// ─── TensorBlockDescriptor ────────────────────────────────────────────────────

/// 64-byte cache-line aligned header at the start of every KV-cache slab block.
///
/// Encodes the shape, dtype, and sequence identity of the tensor payload
/// that immediately follows this descriptor in memory. Python workers can
/// reconstruct a `torch.Tensor` from any KacheDB slab block without
/// serialisation or data copying.
///
/// # Magic
///
/// `magic` is always `0x4B41_4348` (`KACH` in little-endian ASCII).
/// Used for integrity validation.
///
/// # Payload Computation
///
/// The raw payload size can be computed with [`TensorBlockDescriptor::compute_payload_size`].
/// KV cache stores **two** tensors (Key + Value) in the same block.
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct TensorBlockDescriptor {
    /// Integrity sentinel: `0x4B41_4348` (`"KACH"`).
    pub magic: u32,
    /// Transformer layer this block belongs to, or `0xFFFF` for all-layer packed.
    pub layer_id: u16,
    /// Total number of transformer layers packed into this block.
    pub num_layers: u16,
    /// Number of tokens per PagedAttention block (e.g., 16 or 32).
    pub block_size: u16,
    /// Number of KV attention heads (GQA / MHA).
    pub num_heads: u16,
    /// Per-head feature dimension (e.g., 64, 128).
    pub head_dim: u16,
    /// Precision dtype tag.
    pub dtype: TensorDType,
    /// Reserved padding bytes.
    pub _reserved: [u8; 7],
    /// Rolling hash of the token prefix sequence (from the radix tree).
    /// Used to cross-validate that the correct tensor is being fetched.
    pub sequence_prefix_hash: u64,
    /// Total byte length of the trailing raw tensor buffer.
    pub payload_bytes: u32,
    /// Cache-line padding to fill exactly 64 bytes.
    pub _cacheline_pad: [u8; 28],
}

/// Compile-time assertion: descriptor must be exactly one cache line.
const _: () = assert!(
    std::mem::size_of::<TensorBlockDescriptor>() == CACHE_LINE_BYTES,
    "TensorBlockDescriptor must be exactly 64 bytes"
);

/// Magic sentinel value: `KACH` in little-endian ASCII bytes.
pub const TENSOR_DESCRIPTOR_MAGIC: u32 = 0x4B41_4348;

impl TensorBlockDescriptor {
    /// Constructs a fully populated `TensorBlockDescriptor`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        layer_id: u16,
        num_layers: u16,
        block_size: u16,
        num_heads: u16,
        head_dim: u16,
        dtype: TensorDType,
        sequence_prefix_hash: u64,
    ) -> Self {
        let payload = Self::compute_payload_size(
            num_layers as usize,
            num_heads as usize,
            block_size as usize,
            head_dim as usize,
            dtype,
        ) as u32;

        Self {
            magic: TENSOR_DESCRIPTOR_MAGIC,
            layer_id,
            num_layers,
            block_size,
            num_heads,
            head_dim,
            dtype,
            _reserved: [0u8; 7],
            sequence_prefix_hash,
            payload_bytes: payload,
            _cacheline_pad: [0u8; 28],
        }
    }

    /// Computes the total byte size of the KV tensor payload for one block.
    ///
    /// ```text
    /// payload = 2 × num_layers × num_heads × block_size × head_dim × dtype_bytes
    /// │ factor 2 = Key tensor + Value tensor │
    /// ```
    ///
    /// # Example (LLaMA-3 8B, BF16)
    ///
    /// ```rust
    /// use kachedb_proto_tensor::{TensorBlockDescriptor, TensorDType};
    /// // 32 layers, 8 KV heads, 16 tokens/block, 128 head-dim, BF16
    /// let size = TensorBlockDescriptor::compute_payload_size(32, 8, 16, 128, TensorDType::BF16);
    /// assert_eq!(size, 2 * 32 * 8 * 16 * 128 * 2); // = 2,097,152 bytes = 2 MB
    /// ```
    pub const fn compute_payload_size(
        num_layers: usize,
        num_heads: usize,
        block_size: usize,
        head_dim: usize,
        dtype: TensorDType,
    ) -> usize {
        2 * num_layers * num_heads * block_size * head_dim * dtype.element_size_bytes()
    }

    /// Returns `true` if the magic sentinel is valid.
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.magic == TENSOR_DESCRIPTOR_MAGIC
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_is_64_bytes() {
        assert_eq!(std::mem::size_of::<TensorBlockDescriptor>(), 64);
    }

    #[test]
    fn descriptor_is_64_byte_aligned() {
        assert_eq!(std::mem::align_of::<TensorBlockDescriptor>(), 64);
    }

    #[test]
    fn llama3_8b_bf16_payload_size() {
        // LLaMA-3 8B: 32 layers, 8 GQA heads, 16 tokens/block, 128 head-dim, BF16
        let size = TensorBlockDescriptor::compute_payload_size(32, 8, 16, 128, TensorDType::BF16);
        // 2 * 32 * 8 * 16 * 128 * 2 = 2,097,152 bytes (exactly 2 MB!)
        assert_eq!(size, 2 * 1024 * 1024);
    }

    #[test]
    fn magic_validation() {
        let desc = TensorBlockDescriptor::new(0, 32, 16, 8, 128, TensorDType::BF16, 0xDEAD);
        assert!(desc.is_valid());
    }

    #[test]
    fn fp32_element_size() {
        assert_eq!(TensorDType::FP32.element_size_bytes(), 4);
    }

    #[test]
    fn fp16_bf16_element_size() {
        assert_eq!(TensorDType::FP16.element_size_bytes(), 2);
        assert_eq!(TensorDType::BF16.element_size_bytes(), 2);
    }

    #[test]
    fn fp8_int8_element_size() {
        assert_eq!(TensorDType::FP8E4M3.element_size_bytes(), 1);
        assert_eq!(TensorDType::INT8.element_size_bytes(), 1);
    }
}
