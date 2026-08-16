"""
Binary decoder for 64-byte TensorBlockDescriptor matching KacheDB Rust layout.
"""

import ctypes
from enum import IntEnum

TENSOR_DESCRIPTOR_MAGIC = 0x4B414348  # "KACH"

class TensorDType(IntEnum):
    FP32 = 0
    FP16 = 1
    BF16 = 2
    FP8E4M3 = 3
    FP8E5M2 = 4
    INT8 = 5
    INT4 = 6

    def element_size_bytes(self) -> int:
        if self in (TensorDType.FP32,):
            return 4
        elif self in (TensorDType.FP16, TensorDType.BF16):
            return 2
        else:
            return 1

class TensorBlockDescriptor(ctypes.Structure):
    _fields_ = [
        ("magic", ctypes.c_uint32),
        ("layer_id", ctypes.c_uint16),
        ("num_layers", ctypes.c_uint16),
        ("block_size", ctypes.c_uint16),
        ("num_heads", ctypes.c_uint16),
        ("head_dim", ctypes.c_uint16),
        ("dtype", ctypes.c_uint8),
        ("_reserved", ctypes.c_uint8 * 7),
        ("sequence_prefix_hash", ctypes.c_uint64),
        ("payload_bytes", ctypes.c_uint32),
        ("_cacheline_pad", ctypes.c_uint8 * 28),
    ]

    def is_valid(self) -> bool:
        return self.magic == TENSOR_DESCRIPTOR_MAGIC

    def compute_shape(self) -> tuple[int, int, int, int, int]:
        """Returns the shape: (2, num_layers, num_heads, block_size, head_dim) where 2 = Key + Value."""
        return (2, self.num_layers, self.num_heads, self.block_size, self.head_dim)

    @classmethod
    def from_buffer_copy(cls, source: bytes) -> "TensorBlockDescriptor":
        if len(source) < 64:
            raise ValueError(f"Descriptor requires 64 bytes, got {len(source)}")
        return cls.from_buffer_copy(source[:64])
