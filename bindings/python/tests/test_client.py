"""
Unit tests for KacheDB Python client and zero-copy tensor descriptor.
"""

import ctypes
import numpy as np
from kachedb import TensorBlockDescriptor, TensorDType, TENSOR_DESCRIPTOR_MAGIC

def test_descriptor_layout():
    assert ctypes.sizeof(TensorBlockDescriptor) == 64

def test_descriptor_magic_and_fields():
    desc = TensorBlockDescriptor()
    desc.magic = TENSOR_DESCRIPTOR_MAGIC
    desc.layer_id = 0
    desc.num_layers = 32
    desc.block_size = 16
    desc.num_heads = 8
    desc.head_dim = 128
    desc.dtype = TensorDType.BF16
    desc.payload_bytes = 2 * 32 * 8 * 16 * 128 * 2  # 2 MB

    assert desc.is_valid()
    assert desc.compute_shape() == (2, 32, 8, 16, 128)

def test_zero_copy_frombuffer():
    # Construct 64-byte header + 1024 floats payload
    desc = TensorBlockDescriptor()
    desc.magic = TENSOR_DESCRIPTOR_MAGIC
    desc.num_layers = 1
    desc.num_heads = 1
    desc.block_size = 16
    desc.head_dim = 64
    desc.dtype = TensorDType.FP32
    desc.payload_bytes = 1024 * 4

    header_bytes = bytes(desc)
    assert len(header_bytes) == 64

    # Simulated tensor payload
    raw_payload = np.arange(1024, dtype=np.float32).tobytes()
    full_buffer = bytearray(header_bytes + raw_payload)

    # Zero-copy view
    tensor_view = np.frombuffer(full_buffer, dtype=np.float32, count=1024, offset=64)

    assert tensor_view[0] == 0.0
    assert tensor_view[100] == 100.0

    # Modify underlying buffer in-place
    tensor_view[0] = 999.0
    # Verify in-place zero-copy mutation
    assert np.frombuffer(full_buffer, dtype=np.float32, count=1, offset=64)[0] == 999.0

if __name__ == "__main__":
    test_descriptor_layout()
    test_descriptor_magic_and_fields()
    test_zero_copy_frombuffer()
    print("✅ All Python descriptor & zero-copy tests passed!")
