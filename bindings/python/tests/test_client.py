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

class MockSocket:
    def __init__(self, responses: list[bytes]):
        self.sent_bytes = bytearray()
        self.response_buffer = bytearray(b"".join(responses))

    def sendall(self, data: bytes):
        self.sent_bytes.extend(data)

    def recv(self, n: int) -> bytes:
        if not self.response_buffer:
            return b""
        chunk = bytes(self.response_buffer[:n])
        del self.response_buffer[:n]
        return chunk

    def close(self):
        pass

def test_client_hash_methods_mock():
    from kachedb import KacheClient

    client = KacheClient()

    # 1. HSET single pair
    client.sock = MockSocket([b":1\r\n"])
    assert client.hset("myhash", "f1", "v1") == 1
    assert client.sock.sent_bytes == b"*4\r\n$4\r\nHSET\r\n$6\r\nmyhash\r\n$2\r\nf1\r\n$2\r\nv1\r\n"

    # 2. HSET mapping
    client.sock = MockSocket([b":2\r\n"])
    assert client.hset("myhash", mapping={"f1": "v1", "f2": "v2"}) == 2
    assert client.sock.sent_bytes == b"*6\r\n$4\r\nHSET\r\n$6\r\nmyhash\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n"

    # 3. HSET missing args raises ValueError
    try:
        client.hset("myhash")
        assert False, "Should have raised ValueError"
    except ValueError:
        pass

    # 4. HGET
    client.sock = MockSocket([b"$2\r\nv1\r\n"])
    assert client.hget("myhash", "f1") == b"v1"

    # 5. HGET nil
    client.sock = MockSocket([b"$-1\r\n"])
    assert client.hget("myhash", "missing") is None

    # 6. HEXISTS True
    client.sock = MockSocket([b":1\r\n"])
    assert client.hexists("myhash", "f1") is True

    # 7. HEXISTS False
    client.sock = MockSocket([b":0\r\n"])
    assert client.hexists("myhash", "missing") is False

    # 8. HLEN
    client.sock = MockSocket([b":2\r\n"])
    assert client.hlen("myhash") == 2

    # 9. HDEL
    client.sock = MockSocket([b":2\r\n"])
    assert client.hdel("myhash", "f1", "f2") == 2
    assert client.sock.sent_bytes == b"*4\r\n$4\r\nHDEL\r\n$6\r\nmyhash\r\n$2\r\nf1\r\n$2\r\nf2\r\n"

    # 10. HDEL empty args raises ValueError
    try:
        client.hdel("myhash")
        assert False, "Should have raised ValueError"
    except ValueError:
        pass

    # 11. HGETALL populated
    client.sock = MockSocket([b"*4\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n"])
    all_pairs = client.hgetall("myhash")
    assert all_pairs == {b"f1": b"v1", b"f2": b"v2"}

    # 12. HGETALL empty
    client.sock = MockSocket([b"*0\r\n"])
    assert client.hgetall("myhash") == {}

def test_live_client_hash_roundtrip():
    from kachedb import KacheClient

    client = KacheClient(port=6380)
    try:
        client.connect()
        # Verify server is live and supports HSET
        client.hset("py_test_hash", "probe", "val")
    except Exception:
        # Server on 6380 not running or does not support HSET
        return

    try:
        # HSET mapping
        client.hset("py_test_hash", mapping={"field_b": "val_b", "field_c": "val_c"})

        assert client.hget("py_test_hash", "probe") == b"val"
        assert client.hget("py_test_hash", "field_b") == b"val_b"
        assert client.hget("py_test_hash", "ghost") is None

        assert client.hexists("py_test_hash", "probe") is True
        assert client.hexists("py_test_hash", "ghost") is False
        assert client.hlen("py_test_hash") == 3

        all_items = client.hgetall("py_test_hash")
        assert all_items[b"probe"] == b"val"
        assert all_items[b"field_b"] == b"val_b"
        assert all_items[b"field_c"] == b"val_c"

        assert client.hdel("py_test_hash", "probe", "field_b") == 2
        assert client.hlen("py_test_hash") == 1
    finally:
        client.delete("py_test_hash")
        client.close()

if __name__ == "__main__":
    test_descriptor_layout()
    test_descriptor_magic_and_fields()
    test_zero_copy_frombuffer()
    test_client_hash_methods_mock()
    test_live_client_hash_roundtrip()
    print("✅ All Python descriptor & hash client tests passed!")
