"""
KacheDB Zero-Copy Python Client for Redis RESP queries and POSIX Shared Memory Tensor extraction.
"""

import mmap
import os
import socket
from typing import Optional, Union

import numpy as np

from .descriptor import TENSOR_DESCRIPTOR_MAGIC, TensorBlockDescriptor, TensorDType

class KacheClient:
    """
    High-performance client for KacheDB.
    
    Communicates with the KacheDB daemon over TCP for control commands and reads
    KV-cache tensors with zero-copy directly from /dev/shm.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 6379):
        self.host = host
        self.port = port
        self.sock: Optional[socket.socket] = None
        self.shm_mappings: dict[int, mmap.mmap] = {}

    def connect(self) -> "KacheClient":
        """Establishes TCP connection to KacheDB daemon."""
        self.sock = socket.create_connection((self.host, self.port))
        return self

    def close(self):
        """Closes TCP socket and unmaps all shared memory regions."""
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

        for shm in self.shm_mappings.values():
            try:
                shm.close()
            except Exception:
                pass
        self.shm_mappings.clear()

    def __enter__(self) -> "KacheClient":
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # ── Redis Protocol Commands ───────────────────────────────────────────────

    def ping(self) -> str:
        """Sends PING command."""
        self._send_command(["PING"])
        return self._read_response()

    def get(self, key: Union[str, bytes]) -> Optional[bytes]:
        """Gets value for key via TCP RESP protocol."""
        self._send_command(["GET", key])
        res = self._read_response()
        return res if isinstance(res, bytes) else None

    def set(self, key: Union[str, bytes], value: Union[str, bytes], ex: Optional[int] = None) -> bool:
        """Sets key-value pair in KacheDB."""
        args = ["SET", key, value]
        if ex:
            args.extend(["EX", str(ex)])
        self._send_command(args)
        res = self._read_response()
        return res == "OK"

    def delete(self, key: Union[str, bytes]) -> int:
        """Deletes key from KacheDB."""
        self._send_command(["DEL", key])
        res = self._read_response()
        return int(res) if isinstance(res, (int, str)) and str(res).isdigit() else 0

    # ── Zero-Copy Shared Memory KV-Cache Extraction ───────────────────────────

    def attach_shm(self, core_id: int, size_bytes: int = 64 * 1024 * 1024) -> mmap.mmap:
        """
        Attaches to the named shared memory region `/dev/shm/kachedb_{core_id}`.
        """
        if core_id in self.shm_mappings:
            return self.shm_mappings[core_id]

        shm_path = f"/dev/shm/kachedb_{core_id}"

        if os.path.exists(shm_path):
            fd = os.open(shm_path, os.O_RDWR)
            shm = mmap.mmap(fd, size_bytes, mmap.MAP_SHARED, mmap.PROT_READ | mmap.PROT_WRITE)
            os.close(fd)
        else:
            # Fallback (e.g. anonymous or mock memory for testing)
            shm = mmap.mmap(-1, size_bytes)

        self.shm_mappings[core_id] = shm
        return shm

    def read_tensor_zero_copy(self, core_id: int, byte_offset: int) -> np.ndarray:
        """
        Reads a 64-byte descriptor and wraps the trailing payload as a zero-copy numpy array.
        """
        shm = self.attach_shm(core_id)
        shm.seek(byte_offset)

        # Read 64-byte descriptor
        header_bytes = shm.read(64)
        desc = TensorBlockDescriptor.from_buffer_copy(header_bytes)

        if desc.magic != TENSOR_DESCRIPTOR_MAGIC:
            raise ValueError(f"Invalid magic header: {hex(desc.magic)}, expected {hex(TENSOR_DESCRIPTOR_MAGIC)}")

        payload_offset = byte_offset + 64
        payload_bytes = desc.payload_bytes

        # Map memory slice without copying
        dtype_map = {
            TensorDType.FP32: np.float32,
            TensorDType.FP16: np.float16,
            TensorDType.BF16: np.uint16,  # uint16 view for BF16 in standard numpy
            TensorDType.INT8: np.int8,
            TensorDType.INT4: np.uint8,
        }
        np_dtype = dtype_map.get(desc.dtype, np.uint8)

        # Create zero-copy numpy buffer view
        array_view = np.frombuffer(shm, dtype=np_dtype, count=payload_bytes // desc.dtype.element_size_bytes(), offset=payload_offset)
        return array_view

    # ── Internal RESP Wire Parsing Helpers ────────────────────────────────────

    def _send_command(self, args: list[Union[str, bytes]]):
        if not self.sock:
            raise ConnectionError("Not connected to KacheDB")

        buf = bytearray()
        buf.extend(f"*{len(args)}\r\n".encode())
        for arg in args:
            if isinstance(arg, str):
                arg_bytes = arg.encode("utf-8")
            else:
                arg_bytes = arg
            buf.extend(f"${len(arg_bytes)}\r\n".encode())
            buf.extend(arg_bytes)
            buf.extend(b"\r\n")

        self.sock.sendall(buf)

    def _read_response(self) -> Union[str, bytes, int, None]:
        if not self.sock:
            raise ConnectionError("Not connected to KacheDB")

        line = self._readline()
        if not line:
            return None

        marker = line[0:1]
        payload = line[1:]

        if marker == b"+":
            return payload.decode("utf-8")
        elif marker == b"-":
            raise RuntimeError(payload.decode("utf-8"))
        elif marker == b":":
            return int(payload)
        elif marker == b"$":
            length = int(payload)
            if length == -1:
                return None
            data = self._read_exact(length)
            self._read_exact(2)  # consume trailing \r\n
            return data
        else:
            return payload.decode("utf-8", errors="replace")

    def _readline(self) -> bytes:
        data = bytearray()
        while True:
            char = self.sock.recv(1)
            if not char:
                break
            if char == b"\r":
                next_char = self.sock.recv(1)
                if next_char == b"\n":
                    break
                data.extend(char)
                data.extend(next_char)
            else:
                data.extend(char)
        return bytes(data)

    def _read_exact(self, n: int) -> bytes:
        data = bytearray()
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise EOFError("Unexpected end of stream")
            data.extend(chunk)
        return bytes(data)
