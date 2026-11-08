"""
KacheDB Zero-Copy Python Client for Redis RESP queries and POSIX Shared Memory Tensor extraction.
"""

import mmap
import os
import socket
from collections.abc import Iterator

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
        self.sock: socket.socket | None = None
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

    def get(self, key: str | bytes) -> bytes | None:
        """Gets value for key via TCP RESP protocol."""
        self._send_command(["GET", key])
        res = self._read_response()
        return res if isinstance(res, bytes) else None

    def set(self, key: str | bytes, value: str | bytes, ex: int | None = None) -> bool:
        """Sets key-value pair in KacheDB."""
        args = ["SET", key, value]
        if ex:
            args.extend(["EX", str(ex)])
        self._send_command(args)
        res = self._read_response()
        return res == "OK"

    def delete(self, key: str | bytes) -> int:
        """Deletes key from KacheDB."""
        self._send_command(["DEL", key])
        res = self._read_response()
        return int(res) if isinstance(res, (int, str)) and str(res).isdigit() else 0

    # ── Redis Hash Commands ───────────────────────────────────────────────────

    def hset(
        self,
        key: str | bytes,
        field: str | bytes | None = None,
        value: str | bytes | None = None,
        mapping: dict[str | bytes, str | bytes] | None = None,
    ) -> int:
        """
        Sets field(s) in a hash stored at key.
        Supports single field-value pairs or a mapping dict:
            client.hset("myhash", "field1", "val1")
            client.hset("myhash", mapping={"field1": "val1", "field2": "val2"})
        """
        args: list[str | bytes] = ["HSET", key]
        if mapping:
            for k, v in mapping.items():
                args.extend([k, v])
        elif field is not None and value is not None:
            args.extend([field, value])
        else:
            raise ValueError("Must provide either (field, value) or mapping")

        self._send_command(args)
        res = self._read_response()
        return int(res) if isinstance(res, (int, str)) and str(res).isdigit() else 0

    def hget(self, key: str | bytes, field: str | bytes) -> bytes | None:
        """Gets value of a hash field. Returns None if field or key does not exist."""
        self._send_command(["HGET", key, field])
        res = self._read_response()
        return res if isinstance(res, bytes) else None

    def hdel(self, key: str | bytes, *fields: str | bytes) -> int:
        """Deletes one or more fields from a hash. Returns the number of fields removed."""
        if not fields:
            raise ValueError("At least one field must be specified")
        args: list[str | bytes] = ["HDEL", key]
        args.extend(fields)
        self._send_command(args)
        res = self._read_response()
        return int(res) if isinstance(res, (int, str)) and str(res).isdigit() else 0

    def hexists(self, key: str | bytes, field: str | bytes) -> bool:
        """Returns True if the hash field exists, False otherwise."""
        self._send_command(["HEXISTS", key, field])
        res = self._read_response()
        return res == 1 or res == "1"

    def hlen(self, key: str | bytes) -> int:
        """Returns the number of fields contained in the hash."""
        self._send_command(["HLEN", key])
        res = self._read_response()
        return int(res) if isinstance(res, (int, str)) and str(res).isdigit() else 0

    def hgetall(self, key: str | bytes) -> dict[bytes, bytes]:
        """
        Returns all fields and values of the hash stored at key as a dictionary of bytes.
        """
        self._send_command(["HGETALL", key])
        res = self._read_response()
        if not isinstance(res, list):
            return {}
        result: dict[bytes, bytes] = {}
        for i in range(0, len(res), 2):
            if i + 1 < len(res):
                f = res[i] if isinstance(res[i], bytes) else str(res[i]).encode("utf-8")
                v = res[i + 1] if isinstance(res[i + 1], bytes) else str(res[i + 1]).encode("utf-8")
                result[f] = v
        return result

    # ── Keyspace & Hash Scanning ──────────────────────────────────────────────

    def scan(
        self,
        cursor: int = 0,
        match: str | bytes | None = None,
        count: int | None = None,
        type: str | bytes | None = None,
    ) -> tuple[int, list[bytes]]:
        """
        Incrementally iterates over keys in the database.
        Returns a tuple of (next_cursor, list_of_keys).
        """
        args: list[str | bytes] = ["SCAN", str(cursor)]
        if match is not None:
            args.extend(["MATCH", match])
        if count is not None:
            args.extend(["COUNT", str(count)])
        if type is not None:
            args.extend(["TYPE", type])

        self._send_command(args)
        res = self._read_response()
        if not isinstance(res, list) or len(res) < 2:
            return 0, []

        next_cursor = int(res[0])
        raw_keys = res[1] if isinstance(res[1], list) else []
        keys = [
            k if isinstance(k, bytes) else str(k).encode("utf-8")
            for k in raw_keys
        ]
        return next_cursor, keys

    def scan_iter(
        self,
        match: str | bytes | None = None,
        count: int | None = None,
        type: str | bytes | None = None,
    ) -> Iterator[bytes]:
        """
        Convenience generator yielding keys matching the pattern until the cursor completes.
        """
        cursor = 0
        while True:
            cursor, keys = self.scan(cursor=cursor, match=match, count=count, type=type)
            yield from keys
            if cursor == 0:
                break

    def hscan(
        self,
        key: str | bytes,
        cursor: int = 0,
        match: str | bytes | None = None,
        count: int | None = None,
    ) -> tuple[int, dict[bytes, bytes]]:
        """
        Incrementally iterates fields and values of a hash stored at key.
        Returns a tuple of (next_cursor, dict_of_field_values).
        """
        args: list[str | bytes] = ["HSCAN", key, str(cursor)]
        if match is not None:
            args.extend(["MATCH", match])
        if count is not None:
            args.extend(["COUNT", str(count)])

        self._send_command(args)
        res = self._read_response()
        if not isinstance(res, list) or len(res) < 2:
            return 0, {}

        next_cursor = int(res[0])
        raw_items = res[1] if isinstance(res[1], list) else []
        items: dict[bytes, bytes] = {}
        for i in range(0, len(raw_items), 2):
            if i + 1 < len(raw_items):
                f = raw_items[i] if isinstance(raw_items[i], bytes) else str(raw_items[i]).encode("utf-8")
                v = raw_items[i + 1] if isinstance(raw_items[i + 1], bytes) else str(raw_items[i + 1]).encode("utf-8")
                items[f] = v
        return next_cursor, items

    def hscan_iter(
        self,
        key: str | bytes,
        match: str | bytes | None = None,
        count: int | None = None,
    ) -> Iterator[tuple[bytes, bytes]]:
        """
        Convenience generator yielding (field, value) pairs until the hash scan completes.
        """
        cursor = 0
        while True:
            cursor, items = self.hscan(key, cursor=cursor, match=match, count=count)
            yield from items.items()
            if cursor == 0:
                break

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

    def _send_command(self, args: list[str | bytes]):
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

    def _read_response(self) -> str | bytes | int | None:
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
        elif marker == b"*":
            num_elements = int(payload)
            if num_elements == -1:
                return None
            elements = []
            for _ in range(num_elements):
                elements.append(self._read_response())
            return elements
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
