# ⏱️ TTL & Key Lifecycle Commands

KacheDB provides high-resolution time-to-live (TTL) expiration support with **dual-engine memory reclamation**:
1. **Sub-Nanosecond Passive Expiry:** On `GET`/`EXISTS` queries, the Swiss Table verifies the cached second timestamp in $\approx 0.5\text{ ns}$.
2. **Active $\mathcal{O}(1)$ Background Timing Wheel:** A lock-free 3,600-bucket per-core circular wheel proactively evicts expired keys and returns 2 MB Megaslab slots back to the free-list every second without waiting for read traffic.

---

## 📋 Command Summary

| Command | Syntax | Return Value | Complexity | Description |
| :--- | :--- | :---: | :---: | :--- |
| **`EXPIRE`** | `EXPIRE key seconds` | `1` or `0` | $\mathcal{O}(1)$ | Sets timeout on `key` in seconds. |
| **`PEXPIRE`** | `PEXPIRE key milliseconds` | `1` or `0` | $\mathcal{O}(1)$ | Sets timeout on `key` in milliseconds. |
| **`EXPIREAT`** | `EXPIREAT key unix_seconds` | `1` or `0` | $\mathcal{O}(1)$ | Sets expiration deadline as an absolute Unix timestamp. |
| **`PEXPIREAT`** | `PEXPIREAT key unix_millis` | `1` or `0` | $\mathcal{O}(1)$ | Sets expiration deadline as an absolute millisecond timestamp. |
| **`TTL`** | `TTL key` | `integer` | $\mathcal{O}(1)$ | Returns remaining TTL in seconds (`-2` if missing, `-1` if no TTL). |
| **`PTTL`** | `PTTL key` | `integer` | $\mathcal{O}(1)$ | Returns remaining TTL in milliseconds (`-2` if missing, `-1` if no TTL). |
| **`PERSIST`** | `PERSIST key` | `1` or `0` | $\mathcal{O}(1)$ | Removes timeout, persisting the key indefinitely. |

---

## 🛠️ Command Details & Examples

### `EXPIRE` & `PEXPIRE`
Sets a relative timeout from the current time.

#### `kachedb-cli` Example
```text
127.0.0.1:6379> SET user:auth "token_xyz123"
OK

# Expire in 60 seconds
127.0.0.1:6379> EXPIRE user:auth 60
(integer) 1

# Check remaining seconds
127.0.0.1:6379> TTL user:auth
(integer) 59

# Check remaining milliseconds
127.0.0.1:6379> PTTL user:auth
(integer) 58942

# Attempting to expire a non-existent key returns 0
127.0.0.1:6379> EXPIRE missing_key 30
(integer) 0
```

---

### `EXPIREAT` & `PEXPIREAT`
Sets an absolute Unix epoch deadline timestamp.

#### `kachedb-cli` Example
```text
# Expire at Unix timestamp 1893456000 (Jan 1, 2030)
127.0.0.1:6379> EXPIREAT user:auth 1893456000
(integer) 1

127.0.0.1:6379> TTL user:auth
(integer) 109895658
```

---

### `PERSIST`
Removes the timeout from a key, converting it back to a permanent key.

#### `kachedb-cli` Example
```text
127.0.0.1:6379> PERSIST user:auth
(integer) 1

# TTL returns -1 for unexpired keys without timeout
127.0.0.1:6379> TTL user:auth
(integer) -1
```

---

## 🏗️ Architecture: How the Timing Wheel Works

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│               HashedTimingWheel (3,600 Circular 1-Second Buckets)                       │
│                                                                                        │
│   Slot 0       Slot 1       Slot 2        ...         Slot 3599                        │
│ ┌─────────┐  ┌─────────┐  ┌─────────┐               ┌─────────┐                        │
│ │ Entries │  │ Entries │  │ Entries │               │ Entries │                        │
│ └────┬────┘  └────┬────┘  └────┬────┘               └────┬────┘                        │
│      │            │            │                         │                             │
│      ▼            ▼            ▼                         ▼                             │
│   KeyHash 1    KeyHash 2    KeyHash 3                 KeyHash N                        │
│  [BlockID 5]  [BlockID 9]  [BlockID 2]               [BlockID 14]                      │
└────────────────────────────────────────────────────────────────────────────────────────┘
                                      ▲
                                      │ Current Second Pointer (Advances every 1.0s)
                               [ Event Loop Tick ]
```

1. **In-Place Mutation:** `EXPIRE` and `PERSIST` mutate `expiry_sec: u32` in the 64-byte `TableEntry` in-place.
2. **Zero Allocation Jitter:** Scheduling a key in the timing wheel appends to a per-bucket pre-allocated array.
3. **Double-Free Safety:** When the 1-second tick advances, the worker thread executes `table.remove_if_matching(key_hash, slab_block_id)`. If the key was updated or replaced by a new `SET` command, the stale entry is safely ignored without double-deallocating slab blocks.
