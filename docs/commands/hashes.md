# 📦 Redis Hash Primitives

KacheDB provides high-performance, strictly $\mathcal{O}(1)$ **Redis Hash** commands (`HSET`, `HGET`, `HDEL`, `HEXISTS`, `HLEN`, `HGETALL`) over the standard RESP2 and RESP3 wire protocol.

Unlike traditional in-memory stores that rely on linked-list linear scans (`listpack`) or pointer-chasing heap structures (`dictEntry`), KacheDB implements an **in-slot open-addressing directory** packed directly inside pre-allocated 64-byte aligned Megaslab slots.

---

## 📋 Command Summary

| Command | Syntax | Complexity | Description |
| :--- | :--- | :---: | :--- |
| **`HSET`** | `HSET key field value [field value ...]` | $\mathcal{O}(M)$ | Sets field-value pairs in the hash. Probing each pair is strictly $\mathcal{O}(1)$. Returns the count of newly added fields. |
| **`HGET`** | `HGET key field` | **Strict $\mathcal{O}(1)$** | Retrieves the binary value of the specified field; returns `(nil)` if the key or field is missing. |
| **`HDEL`** | `HDEL key field [field ...]` | $\mathcal{O}(M)$ | Removes field(s) from the hash. Returns the number of fields removed. Deletes the key if the hash becomes empty. |
| **`HEXISTS`** | `HEXISTS key field` | **Strict $\mathcal{O}(1)$** | Returns `1` if the field exists in the hash, or `0` if the key or field does not exist. |
| **`HLEN`** | `HLEN key` | $\mathcal{O}(1)$ | Returns the number of live fields in the hash; returns `0` if the key does not exist. |
| **`HGETALL`** | `HGETALL key` | $\mathcal{O}(N)$ | Returns a flat array of all field-value pairs `[field1, val1, field2, val2, ...]`; returns an empty list if the key does not exist. |

* $M$ = number of fields in the request; $N$ = total live fields stored in the hash.

---

## 🏗️ Architecture & Strict $\mathcal{O}(1)$ In-Slot Directory

Traditional Redis stores hashes in two encodings:
- **`listpack` ($\le 512$ fields):** Linear byte array where every read and write is $\mathcal{O}(N)$ linear scan with `memmove()` memory reallocation.
- **`dict` ($> 512$ fields):** Chained hash table with 3 heap allocations per field (`dictEntry`, key `sds`, value `sds`), consuming 50–80 bytes of pointer overhead per field and pointer-chasing CPU cache misses (~80–120 ns).

### The KacheDB In-Slot Directory

KacheDB embeds both data payload and open-addressing hash table directory directly into a single slab slot:

```text
+---------------------------------------------------------------------------------------+
| Header (8B)  | Payloads Area (f1:v1, f2:v2, ...) | Directory Buckets (Tail of Slot)   |
| count: u16   | Contiguous raw byte slices        | Open-addressing table of K buckets |
| deleted: u16 | Length-prefixed keys & values     | [hash32: u32, offset: u16, ...]   |
+---------------------------------------------------------------------------------------+
```

1. **Sub-25 ns Lookups:** Probing computes `bucket_idx = hash32 & (K - 1)`. At load factor $\le 0.70$, probes resolve in 1.1–1.3 bucket steps, executing within the **exact same 64-byte L1 CPU cache line**.
2. **Zero Heap Allocations:** For requests with 8 or fewer field pairs, operations allocate zero heap memory (`SmallVec` stack buffer).
3. **App-Cache Megaslab Isolation:** Hashes strictly reside in `AppSmall` (128 B), `AppMedium` (512 B), or `AppLarge` (4 KB) slots. Slots are automatically upgraded as fields grow.
4. **Type Safety:** The key type is tracked via `value_type = VALUE_TYPE_HASH` in the 64-byte SwissTable entry. String commands (`GET`, `APPEND`, `INCR`) reject hash keys with `-WRONGTYPE Operation against a key holding the wrong kind of value`.

---

## 🛠️ Detailed Command Reference & Examples

### `HSET`
Sets one or more field-value pairs in a hash. If the key does not exist, a new hash is created. If a field already exists, its value is overwritten.

#### Syntax
```text
HSET key field value [field value ...]
```

#### `kachedb-cli` / `redis-cli` Example
```text
127.0.0.1:6379> HSET user:1000 name "Alice Smith" email "alice@example.com" role "admin"
(integer) 3

127.0.0.1:6379> HSET user:1000 role "superadmin" age "32"
(integer) 1
```

---

### `HGET`
Retrieves the value associated with `field` in the hash stored at `key`.

#### Syntax
```text
HGET key field
```

#### `kachedb-cli` / `redis-cli` Example
```text
127.0.0.1:6379> HGET user:1000 name
"Alice Smith"

127.0.0.1:6379> HGET user:1000 role
"superadmin"

127.0.0.1:6379> HGET user:1000 missing_field
(nil)
```

---

### `HDEL`
Removes the specified field(s) from the hash. If all fields are deleted, the parent key is removed from the database and its slab slot is reclaimed.

#### Syntax
```text
HDEL key field [field ...]
```

#### `kachedb-cli` / `redis-cli` Example
```text
127.0.0.1:6379> HDEL user:1000 age non_existing
(integer) 1

127.0.0.1:6379> HEXISTS user:1000 age
(integer) 0
```

---

### `HEXISTS`
Determines if a hash field exists.

#### Syntax
```text
HEXISTS key field
```

#### `kachedb-cli` / `redis-cli` Example
```text
127.0.0.1:6379> HEXISTS user:1000 email
(integer) 1

127.0.0.1:6379> HEXISTS user:1000 phone
(integer) 0
```

---

### `HLEN`
Returns the number of live fields contained in the hash.

#### Syntax
```text
HLEN key
```

#### `kachedb-cli` / `redis-cli` Example
```text
127.0.0.1:6379> HLEN user:1000
(integer) 3
```

---

### `HGETALL`
Returns all fields and values of the hash stored at `key` as a flat array of alternating `[field1, value1, field2, value2, ...]`.

#### Syntax
```text
HGETALL key
```

#### `kachedb-cli` / `redis-cli` Example
```text
127.0.0.1:6379> HGETALL user:1000
1) "name"
2) "Alice Smith"
3) "email"
4) "alice@example.com"
5) "role"
6) "superadmin"
```

---

## 🐍 Client Integration Examples

### Python (`redis-py`)
```python
import redis

client = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

# Set multiple fields
client.hset("session:agent_42", mapping={
    "model": "claude-3-7-sonnet",
    "step": "12",
    "tokens": "8450",
    "status": "active"
})

# Retrieve a single field
model = client.hget("session:agent_42", "model")
print(f"Active Model: {model}")

# Query existence and length
assert client.hexists("session:agent_42", "tokens") == 1
field_count = client.hlen("session:agent_42")
print(f"Total fields: {field_count}")

# Fetch all fields as a dictionary
session_data = client.hgetall("session:agent_42")
print(session_data)
# {'model': 'claude-3-7-sonnet', 'step': '12', 'tokens': '8450', 'status': 'active'}

# Delete fields
client.hdel("session:agent_42", "status")
```

---

## 💾 Durability & Snapshots (Format V4)

KacheDB persists Redis Hash entries with full ACID durability:
1. **Append-Only File (AOF):** Hash mutations are logged as logical opcode pairs:
   - `0x07` (`HSet`): `[key_len, key, field_len, field, val_len, val]`
   - `0x08` (`HDel`): `[key_len, key, field_len, field]`
2. **Snapshot Format V4 (`KDB\x04`):** Snapshots write a typed record header `RecordType = 0x01` (Hash) followed by encoded field-value pairs, maintaining compatibility with both plaintext and encrypted snapshots (`AES-256-GCM` or `ChaCha20-Poly1305`).
