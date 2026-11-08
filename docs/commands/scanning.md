# 🔍 Keyspace & Hash Cursor Scanning (`SCAN`, `HSCAN`)

KacheDB provides non-blocking, cursor-based iterations over the entire keyspace and individual Redis Hashes via `SCAN` and `HSCAN`. Both commands follow the Redis wire protocol and work seamlessly with existing Redis and Valkey client libraries.

Unlike traditional in-memory stores that can stall the server during full table scans or allocate large intermediate buffers, KacheDB implements **home-bucket bit-reversed cursor iteration**, a **zero-allocation glob matcher**, and **inline Short String Optimization (SSO)** to scan millions of keys with zero heap allocations and sub-microsecond latency per batch.

---

## 📋 Command Summary

| Command | Syntax | Complexity | Description |
| :--- | :--- | :---: | :--- |
| **`SCAN`** | `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]` | $\mathcal{O}(1)$ per step, $\mathcal{O}(N)$ full keyspace | Iterates through the entire keyspace across all 256 shards using a 64-bit composite cursor. |
| **`HSCAN`** | `HSCAN key cursor [MATCH pattern] [COUNT count]` | $\mathcal{O}(1)$ per step, $\mathcal{O}(F)$ full hash | Iterates through fields and values within a Redis hash stored in a Megaslab slot. |

* $N$ = total keys in the database; $F$ = total fields stored in the target hash.

---

## 📐 Architecture & Algorithmic Design

### 1. 64-Bit Composite Cursor Across 256 Shards
KacheDB partitions the keyspace across 256 independent Swiss Table shards to enable concurrent worker execution without lock contention:

```text
+-----------------------------------+---------------------------------------------------------+
| Bits 63..56 (8 bits: Shard Index) | Bits 55..0 (56 bits: Home-Bucket Bit-Reversed Cursor)   |
+-----------------------------------+---------------------------------------------------------+
  0 .. 255                            Shard-local Swiss Table bucket index
```

- **Shard Progression:** The scan begins at composite cursor `0`. Once a shard completes its bucket cycle, the high 8 bits increment to advance to the next shard (`cursor = (shard_idx + 1) << 56`). When all 256 shards have been scanned, cursor `0` is returned, signaling completion.
- **Empty Database Optimization:** If `sharded.len() == 0`, `SCAN` immediately returns `["0", []]` in $\mathcal{O}(1)$ time without traversing shards.
- **Shard Batching:** When scanning sparsely populated shards, KacheDB automatically sweeps up to `SHARD_BATCH_BUCKETS = 256` buckets per iteration, preventing empty-page roundtrip thrashing over TCP.

### 2. Home-Bucket Bit-Reversed Cursor Iteration
Standard linear counters (`0, 1, 2, ...`) miss keys or produce duplicates when a hash table dynamically doubles or halves in size during an active scan. 

KacheDB uses **bit-reversed radix counting**:
1. Incrementing from the highest significant bit to the lowest guarantees that when a table doubles ($M \to 2M$), entries in bucket $B$ only ever split into bucket $B$ and $B + M$.
2. The bit-reversed traversal visits parent and split buckets contiguously, ensuring **zero dropped keys** and **strictly bounded duplicates** even under continuous writes, rehashing, and tombstone compactions.

### 3. Inline SSO & Overflow Key Storage
To keep memory density high and cache-line lookups blazing fast:
- **Keys $\le 40$ Bytes:** Stored directly inside the 64-byte aligned `HashEntry` payload (`inline_key`), avoiding pointer indirection and eliminating secondary heap allocations.
- **Keys $> 40$ Bytes:** Stored in the shard's `overflow_keys` map, referenced by slot index and automatically purged upon deletion, overwrite, or expiration.

### 4. Zero-Allocation Glob Pattern Matcher
Pattern filtering via `MATCH` runs on borrowed byte slices (`&[u8]`) without allocating temporary strings or regex engines:
- **Supported Wildcards:**
  - `*`: Matches any sequence of characters (including empty). Single-star patterns (`prefix*`, `*suffix`, `*substr*`) use a linear search fast-path with zero backtracking, guaranteeing immunity to **ReDoS (Regular Expression Denial of Service)** attacks.
  - `?`: Matches exactly one byte.
  - `[abc]` / `[a-z]`: Matches any character in the set or range.
  - `[^abc]` / `[^a-z]`: Negated character classes.
  - `\`: Escapes special characters (`\*`, `\?`, `\[`, etc.).
- **Prefix Fast-Path:** The `CompiledPattern` analyzer extracts exact prefixes before the first wildcard (e.g. `user:session:*` extracts prefix `user:session:`). Non-matching candidate keys are rejected before running character matching.

---

## 🛠️ Detailed Command Reference

### `SCAN`
Iterates over the keyspace.

#### Syntax
```text
SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
```

#### Parameters
* **`cursor`** *(integer, required)*: The cursor position. Pass `0` to begin a new scan. Use the returned cursor in subsequent calls until the server returns `0`.
* **`MATCH pattern`** *(string, optional)*: Glob-style pattern. Only keys matching the pattern are returned.
* **`COUNT count`** *(integer, optional, default: 10)*: Work budget hint representing the number of bucket slots to examine per call.
* **`TYPE type`** *(string, optional)*: Filters keys by their data type: `string`, `hash`, or `vector`. Keys of other types are skipped without payload retrieval.

#### Return Value
An array of two elements:
1. The next cursor as a string (e.g. `"144115188075855872"`, or `"0"` when finished).
2. An array of matched keys.

#### `redis-cli` / `kachedb-cli` Example
```text
127.0.0.1:6379> MSET user:101 "Alice" user:102 "Bob" session:901 "active" config:env "prod"
OK

# Scan matching user:* prefix
127.0.0.1:6379> SCAN 0 MATCH "user:*" COUNT 10
1) "0"
2) 1) "user:101"
   2) "user:102"

# Filter strictly by data type
127.0.0.1:6379> SCAN 0 TYPE hash
1) "0"
2) (empty list or set)
```

---

### `HSCAN`
Iterates over the fields and values of a Redis Hash stored at `key`.

#### Syntax
```text
HSCAN key cursor [MATCH pattern] [COUNT count]
```

#### Parameters
* **`key`** *(string, required)*: Key of the hash to scan.
* **`cursor`** *(integer, required)*: Cursor position (always `0` to start).
* **`MATCH pattern`** *(string, optional)*: Glob-style pattern applied to **field names**.
* **`COUNT count`** *(integer, optional, default: 10)*: Maximum number of field-value pairs to return.

#### Return Value
An array of two elements:
1. The next cursor (`"0"` indicates iteration complete).
2. A flat array of alternating field-value pairs: `[field1, value1, field2, value2, ...]`. If the key does not exist or has expired, `["0", []]` is returned. If the key holds a non-hash value, `-WRONGTYPE` is returned.

#### `redis-cli` / `kachedb-cli` Example
```text
127.0.0.1:6379> HSET order:9801 item "laptop" price "1200" status "shipped" sku "TECH-100"
(integer) 4

# Match fields starting with 's'
127.0.0.1:6379> HSCAN order:9801 0 MATCH "s*"
1) "0"
2) 1) "status"
   2) "shipped"
   3) "sku"
   4) "TECH-100"
```

---

## 🐍 Client Integration Examples

### Python (`kachedb` Client SDK)

The native `kachedb` Python package provides both low-level paged methods and high-level generator iterators:

```python
from kachedb import KacheClient

with KacheClient(host="127.0.0.1", port=6379) as client:
    # 1. Populate test data
    for i in range(100):
        client.set(f"device:sensor:{i}", f"val_{i}")
    client.hset("device:metadata", mapping={"vendor": "Acme", "fw_version": "2.4.1", "status": "online"})

    # 2. Generator-based streaming keyspace iteration (recommended)
    print("--- Streaming SCAN Iteration ---")
    for key in client.scan_iter(match="device:sensor:*", count=25):
        print(f"Discovered key: {key.decode('utf-8')}")

    # 3. Manual page-by-page SCAN
    cursor = 0
    all_keys = []
    while True:
        cursor, keys = client.scan(cursor, match="device:sensor:*", count=50)
        all_keys.extend(keys)
        if cursor == 0:
            break
    print(f"Total keys retrieved: {len(all_keys)}")

    # 4. Streaming Hash iteration
    print("--- Streaming HSCAN Iteration ---")
    for field, value in client.hscan_iter("device:metadata", match="*"):
        print(f"Field: {field.decode('utf-8')} -> Value: {value.decode('utf-8')}")
```

### Python (`redis-py`)

Because KacheDB is fully wire-compatible with RESP2/RESP3, standard Redis libraries work out-of-the-box:

```python
import redis

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

# Scan keyspace with pattern match
for key in r.scan_iter(match="device:sensor:*", count=50):
    print("Key:", key)

# Scan hash fields
for field, value in r.hscan_iter("device:metadata"):
    print(f"{field} = {value}")
```

---

## 💾 Snapshot Durability & Recovery (Format V5)

In **Snapshot Format V5 (`KDB\x05`)**, all key byte slices are serialized with explicit 16-bit length prefixes alongside their value payloads:
- **Lossless Reconstruction:** Restoring a V5 snapshot immediately populates both the Swiss Table SIMD index and the `HashEntry` inline SSO bytes (or `overflow_keys` map for long keys).
- **Backward Compatibility:** KacheDB transparently reads legacy `KDB\x02`, `KDB\x03`, and `KDB\x04` snapshots. Legacy anonymous keys are logged with a telemetry warning and lazily upgraded to named entries whenever updated over TCP (`SET`, `HSET`, `EXPIRE`, `PERSIST`).

