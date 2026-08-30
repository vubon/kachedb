# 🔑 Core Key-Value Commands

KacheDB implements the standard **Redis / Valkey RESP2 and RESP3** binary wire protocol for standard key-value operations. All keys and values are treated as raw byte slices (`&[u8]`) and stored with zero heap allocation overhead inside 64-byte aligned Megaslab slots.

---

## 📋 Command Summary

| Command | Syntax | Complexity | Description |
| :--- | :--- | :---: | :--- |
| **`PING`** | `PING [message]` | $\mathcal{O}(1)$ | Tests server liveness; returns `PONG` or echoed message. |
| **`GET`** | `GET key` | $\mathcal{O}(1)$ | Retrieves binary value; returns `nil` if missing or expired. |
| **`SET`** | `SET key value [EX seconds] [PX millis]` | $\mathcal{O}(1)$ | Stores binary value with optional TTL expiration. |
| **`MGET`** | `MGET key [key ...]` | $\mathcal{O}(N)$ | Batch retrieves multiple keys in a single pipelined operation. |
| **`MSET`** | `MSET key value [key value ...]` | $\mathcal{O}(N)$ | Atomically stores multiple key-value pairs. |
| **`DEL`** | `DEL key [key ...]` | $\mathcal{O}(N)$ | Deletes keys and immediately frees Megaslab slots. |
| **`EXISTS`** | `EXISTS key [key ...]` | $\mathcal{O}(N)$ | Returns the count of existing, unexpired keys. |
| **`INCR`** | `INCR key` | $\mathcal{O}(1)$ | Atomically increments string integer value by 1. |
| **`DECR`** | `DECR key` | $\mathcal{O}(1)$ | Atomically decrements string integer value by 1. |
| **`INCRBY`** | `INCRBY key delta` | $\mathcal{O}(1)$ | Atomically increments string integer value by `delta`. |
| **`DECRBY`** | `DECRBY key delta` | $\mathcal{O}(1)$ | Atomically decrements string integer value by `delta`. |
| **`APPEND`** | `APPEND key value` | $\mathcal{O}(1)$ | Appends `value` to existing string, returning new byte length. |
| **`STRLEN`** | `STRLEN key` | $\mathcal{O}(1)$ | Returns length of string value in bytes (0 if missing). |

---

## 🛠️ Detailed Command Reference & Examples

### `SET` & `GET`
Stores and retrieves binary-safe values up to 2 MB per slot.

#### Syntax
```text
SET key value [EX seconds] [PX milliseconds]
GET key
```

#### `kachedb-cli` Example
```text
127.0.0.1:6379> SET user:100 "Alice"
OK

127.0.0.1:6379> GET user:100
"Alice"

127.0.0.1:6379> SET session:temp "abc123xyz" EX 30
OK

127.0.0.1:6379> GET missing_key
(nil)
```

#### Python SDK Example
```python
with KacheClient() as client:
    client.set("user:100", "Alice")
    val = client.get("user:100")  # b'Alice'
```

---

### `MSET` & `MGET`
Batch operations that store and retrieve multiple keys in a single network round-trip.

#### Syntax
```text
MSET key value [key value ...]
MGET key [key ...]
```

#### `kachedb-cli` Example
```text
127.0.0.1:6379> MSET config:theme "dark" config:lang "en" config:tz "UTC"
OK

127.0.0.1:6379> MGET config:theme config:lang config:missing config:tz
1) "dark"
2) "en"
3) (nil)
4) "UTC"
```

---

### `INCR`, `DECR`, `INCRBY`, `DECRBY`
Atomic integer arithmetic executed in-place on string values. If the key does not exist, it is initialized to `0` before applying the operation.

#### Syntax
```text
INCR key
DECR key
INCRBY key delta
DECRBY key delta
```

#### `kachedb-cli` Example
```text
127.0.0.1:6379> INCR page_views
(integer) 1

127.0.0.1:6379> INCRBY page_views 100
(integer) 101

127.0.0.1:6379> DECR page_views
(integer) 100

127.0.0.1:6379> DECRBY page_views 50
(integer) 50
```

---

### `APPEND` & `STRLEN`
String manipulation and length inspection.

#### Syntax
```text
APPEND key value
STRLEN key
```

#### `kachedb-cli` Example
```text
127.0.0.1:6379> SET doc:title "KacheDB"
OK

127.0.0.1:6379> APPEND doc:title " Architecture"
(integer) 20

127.0.0.1:6379> GET doc:title
"KacheDB Architecture"

127.0.0.1:6379> STRLEN doc:title
(integer) 20
```

---

### `DEL` & `EXISTS`
Key deletion and existence verification.

#### Syntax
```text
DEL key [key ...]
EXISTS key [key ...]
```

#### `kachedb-cli` Example
```text
127.0.0.1:6379> EXISTS user:1 user:2
(integer) 1

127.0.0.1:6379> DEL user:1 user:2
(integer) 1

127.0.0.1:6379> EXISTS user:1
(integer) 0
```
