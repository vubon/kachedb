# 📊 Server Observability & Introspection

KacheDB provides self-describing runtime introspection compatible with modern Redis tooling, GUI clients (such as **Redis Insight** and **TablePlus**), and monitoring agents.

---

## 📋 Command Summary

| Command | Syntax | Description |
| :--- | :--- | :--- |
| **`INFO`** | `INFO [section]` | Returns server, memory, traffic, keyspace, and vector statistics. |
| **`HELLO`** | `HELLO [protover [AUTH user pass] [SETNAME name]]` | Protocol handshake negotiating RESP2 or RESP3 and returning connection metadata. |
| **`CLIENT`** | `CLIENT <SETNAME \| GETNAME \| ID \| LIST>` | Inspects and configures client connection state. |
| **`COMMAND`** | `COMMAND [DOCS]` | Returns server capability descriptors for client auto-discovery. |
| **`QUIT`** | `QUIT` | Gracefully closes the client connection. |

---

## 🛠️ Detailed Command Reference & Examples

### `INFO`
Outputs multi-section server metrics in standard Redis key-value format.

#### Syntax
```text
INFO [server | memory | stats | keyspace | vector]
```

#### `kachedb-cli` Example
```text
127.0.0.1:6379> INFO
# Server
kachedb_version:0.1.0
os:macos
arch_bits:64
process_id:48123
tcp_port:6379
uptime_in_seconds:1250

# Memory
used_memory:134217728
used_memory_human:128.00M
used_memory_peak:134217728
megaslabs_allocated:64
slab_slots_active:5120
fragmentation_ratio:1.00

# Stats
total_connections_received:128
total_commands_processed:150240
instantaneous_ops_per_sec:0
keyspace_hits:148000
keyspace_misses:2240

# Keyspace
db0:keys=5120,expires=450,avg_ttl=1820

# VectorEngine
active_indices:3
total_vectors:10240
vector_memory_bytes:655360
simd_kernel:auto
```

---

### `HELLO`
Negotiates RESP wire protocol version with the server (supports version 2 and version 3).

#### Syntax
```text
HELLO 3 [SETNAME client_name]
```

#### `kachedb-cli` Example
```text
127.0.0.1:6379> HELLO 3 SETNAME my_worker
 1) "server"
 2) "kachedb"
 3) "version"
 4) "0.1.0"
 5) "proto"
 6) (integer) 3
 7) "id"
 8) (integer) 1
 9) "mode"
10) "standalone"
11) "role"
12) "master"
13) "modules"
14) (empty array)
```

---

### `CLIENT`
Manages client connection names and identifiers.

#### Subcommands
* `CLIENT SETNAME <name>`: Assigns a human-readable name to the current TCP connection.
* `CLIENT GETNAME`: Retrieves the assigned name (or `nil`).
* `CLIENT ID`: Returns the unique client connection ID.
* `CLIENT LIST`: Returns connected client details.

#### `kachedb-cli` Example
```text
127.0.0.1:6379> CLIENT SETNAME web_app_1
OK

127.0.0.1:6379> CLIENT GETNAME
"web_app_1"

127.0.0.1:6379> CLIENT ID
(integer) 1
```

---

### `COMMAND` & `COMMAND DOCS`
Provides capability introspection so GUI tools and drivers can discover supported commands dynamically without crashing.

#### `kachedb-cli` Example
```text
127.0.0.1:6379> COMMAND DOCS
OK
```
