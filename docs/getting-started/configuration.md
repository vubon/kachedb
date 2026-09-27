# ⚙️ Server Configuration & Tuning

This guide covers operational parameters, memory pool configuration, CPU core pinning, and kernel bypass tuning for **KacheDB**.

---

## 🚀 Daemon Command-Line Arguments (`kachedb-server`)

The `kachedb-server` binary accepts several command-line flags to control hardware allocation and network binding:

```bash
./target/release/kachedb-server [OPTIONS]
```

### Options Reference

| Flag | Long Option | Description | Default | Recommended Production |
| :--- | :--- | :--- | :--- | :--- |
| `-c` | `--config <PATH>` | Path to configuration file (e.g., `kachedb.conf`) | None | `/etc/kachedb/kachedb.conf` |
| `-p` | `--port <PORT>` | TCP listening port | `6379` | `6379` |
| `-w` | `--workers <NUM>` | Number of worker threads (1 per CPU core) | Auto (all physical cores) | Equal to physical CPU cores |
| | `--pool-mb <MB>` | Megaslab memory pool allocated per core in megabytes | `64` | `256` or `1024` |
| | `--maxclients <NUM>` | Maximum simultaneous client connections | `10000` | `10000`–`65535` |
| | `--requirepass <PASS>` | Password required for client authentication via `AUTH` | None | Set strong password |
| | `--snapshot` / `--no-snapshot` | Enable or disable periodic binary snapshotting | `true` | `true` |
| | `--snapshot-file <PATH>` | Path to binary snapshot file (`dbfilename`) | `dump.kdb` | `/var/lib/kachedb/dump.kdb` |
| | `--snapshot-interval <SECS>` | Interval in seconds between snapshots | `300` | `300`–`900` |
| | `--snapshot-encryption` | Enable AES-256-GCM / ChaCha20 streaming encryption | `false` | `true` (for sensitive/KV data) |
| | `--snapshot-cipher <CIPHER>` | AEAD cipher (`aes-256-gcm` or `chacha20-poly1305`) | `aes-256-gcm` | `aes-256-gcm` |
| | `--snapshot-key <KEY>` | Secret passphrase or 64-character hex key | None | Passphrase or hex string |
| | `--snapshot-key-file <PATH>` | Path to 32-byte raw binary master key file | None | `/etc/kachedb/snapshot.key` |
| | `--aof <true\|false>` | Enable Append-Only File (AOF) persistence | `false` | `true` (if durability needed) |
| | `--aof-path <PATH>` | Path to Append-Only File log | `kachedb.aof` | `/var/lib/kachedb/kachedb.aof` |
| | `--appendfsync <POLICY>` | AOF disk sync policy (`always`, `everysec`, `no`) | `everysec` | `everysec` |
| | `--shm <true\|false>` | Enable POSIX Shared Memory (`/dev/shm`) IPC | `true` | `true` |
| | `--tls-cert <PATH>` | Path to TLS server certificate PEM file | None | Optional |
| | `--tls-key <PATH>` | Path to TLS private key PEM file | None | Optional |
| | `--tls-ca <PATH>` | Optional path to CA certificate for mTLS verification | None | Optional |

---

## 📄 Configuration File (`kachedb.conf`)

KacheDB supports declarative configuration using a canonical `kachedb.conf` file:

```text
# Network & Binding
bind 127.0.0.1
port 6379
maxclients 10000

# Worker Threads & Memory
workers 4
pool_mb_per_core 256

# IPC & LLM Tensor Streaming
shm yes

# Binary Snapshotting & Encryption-at-Rest
snapshot yes
dbfilename dump.kdb
snapshot-interval 300
snapshot-encryption yes
snapshot-encryption-cipher aes-256-gcm
# snapshot-encryption-key-file /etc/kachedb/snapshot.key
snapshot-encryption-key "your_secret_passphrase_here"

# Persistence (Append-Only File)
appendonly no
appendfilename kachedb.aof
appendfsync everysec

# Security
# requirepass my_secret_token
```

To launch `kachedb-server` using the configuration file:
```bash
./target/release/kachedb-server -c /path/to/kachedb.conf
```
*Note: Any explicit CLI flags will take precedence over directives inside the configuration file.*

---

## 🧵 Thread-per-Core Topology & CPU Pinning

KacheDB operates on a **shared-nothing, thread-per-core architecture**:
* Each active worker thread is pinned to a dedicated physical CPU core using `core_affinity`.
* **Zero Cross-Core Contention:** Each worker thread owns its private 2 MB Megaslab arena pool and independent Swiss Table shard.
* Request execution requires **no global mutex locks**, eliminating lock contention and cache-line bouncing.

### Example: Running on a Dedicated 8-Core Node
```bash
# Pin 8 workers to cores 0..7 with 512 MB memory per core (4 GB total)
./target/release/kachedb-server -p 6379 -w 8 --pool-mb 512
```

---

## 🧱 Memory Sizing & S3-FIFO Quota Management

Memory is managed through uniform **2 MB Megaslabs**:
* Rather than calling `malloc()` on every request, KacheDB pre-allocates contiguous megaslab page frames.
* **Per-Core Sizing:** If `--pool-mb` is set to `256` on a 4-core machine, total initial memory allocated across the daemon is $4 \times 256\text{ MB} = 1.024\text{ GB}$.
* **Elastic Borrowing:** The dynamic `WorkloadQuota` manager elastically allocates megaslabs between application key-value cache and tensor memory based on current demand.

---

## 🔐 Snapshot Encryption-at-Rest Configuration

KacheDB provides zero hot-path overhead streaming AEAD encryption for on-disk snapshots (`dump.kdb`).

### Configuration Directives & CLI Flags

| `kachedb.conf` Directive | CLI Flag Equivalent | Default | Description |
| :--- | :--- | :---: | :--- |
| `snapshot-encryption yes\|no` | `--snapshot-encryption`, `--no-snapshot-encryption` | `no` | Enable or disable authenticated streaming encryption for snapshots. |
| `snapshot-encryption-cipher <name>` | `--snapshot-cipher <cipher>` | `aes-256-gcm` | AEAD cipher suite: `aes-256-gcm` (hardware-accelerated) or `chacha20-poly1305`. |
| `snapshot-encryption-key-file <path>` | `--snapshot-key-file <path>` | *None* | Path to a 32-byte binary master key file (recommended for production). |
| `snapshot-encryption-key <string>` | `--snapshot-key <key>` | *None* | 64-character hexadecimal key or variable-length passphrase (derived via HKDF-SHA256). |

### Example CLI Launch
```bash
# Start daemon with hardware AES-256-GCM encryption using a binary key file
./target/release/kachedb-server -c kachedb.conf \
  --snapshot-encryption \
  --snapshot-cipher aes-256-gcm \
  --snapshot-key-file /etc/kachedb/snapshot.key
```

For the comprehensive wire format (`KDB\x03`) and security architecture, see [Snapshot Encryption-at-Rest](../architecture/snapshot-encryption.md).

---

## 🐧 Linux Kernel & `io_uring` Tuning

For maximum throughput on Linux (> 2.5M QPS), apply the following kernel optimizations:

### 1. `somaxconn` & TCP Backlog
```bash
sudo sysctl -w net.core.somaxconn=65535
sudo sysctl -w net.ipv4.tcp_max_syn_backlog=65535
```

### 2. POSIX Shared Memory Limits (`/dev/shm`)
Ensure `/dev/shm` has sufficient space for high-volume LLM KV-cache offloading:
```bash
# Verify current /dev/shm size
df -h /dev/shm

# Remount /dev/shm with 32 GB (if serving 70B+ LLM inference nodes)
sudo mount -o remount,size=32G /dev/shm
```

### 3. File Descriptor Limits
```bash
ulimit -n 1048576
```

---

## 🐳 Production Docker Configuration

Here is the recommended production `docker-compose.yml`:

```yaml
services:
  kachedb:
    image: ghcr.io/vubon/kachedb:latest
    container_name: kachedb
    privileged: true
    ipc: host
    network_mode: host
    restart: always
    command: ["-p", "6379", "-w", "4", "--pool-mb", "256"]
    ulimits:
      nofile:
        soft: 1048576
        hard: 1048576
      memlock:
        soft: -1
        hard: -1
```
