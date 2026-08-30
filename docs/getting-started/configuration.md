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
| `-p` | `--port <PORT>` | TCP listening port | `6379` | `6379` |
| `-w` | `--workers <NUM>` | Number of worker threads (1 per CPU core) | Auto (all physical cores) | Equal to physical CPU cores |
| | `--pool-mb <MB>` | Memory pool size allocated per core in megabytes | `64` | `256` or `1024` |
| | `--shm-name <NAME>` | Prefix name for POSIX Shared Memory regions | `kachedb` | `kachedb` |
| | `--legacy-reuseport` | Use legacy kernel `SO_REUSEPORT` instead of accept-dispatch | `false` | `false` (Accept-Dispatch is faster) |

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
