#!/usr/bin/env bash
# ==============================================================================
# KacheDB vs Redis 7 vs Valkey 8 vs DragonflyDB: Automated Benchmark Runner
# ==============================================================================
# Runs strictly SEQUENTIALLY (one database container at a time) to prevent
# resource contention on host CPU and RAM.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.benchmark.yml"
RESULTS_DIR="${REPO_ROOT}/docs/benchmarks"
RESULTS_FILE="${RESULTS_DIR}/benchmark_comparison_results.md"

mkdir -p "${RESULTS_DIR}"

CLIENTS=50
THREADS=4
REQUESTS=100000
PIPELINE=16
DATA_SIZE=64

TARGETS=("redis" "valkey" "dragonfly" "kachedb")

echo "======================================================================"
echo "🏎️  Starting In-Memory Cache Benchmark Suite (Sequential Mode)"
echo "   - Clients:    ${CLIENTS}"
echo "   - Threads:    ${THREADS}"
echo "   - Requests:   ${REQUESTS} per client"
echo "   - Pipeline:   ${PIPELINE}"
echo "   - Value Size: ${DATA_SIZE} bytes"
echo "   - Resource:   4 CPUs, 4 GB RAM per container"
echo "======================================================================"
echo ""

# Declare associative arrays for metrics (bash 4+) or standard log files
mkdir -p "${SCRIPT_DIR}/.bench_tmp"
TMP_DIR="${SCRIPT_DIR}/.bench_tmp"
rm -f "${TMP_DIR}"/*

wait_for_server() {
  local port=6379
  local max_retries=30
  local retry=0
  
  echo -n "   Waiting for server readiness on port ${port}..."
  while [ $retry -lt $max_retries ]; do
    if docker run --rm --network host redis:7.4-alpine redis-cli -h 127.0.0.1 -p ${port} ping 2>/dev/null | grep -q "PONG"; then
      echo " READY!"
      return 0
    fi
    sleep 1
    retry=$((retry + 1))
  done
  echo " FAILED (timeout)!"
  return 1
}

run_memtier() {
  local target=$1
  local mode=$2   # "SET", "GET", or "MIXED"
  local ratio="1:0"
  
  if [ "$mode" == "GET" ]; then
    ratio="0:1"
  elif [ "$mode" == "MIXED" ]; then
    ratio="1:4" # 20% SET, 80% GET
  fi

  echo "   ⚡ Running ${mode} benchmark..."
  
  # Run memtier_benchmark in host network mode
  docker run --rm --network host redislabs/memtier_benchmark:latest \
    --server=127.0.0.1 \
    --port=6379 \
    --protocol=redis \
    --clients="${CLIENTS}" \
    --threads="${THREADS}" \
    --requests="${REQUESTS}" \
    --pipeline="${PIPELINE}" \
    --data-size="${DATA_SIZE}" \
    --ratio="${ratio}" \
    --key-pattern=G:G \
    --distinct-client-seed \
    --hide-histogram 2>&1 | tee "${TMP_DIR}/${target}_${mode}.log"
}

# 1. Ensure KacheDB release container is built
echo "📦 Building KacheDB release image..."
docker compose -f "${COMPOSE_FILE}" build kachedb

# 2. Iterate through each engine sequentially
for target in "${TARGETS[@]}"; do
  echo ""
  echo "======================================================================"
  echo "🚀 Testing: [ ${target^^} ] (Isolated 4 Cores, 4GB RAM)"
  echo "======================================================================"

  # Clean any old container
  docker compose -f "${COMPOSE_FILE}" down -v 2>/dev/null || true
  sleep 2

  # Start single target container
  echo "   Starting container..."
  docker compose -f "${COMPOSE_FILE}" up -d "${target}"

  if ! wait_for_server; then
    echo "❌ Failed to start ${target}. Showing logs:"
    docker compose -f "${COMPOSE_FILE}" logs "${target}"
    docker compose -f "${COMPOSE_FILE}" down -v
    continue
  fi

  # Run Benchmarks
  run_memtier "${target}" "SET"
  sleep 2
  run_memtier "${target}" "GET"
  sleep 2
  run_memtier "${target}" "MIXED"
  sleep 1

  # Capture memory consumption
  MEM_USAGE=$(docker stats --no-stream --format "{{.MemUsage}}" "${target}-bench-target" 2>/dev/null || echo "N/A")
  echo "   📊 Final Memory Usage: ${MEM_USAGE}"
  echo "${MEM_USAGE}" > "${TMP_DIR}/${target}_mem.txt"

  # Stop and clean up container
  echo "   🛑 Tearing down container..."
  docker compose -f "${COMPOSE_FILE}" down -v
  sleep 3
done

# 3. Parse results and generate Scorecard Markdown
echo ""
echo "📝 Compiling benchmark scorecard..."

cat << 'EOF' > "${RESULTS_FILE}"
# 🏎️ In-Memory Storage Engine Benchmark Scorecard

**Benchmark Date:** $(date -u +"%Y-%m-%d %H:%M:%S UTC")  
**Environment:** Docker Linux (Isolated 4 CPUs, 4 GB RAM per container)  
**Load Generator:** `memtier_benchmark` (50 clients, 4 threads, 16 pipeline, 64-byte value)

---

## 📊 Comparative Performance Results

| Storage Engine | SET (Writes/sec) | GET (Reads/sec) | Mixed 80/20 (QPS) | Latency P50 (ms) | Latency P99 (ms) | Peak RAM (RSS) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
EOF

parse_metric() {
  local file=$1
  local metric=$2
  if [ -f "$file" ]; then
    grep -i "$metric" "$file" | awk '{print $(NF-1)}' | head -1 || echo "N/A"
  else
    echo "N/A"
  fi
}

for target in "${TARGETS[@]}"; do
  SET_QPS=$(grep -E "^Totals" "${TMP_DIR}/${target}_SET.log" 2>/dev/null | awk '{print $2}' || echo "N/A")
  GET_QPS=$(grep -E "^Totals" "${TMP_DIR}/${target}_GET.log" 2>/dev/null | awk '{print $2}' || echo "N/A")
  MIX_QPS=$(grep -E "^Totals" "${TMP_DIR}/${target}_MIXED.log" 2>/dev/null | awk '{print $2}' || echo "N/A")
  P50_LAT=$(grep -E "^Totals" "${TMP_DIR}/${target}_GET.log" 2>/dev/null | awk '{print $5}' || echo "N/A")
  P99_LAT=$(grep -E "^Totals" "${TMP_DIR}/${target}_GET.log" 2>/dev/null | awk '{print $6}' || echo "N/A")
  MEM=$(cat "${TMP_DIR}/${target}_mem.txt" 2>/dev/null || echo "N/A")

  echo "| **${target^^}** | ${SET_QPS} | ${GET_QPS} | ${MIX_QPS} | ${P50_LAT} | ${P99_LAT} | ${MEM} |" >> "${RESULTS_FILE}"
done

cat << 'EOF' >> "${RESULTS_FILE}"

---

## 🔬 Key Architectural Differences

1. **KacheDB (Rust):** Thread-per-core topology with SIMD Swiss Table, S3-FIFO eviction, and 2 MB Megaslab bump allocator (zero runtime heap jitter).
2. **DragonflyDB (C++):** Multi-threaded fiber pool per core with `dashtable` segment locking.
3. **Redis 7.4 (C):** Single-threaded execution core with multi-threaded socket I/O (`io-threads 4`) and `jemalloc`.
4. **Valkey 8.0 (C):** Open-source Linux Foundation engine based on Redis with multi-threaded I/O.
EOF

echo "======================================================================"
echo "✅ Benchmark Completed! Results saved to:"
echo "   ${RESULTS_FILE}"
echo "======================================================================"
cat "${RESULTS_FILE}"
