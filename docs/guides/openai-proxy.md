# OpenAI Semantic Reverse Proxy (`kachedb-proxy`)

The **`kachedb-proxy`** service is a lightweight, standalone reverse proxy that bridges AI developer tools (**Cursor**, **Aider**, **Continue.dev**, **LiteLLM**, **LangChain**, and **OpenAI SDKs**) with **KacheDB**'s in-memory SIMD vector cache.

---

## 🏗️ Architecture & Packet Flow

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CLIENT: Cursor / Aider / OpenAI SDK                      │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │ POST /v1/chat/completions
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                   KACHEDB-PROXY (localhost:8080)                            │
│  1. Extract messages & canonicalize prompt text                             │
│  2. Compute local 384-dim embedding via FastEmbed/ONNX in 2-4 ms ($0 cost)  │
│  3. SIMD vector query to KacheDB daemon (127.0.0.1:6379, < 0.3 ms)          │
└───────────────────────┬─────────────────────────────┬───────────────────────┘
                        │                             │
              [ SEMANTIC HIT (>= 0.85) ]      [ SEMANTIC MISS ]
                        │                             │
                        ▼                             ▼
        ⚡ Return Cached Response (JSON/SSE)    🌐 Forward to Upstream LLM
        - Latency: < 5 ms                      - Pass client auth headers
        - Upstream Cost: $0.00                 - Stream chunks to client
        - Header: X-KacheDB-Cache: HIT         - Async VADD to KacheDB
```

---

## 🚀 Quickstart

### 1. Start KacheDB Server
```bash
./target/release/kachedb-server -p 6379
```

### 2. Launch `kachedb-proxy`
```bash
cargo run --release --manifest-path kachedb-proxy/Cargo.toml -- \
  --port 8080 \
  --upstream https://api.openai.com/v1 \
  --kachedb-port 6379
```

### 3. Connect Cursor / IDE
Set **OpenAI Base URL** in Cursor settings:
```text
http://localhost:8080/v1
```

---

## ⚙️ Configuration Reference

| Parameter | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `--port` | `KACHEDB_PROXY_PORT` | `8080` | Local HTTP listening port |
| `--upstream` | `UPSTREAM_BASE_URL` | `https://api.openai.com/v1` | Target LLM endpoint |
| `--kachedb-host` | `KACHEDB_HOST` | `127.0.0.1` | KacheDB daemon IP |
| `--kachedb-port` | `KACHEDB_PORT` | `6379` | KacheDB daemon port |
| `--threshold` | `SIMILARITY_THRESHOLD` | `0.85` | Cosine similarity threshold (0.0–1.0) |
| `--ttl` | `CACHE_TTL_SECONDS` | `86400` | Expiration time for cached responses (seconds) |
| `--embedding-model` | `EMBEDDING_MODEL` | `bge-small-en-v1.5` | On-device ONNX embedding model |
