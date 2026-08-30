# 🤖 vLLM Integration Guide

This guide walks through configuring **KacheDB** as a high-speed external KV-cache offloading tier for **vLLM** (PagedAttention).

---

## ⚡ Overview

By offloading PagedAttention KV-cache blocks from GPU VRAM to KacheDB's zero-copy POSIX Shared Memory (`/dev/shm`), vLLM inference instances achieve:
* **Up to 10,000× faster prompt prefill** for repeated system prompts, few-shot examples, and multi-turn chat sessions.
* **Zero socket serialization overhead** using PyTorch tensor memory views.
* Seamless multi-GPU and distributed tensor parallel support.

---

## 📦 Installation & Setup

1. Start the KacheDB daemon with `--ipc host` or native POSIX Shared Memory enabled:
```bash
./target/release/kachedb-server -p 6379 -w 4 --pool-mb 512
```

2. Install the KacheDB Python client with PyTorch support:
```bash
pip install kachedb[torch]
```

---

## 🚀 Programmatic Integration Example

The `KacheDBConnector` handles prefix caching, block hashing, and zero-copy restoration automatically:

```python
import torch
from kachedb.vllm import KacheDBConnector

# 1. Initialize the connector for the active GPU worker rank
connector = KacheDBConnector(
    rank=0,
    local_rank=0,
    block_size=16,
    pool_size_mb=256,
)

# 2. Simulated PagedAttention tensor for 2 transformer layers
# Shape: [num_blocks=4, 2 (K/V), num_heads=8, block_size=16, head_dim=64]
kv_shape = (4, 2, 8, 16, 64)
kv_caches = [
    torch.randn(kv_shape, dtype=torch.float16),
    torch.randn(kv_shape, dtype=torch.float16),
]

# 3. Offload KV cache blocks to KacheDB
prompt_tokens = [101, 2054, 2003, 1037, 2742, 102]
connector.offload_kv_cache(
    prompt_tokens=prompt_tokens,
    kv_caches=kv_caches,
)

# 4. On subsequent requests, restore matching prefix blocks
new_request_tokens = [*prompt_tokens, 999, 1000]
target_kv_caches = [
    torch.zeros(kv_shape, dtype=torch.float16),
    torch.zeros(kv_shape, dtype=torch.float16),
]

matched_tokens, is_hit = connector.restore_kv_cache(
    prompt_tokens=new_request_tokens,
    target_kv_caches=target_kv_caches,
)

if is_hit:
    print(f"✅ Cache Hit! Restored {matched_tokens} tokens from KacheDB zero-copy.")
```
