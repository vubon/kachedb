# 🌲 SGLang RadixAttention Integration Guide

This guide describes integrating **KacheDB** with **SGLang**'s RadixAttention hierarchical tree-branching engine.

---

## ⚡ Overview

SGLang manages KV-caches as a dynamic Radix Tree across multiple conversational branches and tool calls. KacheDB's `KacheDBSGLangConnector` maps SGLang tree nodes directly to lock-free memory frames:
* **Arbitrary Slice Lengths:** Variable-length token chunks hashed via chained Blake2b.
* **Hierarchical Multi-Branch Restoration:** Restores branched tree paths in a single pass.
* **Multi-Precision Support:** Native support for `FP16`, `BF16` (LLaMA default), `FP32`, and `INT8`.

---

## 🚀 Programmatic Integration Example

```python
import torch
from kachedb.sglang import KacheDBSGLangConnector

# 1. Initialize connector
connector = KacheDBSGLangConnector(
    rank=0,
    local_rank=0,
    pool_size_mb=256,
)

num_heads = 4
head_dim = 64
num_layers = 2
dtype = torch.bfloat16

# 2. Node 1: Root System Prompt (10 tokens)
root_tokens = list(range(10))
k_root = [torch.randn((num_heads, 10, head_dim), dtype=dtype) for _ in range(num_layers)]
v_root = [torch.randn((num_heads, 10, head_dim), dtype=dtype) for _ in range(num_layers)]

desc_root = connector.offload_node(
    node_id=1,
    token_ids=root_tokens,
    k_tensors=k_root,
    v_tensors=v_root,
    parent_hash=0,
)

# 3. Node 2: Child Branch Turn (20 tokens)
child_tokens = list(range(10, 30))
k_child = [torch.randn((num_heads, 20, head_dim), dtype=dtype) for _ in range(num_layers)]
v_child = [torch.randn((num_heads, 20, head_dim), dtype=dtype) for _ in range(num_layers)]

connector.offload_node(
    node_id=2,
    token_ids=child_tokens,
    k_tensors=k_child,
    v_tensors=v_child,
    parent_hash=desc_root.node_hash,
)

# 4. Restore Full 30-Token Sequence (Root + Child)
full_prompt = list(range(30))
target_k = [torch.zeros((num_heads, 30, head_dim), dtype=dtype) for _ in range(num_layers)]
target_v = [torch.zeros((num_heads, 30, head_dim), dtype=dtype) for _ in range(num_layers)]

matched_count, is_hit = connector.restore_prefix(
    prompt_tokens=full_prompt,
    target_k_buffers=target_k,
    target_v_buffers=target_v,
)

if is_hit:
    print(f"🌲 SGLang Restored {matched_count} tokens across Radix tree hierarchy!")
```
