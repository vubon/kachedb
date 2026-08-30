# 🌳 Token Radix Prefix Tree

The `kachedb-radix` subsystem implements a hierarchical token prefix tree optimized for LLM attention prompt prefill reuse (such as **vLLM PagedAttention** and **SGLang RadixAttention**).

---

## ⚡ The LLM Prefill Problem

During LLM inference, requests operate in two phases:
1. **Prefill Phase:** Computes the Key and Value ($K, V$) attention matrices across all layers for the prompt tokens. Computational complexity is $\mathcal{O}(N^2)$ with respect to sequence length.
2. **Decode Phase:** Autoregressively generates tokens one by one using previously computed $K, V$ states.

For long multi-turn prompts (e.g. system prompts, few-shot examples, large codebases, 16K–128K tokens):
* Recomputing a 32K token prefill on an NVIDIA H100 GPU takes **$200\text{--}500\text{ ms}$**.
* Restoring precomputed KV tensors from KacheDB takes **$< 20\text{ ms}$**.
* **Result:** Offloading attention states cuts Time-To-First-Token (TTFT) by **5× to 15×**.

---

## 🌲 Tree Topology & Chunked Edges

```text
               [ Root (Parent Hash = 0) ]
                            │
               ┌────────────┴────────────┐
               ▼                         ▼
      "You are an assistant"   "System Prompt: Coding"
       [Chunk: 16 Tokens]        [Chunk: 16 Tokens]
               │                         │
       ┌───────┴───────┐                 │
       ▼               ▼                 ▼
  "Turn 1: Python" "Turn 1: Rust"  "User Prompt: Refactor"
   [Block ID 4]     [Block ID 9]     [Block ID 12]
```

### 1. Compressed Edge Hops (`[u32; 16]`)
* Rather than storing 1 token per node hop, KacheDB edges compress **16 tokens per block**.
* Traversal complexity drops from $\mathcal{O}(L)$ to $\mathcal{O}(L / 16)$.
* A 1,024-token prompt prefix matches in **$2.45\ \mu\text{s}$** ($> 10,000\times$ faster than GPU recomputation).

### 2. Epoch-Based RCU Concurrency (`EpochTree`)
* Multi-reader concurrency is managed via lock-free Read-Copy-Update (RCU) snapshots using `arc-swap`.
* Reader inference threads access snapshots with **$\approx 1\text{ ns}$** atomic load overhead without locking writers.

### 3. Reference Counting & Pinning
* Active attention decoders pin tree nodes (`ref_count++`), ensuring active conversation blocks are never evicted while a request is in flight.
* Eviction uses **Bottom-Up Hierarchical LRU**, pruning leaf turns while protecting shared root system prompts.
