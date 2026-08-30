# 🧠 SIMD Semantic Vector Commands

KacheDB features a hardware-accelerated **SIMD Vector Search Engine** (`kachedb-vector`) built directly into the storage core. It enables sub-microsecond nearest-neighbor vector lookups, semantic caching, and LLM prompt similarity matching with zero external dependencies.

---

## ⚡ Hardware SIMD Acceleration

* **ARM NEON (`aarch64`):** 128-bit `vfmaq_f32` with 4-way loop unrolling (16 floats per loop iteration) delivering $< 120\text{ ns}$ dot products on Apple Silicon and AWS Graviton.
* **x86_64 AVX2 / FMA:** 256-bit `_mm256_fmadd_ps` with 4-way loop unrolling (32 floats per iteration) delivering $> 40\text{ GB/s}$ throughput.
* **Normalized Cosine Similarity:** All stored vectors are automatically $L_2$-normalized upon ingestion, transforming cosine distance calculation into a single high-speed inner dot product:
$$\text{CosineSimilarity}(\vec{u}, \vec{v}) = \sum_{i=1}^{D} u_i \cdot v_i$$

---

## 📋 Command Summary

| Command | Syntax | Complexity | Description |
| :--- | :--- | :---: | :--- |
| **`VADD`** | `VADD index id dim vector_bytes [PAYLOAD text] [EX sec]` | $\mathcal{O}(D)$ | Ingests vector embedding into named index with optional payload and TTL. |
| **`VSEARCH`** | `VSEARCH index query_bytes [TOPK k] [THRESHOLD min_score]` | $\mathcal{O}(N \cdot D)$ | Nearest-neighbor cosine search returning matched IDs, scores, and payloads. |
| **`VDEL`** | `VDEL index id` | $\mathcal{O}(1)$ | Deletes vector from named index. |
| **`VSTATS`** | `VSTATS index` | $\mathcal{O}(1)$ | Returns index dimension, active vector count, and memory consumption. |

---

## 🛠️ Detailed Command Reference & Examples

### `VADD`
Ingests a single float32 vector into a named index. The vector is provided as raw little-endian IEEE 754 float32 byte buffers.

#### Syntax
```text
VADD <index> <id> <dim> <vector_bytes> [PAYLOAD <payload>] [EX <seconds>]
```

#### `kachedb-cli` Example
```text
# Insert a 4-dimensional vector with payload and 1-hour expiration
127.0.0.1:6379> VADD faq q:101 4 "\x00\x00\x80?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" PAYLOAD "To reset password, go to Settings -> Security." EX 3600
OK
```

---

### `VSEARCH`
Performs nearest-neighbor cosine similarity search across all vectors in the index.

#### Syntax
```text
VSEARCH <index> <query_bytes> [TOPK <k>] [THRESHOLD <min_similarity>]
```

#### `kachedb-cli` Example
```text
# Search index 'faq' for top 1 match with similarity >= 0.80
127.0.0.1:6379> VSEARCH faq "\x00\x00\x80?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" TOPK 1 THRESHOLD 0.80
1) 1) "q:101"
   2) "1.000000"
   3) "To reset password, go to Settings -> Security."
```

---

### `VDEL` & `VSTATS`
Index management and telemetry.

#### `kachedb-cli` Example
```text
127.0.0.1:6379> VSTATS faq
1) "dimension"
2) (integer) 4
3) "total_vectors"
4) (integer) 1
5) "active_vectors"
6) (integer) 1
7) "memory_bytes"
8) (integer) 64

127.0.0.1:6379> VDEL faq q:101
(integer) 1
```

---

## 🐍 Python SDK (`kachedb-py`) Example

Using vectors is seamless via `kachedb-py`:

```python
from kachedb import KacheClient

with KacheClient() as client:
    # 1. Ingest vector embedding (automatically packs float lists to IEEE-754 bytes)
    embedding = [0.12, -0.45, 0.88, 0.05]
    client.vadd(
        index="products",
        item_id="item:1001",
        vector=embedding,
        payload="Ergonomic Mechanical Keyboard",
        ex=86400,
    )

    # 2. Query nearest vectors
    query_vector = [0.10, -0.40, 0.85, 0.04]
    results = client.vsearch(
        index="products",
        query_vector=query_vector,
        top_k=3,
        threshold=0.85,
    )

    for item_id, score, payload in results:
        print(f"Matched {item_id} (Score: {score:.4f}): {payload}")
```
