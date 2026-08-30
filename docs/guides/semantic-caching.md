# 💬 Semantic Caching Guide

This guide explains how to use **KacheDB's Semantic Cache Engine** to intercept and cache LLM completions based on semantic intent and cosine similarity, saving 100% of GPU compute and token costs on semantic cache hits.

---

## ⚡ How Semantic Caching Works

Traditional caching requires an exact character-for-character string match (`"What is KacheDB?"` vs `"what is kachedb?"`).

KacheDB's `SemanticCache` computes a dense vector embedding for incoming prompts and searches the in-memory SIMD vector index using normalized Cosine Similarity:
* **Cache HIT:** If similarity $\ge \text{threshold}$ (default `0.85`), KacheDB immediately returns the cached LLM answer in **$< 50\ \mu\text{s}$** without invoking the LLM.
* **Cache MISS:** The application queries the LLM and writes the answer to KacheDB for future semantic matches.

---

## 🚀 Synchronous Usage (`SemanticCache`)

```python
from kachedb import KacheClient, SemanticCache

# 1. Connect to KacheDB
client = KacheClient(host="127.0.0.1", port=6379)

# 2. Initialize the semantic cache (auto-detects FastEmbed or SentenceTransformers)
cache = SemanticCache(
    client=client,
    index_name="customer_support_faq",
    similarity_threshold=0.85,
    ttl_seconds=86400,  # 24 hours
)

# 3. Store a Q&A pair in the semantic cache
cache.set(
    prompt="How do I change my billing address?",
    response="Go to Account Settings -> Billing -> Edit Address.",
)

# 4. Query with a semantically equivalent but differently worded prompt
query = "Where can I update my billing location?"
match = cache.get(query)

if match:
    print(f"🎯 Cache HIT! (Similarity: {match.similarity:.2f})")
    print(f"Response: {match.value}")
else:
    print("❌ Cache MISS")
```

---

## ⚡ Asynchronous Usage (`AsyncSemanticCache`)

For non-blocking `asyncio` inference servers (such as FastAPI, vLLM, or LiteLLM):

```python
import asyncio
from kachedb import AsyncKacheClient, AsyncSemanticCache

async def main():
    async with AsyncKacheClient(host="127.0.0.1", port=6379) as client:
        cache = AsyncSemanticCache(
            client=client,
            index_name="async_chat_cache",
            similarity_threshold=0.88,
        )

        # Store response
        await cache.set(
            prompt="Explain quantum entanglement briefly",
            response="Quantum entanglement is a physical phenomenon where particles remain connected so that actions performed on one affect the other.",
        )

        # Retrieve asynchronously
        result = await cache.get("What is quantum entanglement in simple terms?")
        if result:
            print(f"⚡ Async Hit: {result.value}")

asyncio.run(main())
```

---

## 🔌 Pluggable Embedding Backends

KacheDB supports multiple embedding backends via `kachedb.semantic.embedders`:

* **`FastEmbedAdapter`**: Ultra-fast local ONNX Runtime embeddings (`pip install fastembed`).
* **`SentenceTransformersAdapter`**: HuggingFace `sentence-transformers` models.
* **`OpenAIAdapter`**: Remote OpenAI embedding API (`text-embedding-3-small`).
* **`CallableAdapter`**: Custom function wrapping any embedding model (`Callable[[str], list[float]]`).
