# 🤖 Antigravity IDE Integration: Autonomous Agent Memory

**KacheDB** functions as a high-speed, sub-microsecond persistent memory and semantic context engine for AI coding agents in **Google Antigravity IDE**.

By pairing KacheDB with Antigravity IDE via the Model Context Protocol (**MCP**), coding agents eliminate redundant file reads, preserve architecture decisions across sessions, and recall codebase solutions in **< 50 nanoseconds** without spending LLM tokens.

---

## ⚡ Why Agent Memory Matters

Modern AI IDE agents operate with bounded context windows and ephemeral memory:
* **The Problem:** On every user prompt or debugging session, an agent typically re-reads dozens of source files, re-indexes directory structures, and re-reasons through architecture rules from scratch. This burns thousands of input tokens and introduces multi-second latency.
* **The KacheDB Solution:** KacheDB acts as an out-of-band **L1/L2 memory hierarchy** for the agent:
  * **L1 Exact Match (SwissTable):** Repeated questions or known tasks resolve via prompt alias shortcuts in **< 50 ns** with **0 ms** LLM reasoning overhead.
  * **L2 Semantic Recall (SIMD Vector Engine):** Conceptual queries execute SIMD dot-product scans in **~150 µs**, filtered by 64-bit tag bitmasks.
  * **L3 Unabridged Parents (Hierarchical Storage):** Large explanations (>1,500 chars) are automatically chunked with breadcrumb headers while maintaining sub-microsecond pointers to their parent context document.

```text
[ User Prompt in Antigravity ]
              │
              ▼
   ┌──────────────────────┐
   │  Antigravity Agent   │
   └──────────┬───────────┘
              │ (1. Pre-Execution Cache Check via MCP)
              ▼
   ┌────────────────────────────────────────────────────────┐
   │                    KacheDB Core                        │
   │  ┌─────────────────────────┐  ┌─────────────────────┐  │
   │  │   SwissTable (Exact)    │  │  SIMD Vector Index  │  │
   │  │       < 50 ns Hit       │  │     150 µs Recall   │  │
   │  └─────────────────────────┘  └─────────────────────┘  │
   └──────────┬─────────────────────────────────────────────┘
              │
      ┌───────┴────────┐
[ Cache Hit ]    [ Cache Miss ]
      │                │
      ▼                ▼
(Instant Answer)  (Agent Researches & Implements)
                       │
                       ▼ (2. Post-Execution Knowledge Persistence)
                  [ kache_save_context ] ──► Stored for all future prompts
```

---

## 🛠️ Step 1: Configure the MCP Server

KacheDB provides an official Model Context Protocol server: **`kachedb-mcp`**.

### 1. Install `kachedb-mcp`
Ensure Python 3.10+ is installed with `uv` or `pip`:

```bash
# Run directly via uvx (recommended)
uvx kachedb-mcp

# Or install in your virtual environment
pip install kachedb-mcp
```

### 2. Register in Antigravity IDE MCP Config
Add KacheDB to your Antigravity IDE MCP configuration file (`mcp_config.json`):

```json
{
  "mcpServers": {
    "kachedb": {
      "command": "uvx",
      "args": ["--refresh", "kachedb-mcp"],
      "env": {
        "KACHEDB_HOST": "127.0.0.1",
        "KACHEDB_PORT": "6379",
        "KACHEDB_AUTO_CHUNK": "true"
      }
    }
  }
}
```

The server exposes 8 autonomous memory tools:
1. `kache_semantic_search`: Hybrid exact-symbol + SIMD vector retrieval with adaptive fallback.
2. `kache_save_context`: Saves agent knowledge with hierarchical chunking, 64-bit tag bitmasks, and exact prompt aliases.
3. `kache_get`: Sub-microsecond exact key-value read from SwissTable.
4. `kache_set`: Fast write into SwissTable memory pool.
5. `kache_get_parent_document`: Retrieves full unabridged parent documents for chunk matches.
6. `kache_delete`: Reclaims key and slab memory.
7. `kache_stats`: Reports active memory, slabs, vectors, and server health.
8. `kache_telemetry`: Reports real-time cache hits, vector hits, exact hits, tag filter skips, and LLM inference time saved.

---

## 📋 Step 2: Set Up Antigravity Agent Rules

Antigravity IDE discovers and executes agent rules located in `.agents/AGENTS.md` (or `.agents/rules/*.md`) in your workspace root.

Create `.agents/AGENTS.md` with the following production-tested template:

```markdown
# AGENTS.md — KacheDB Autonomous Caching & Memory Rules
# =========================================================
# These rules apply to ALL AI agents working inside this workspace.

## 🔴 MANDATORY PRE-EXECUTION CACHE CHECK (RULE 1)

**Before doing ANY extensive file reading, research, or answering architecture questions:**
1. Call `kache_semantic_search` or `kache_get` as your FIRST step:
   ```json
   {
     "query": "<user_query_or_topic>",
     "exact_first": true,
     "resolve_parent": true
   }
   ```
2. If relevant cached knowledge exists:
   - **USE THE CACHED CONTEXT DIRECTLY.**
   - Do NOT re-read dozens of source files or repeat costly reasoning for already-solved problems.
3. If no cache exists, proceed to research and remember to persist your solution in Step 2.

---

## 🟢 MANDATORY POST-EXECUTION KNOWLEDGE PERSISTENCE (RULE 2)

**Whenever you complete one of the following:**
- Answer a technical explanation or architecture question
- Solve a non-trivial bug or build failure
- Add or modify a major feature or API component
- Run benchmarks or performance measurements
- Define or update an architecture decision (ADR)

**Call `kache_save_context` in the SAME turn:**
```json
{
  "topic": "<topic_slug>",
  "prompt_alias": "<user_exact_query>",
  "content": "<full_answer_or_summary>",
  "tags": ["<tag1>", "<tag2>"],
  "auto_chunk": true
}
```
- Always supply `prompt_alias="<user_exact_query>"`: This registers a sub-microsecond exact shortcut in SwissTable so identical prompts return in < 50 ns with 0 ms LLM time.
- Large documents (> 1,500 chars) are automatically chunked with breadcrumb headers and linked to their parent document.

---

## 📊 REAL-TIME TELEMETRY & HEALTH CHECKS

- Verify cache status and token savings using `kache_stats()` and `kache_telemetry()`.
- Ensure connection stays healthy on `127.0.0.1:6379`.
```

---

## 🚀 Step 3: Verify the Autonomous Loop

Once configured, verify the workflow in Antigravity IDE:

1. **Ask a complex architecture question:**
   > *"How does snapshot encryption-at-rest work in KacheDB?"*
   - The agent notices no cache exists, researches the source code, and writes a detailed explanation.
   - At the end of the turn, the agent automatically executes:
     `kache_save_context(topic="snapshot_encryption", prompt_alias="How does snapshot encryption-at-rest work in KacheDB?", content=..., tags=["encryption", "snapshot", "architecture"])`.

2. **Ask the same or related question in a fresh chat session:**
   > *"How does snapshot encryption-at-rest work in KacheDB?"*
   - The agent immediately calls `kache_semantic_search` or `kache_get`.
   - **Result:** SwissTable returns an exact shortcut hit in **< 50 ns**! The agent provides the answer instantly without touching a single file on disk.

3. **Check Telemetry:**
   Run `kache_telemetry()` or check through the MCP inspector:
   ```json
   {
     "total_requests": 14,
     "cache_hits": 9,
     "exact_hits": 6,
     "vector_hits": 3,
     "tag_filter_skips": 42,
     "estimated_tokens_saved": 48500,
     "estimated_usd_saved": "$0.145",
     "avoided_embedding_ms": 3200
   }
   ```

---

## 🔮 Future IDE Support

Support and templates for **Cursor** (`.cursor/rules/`), **Claude Code** (`CLAUDE.md`), and **Windsurf** (`.windsurfrules`) are currently in active testing and will be published in upcoming documentation updates.
