## Description

<!-- Brief summary of what this pull request changes and why. Link any related issues with `#issue_number`. -->

Fixes # / Related to #

---

## Type of Change

- [ ] 🐛 Bug fix (non-breaking change which fixes an issue)
- [ ] ⚡ Performance improvement / optimization
- [ ] 🚀 New feature (non-breaking change adding functionality)
- [ ] 💥 Breaking change (fix or feature causing existing functionality to change)
- [ ] 📖 Documentation update or RFC addition
- [ ] 🧪 Tests or benchmark additions

---

## Systems & Performance Checklist

- [ ] **L1 Cache Line Invariant:** Any modified hot structs remain cache-line aligned (`#[repr(align(64))]`) without false sharing.
- [ ] **Memory Safety:** Every single `unsafe` block is preceded by an explicit `// SAFETY:` explanation.
- [ ] **Lock-Free Fast Path:** Hot read/lookup paths avoid mutexes, blocking CAS loops, or runtime OS syscalls.
- [ ] **Testing:** All workspace tests pass via `cargo test --workspace`.
- [ ] **Clippy & Formatting:** Zero lint warnings via `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --all -- --check`.
- [ ] **Benchmarks:** If this modifies a hot path, micro-benchmarks were run via `cargo bench --workspace` to confirm zero regression.

---

## Benchmark Results (if applicable)

```text
<!-- Paste Criterion or kachedb-bench comparison output here -->
```
