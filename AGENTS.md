# AGENTS.md — Development & Pre-Commit Rules for KacheDB
# =========================================================

## Mandatory Pre-Commit Gate

Before EVERY `git commit`, the following verification gate MUST be executed and pass with 0 errors and 0 warnings:

```bash
# 1. Format verification
cargo fmt --all -- --check

# 2. Strict linter check
cargo clippy --workspace --all-targets --all-features -- -D warnings

# 3. Full test suite verification
cargo test --workspace
```

### Rules:
1. **Never commit without running `cargo fmt --all -- --check` first.** If formatting fails, run `cargo fmt --all` to fix formatting before committing.
2. **Zero Clippy Warnings:** All code must compile cleanly under `-D warnings`.
3. **All Tests Passing:** All unit, integration, and doc tests across all crates must pass.
4. **Never Commit Secrets:** Do not track `.env` or sensitive credentials.
5. **Ask Before Committing:** Only create git commits when explicitly requested or approved by the user.
