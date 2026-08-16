//! `kachedb-shm` — Zero-copy POSIX shared memory IPC for LLM tensor transfer.
//!
//! Implements the **Zero-Copy IPC Transport** layer from the KacheDB blueprint:
//!
//! > _"For co-located GPU inference nodes, KacheDB communicates with Python
//! > inference workers (vLLM/SGLang) via POSIX Shared Memory (`/dev/shm`) and
//! > lock-free SPSC ring buffers. Bypasses the network stack entirely, streaming
//! > tensors at PCIe transfer speeds (~50+ GB/s)."_
//!
//! # Architecture
//!
//! ```text
//! KacheDB Daemon (Rust)                    vLLM/SGLang Worker (Python)
//! ─────────────────────                    ──────────────────────────
//! ShmChannel::create("kachedb_0", 256)     mmap("/dev/shm/kachedb_0")
//! push(IpcSlot { desc, slab_id })  ──────► read ShmRingHeader.head
//!                                          slot = ring[tail % cap]
//!                                          ptr = slab_base + offset
//!                                          t = torch.frombuffer(ptr, dtype=bf16)
//! ```
//!
//! # Modules
//!
//! - **[`region`]** — `ShmRegion`: cross-platform `mmap` / `shm_open` wrapper.
//! - **[`ring`]** — `ShmRingHeader`: 3-cache-line SPSC control state.
//! - **[`channel`]** — `ShmChannel`: high-level producer/consumer API with
//!   adaptive spin-then-park synchronization.
//! - **[`error`]** — `ShmError` unified error type.

pub mod channel;
pub mod error;
pub mod region;
pub mod ring;

pub use channel::{IpcSlot, ShmChannel};
pub use error::ShmError;
pub use region::ShmRegion;
pub use ring::{ConsumerState, ShmRingHeader};
