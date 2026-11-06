//! `kachedb-net` — Shutdown synchronization barrier for zero-data-loss persistence.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// Synchronization barrier holding worker threads alive on shutdown
/// so their thread-local Megaslab memory pools remain registered and valid
/// during final snapshot serialization.
pub struct ShutdownBarrier {
    workers_stopped: AtomicUsize,
    released: AtomicBool,
    lock: Mutex<()>,
    condvar: Condvar,
}

impl Default for ShutdownBarrier {
    fn default() -> Self {
        Self::new()
    }
}

impl ShutdownBarrier {
    /// Creates a new `ShutdownBarrier`.
    pub fn new() -> Self {
        Self {
            workers_stopped: AtomicUsize::new(0),
            released: AtomicBool::new(false),
            lock: Mutex::new(()),
            condvar: Condvar::new(),
        }
    }

    /// Called by a worker thread when its event loop has stopped and connections are closed.
    /// The worker pauses here with its `SlabPool` intact until `release()` is called.
    pub fn wait_worker(&self) {
        self.workers_stopped.fetch_add(1, Ordering::SeqCst);
        let guard = self.lock.lock().unwrap();
        drop(
            self.condvar
                .wait_while(guard, |_| !self.released.load(Ordering::SeqCst))
                .unwrap(),
        );
    }

    /// Called by the coordinator (e.g. main thread) to wait until `expected_workers`
    /// have paused their event loops.
    pub fn wait_all_stopped(&self, expected_workers: usize, timeout: Duration) -> bool {
        let start = Instant::now();
        while self.workers_stopped.load(Ordering::SeqCst) < expected_workers {
            if start.elapsed() >= timeout {
                return false;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        true
    }

    /// Releases all waiting workers to allow their thread functions to terminate
    /// and their thread-local memory pools to be deallocated.
    pub fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        let _guard = self.lock.lock().unwrap();
        self.condvar.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_shutdown_barrier_coordination() {
        let barrier = Arc::new(ShutdownBarrier::new());
        let num_workers = 4;
        let mut handles = Vec::new();

        for _ in 0..num_workers {
            let b = barrier.clone();
            handles.push(thread::spawn(move || {
                // Simulate work
                thread::sleep(Duration::from_millis(10));
                b.wait_worker();
            }));
        }

        assert!(barrier.wait_all_stopped(num_workers, Duration::from_secs(1)));
        assert_eq!(barrier.workers_stopped.load(Ordering::SeqCst), num_workers);

        // Workers should still be blocked
        barrier.release();

        for h in handles {
            h.join().unwrap();
        }
    }
}
