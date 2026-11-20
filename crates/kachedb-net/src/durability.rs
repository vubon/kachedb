//! Bounded AOF admission and ordered publication shared by server workers.

use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, bounded, unbounded};

use crate::aof_encode::MAX_AOF_RECORD_SIZE;

const QUEUE_BYTES: usize = 128 * 1024 * 1024;
const QUEUE_RECORDS: usize = 65_536;
const ADMISSION_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AppendFsync {
    Always,
    #[default]
    EverySec,
    No,
}

impl AppendFsync {
    pub fn parse(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("always") {
            Some(Self::Always)
        } else if value.eq_ignore_ascii_case("everysec") {
            Some(Self::EverySec)
        } else if value.eq_ignore_ascii_case("no") {
            Some(Self::No)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checkpoint {
    pub generation: [u8; 16],
    pub sequence: u64,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AdmissionError {
    #[error("ERR persistence queue saturated")]
    Saturated,
    #[error("ERR persistence writer unavailable")]
    Unavailable,
    #[error("ERR AOF record exceeds 64 MiB")]
    Oversized,
    #[error("ERR persistence failed after publication; mutation outcome is uncertain")]
    Uncertain,
}

pub enum WriterRequest {
    Record {
        bytes: Vec<u8>,
        done: Option<Sender<()>>,
        permit: Permit,
    },
    Sync(Sender<()>),
    Stop,
}

struct QueueState {
    used_bytes: usize,
    used_records: usize,
    failed: bool,
    sequence: u64,
}

pub struct AofDispatch {
    tx: Sender<WriterRequest>,
    state: Mutex<QueueState>,
    available: Condvar,
    generation: [u8; 16],
    policy: AppendFsync,
    byte_limit: usize,
    record_limit: usize,
}

static DISPATCH: OnceLock<Arc<AofDispatch>> = OnceLock::new();
static COMMIT: Mutex<()> = Mutex::new(());
static SNAPSHOTS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub fn set_dispatch(dispatch: Arc<AofDispatch>) {
    assert!(
        DISPATCH.set(dispatch).is_ok(),
        "AOF dispatch already installed"
    );
}

pub fn dispatch() -> Option<&'static Arc<AofDispatch>> {
    DISPATCH.get()
}

pub fn enable_snapshot_fence() {
    SNAPSHOTS.store(true, std::sync::atomic::Ordering::Release);
}

pub(crate) fn needs_commit_fence() -> bool {
    dispatch().is_some() || SNAPSHOTS.load(std::sync::atomic::Ordering::Acquire)
}

/// Holds the same fence as live mutations while taking a consistent memory view.
pub fn with_commit_fence<R>(f: impl FnOnce() -> R) -> R {
    let _guard = COMMIT.lock().unwrap();
    f()
}

impl AofDispatch {
    pub fn new(
        checkpoint: Checkpoint,
        policy: AppendFsync,
    ) -> (Arc<Self>, Receiver<WriterRequest>) {
        Self::with_limits(checkpoint, policy, QUEUE_BYTES, QUEUE_RECORDS)
    }

    fn with_limits(
        checkpoint: Checkpoint,
        policy: AppendFsync,
        byte_limit: usize,
        record_limit: usize,
    ) -> (Arc<Self>, Receiver<WriterRequest>) {
        let (tx, rx) = unbounded();
        let dispatch = Arc::new(Self {
            tx,
            available: Condvar::new(),
            generation: checkpoint.generation,
            policy,
            byte_limit,
            record_limit,
            state: Mutex::new(QueueState {
                used_bytes: 0,
                used_records: 0,
                failed: false,
                sequence: checkpoint.sequence,
            }),
        });
        (dispatch, rx)
    }

    pub fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Permit, AdmissionError> {
        if bytes > MAX_AOF_RECORD_SIZE || bytes > self.byte_limit {
            return Err(AdmissionError::Oversized);
        }
        let deadline = Instant::now() + ADMISSION_TIMEOUT;
        let mut state = self.state.lock().unwrap();
        loop {
            if state.failed {
                return Err(AdmissionError::Unavailable);
            }
            if state.used_bytes <= self.byte_limit - bytes && state.used_records < self.record_limit
            {
                state.used_bytes += bytes;
                state.used_records += 1;
                return Ok(Permit {
                    dispatch: self.clone(),
                    bytes,
                });
            }
            if self.policy != AppendFsync::Always || Instant::now() >= deadline {
                return Err(AdmissionError::Saturated);
            }
            (state, _) = self
                .available
                .wait_timeout(state, deadline.saturating_duration_since(Instant::now()))
                .unwrap();
        }
    }

    pub fn is_healthy(&self) -> bool {
        !self.state.lock().unwrap().failed
    }

    pub fn fail(&self) {
        self.state.lock().unwrap().failed = true;
        self.available.notify_all();
    }

    /// Call under the commit fence. The receiver confirms the checkpoint's
    /// log prefix reached disk, including under the asynchronous fsync policies.
    pub fn checkpoint(&self) -> Result<(Checkpoint, Receiver<()>), AdmissionError> {
        let state = self.state.lock().unwrap();
        if state.failed {
            return Err(AdmissionError::Unavailable);
        }
        let (send, receive) = bounded(1);
        self.tx
            .send(WriterRequest::Sync(send))
            .map_err(|_| AdmissionError::Unavailable)?;
        Ok((
            Checkpoint {
                generation: self.generation,
                sequence: state.sequence,
            },
            receive,
        ))
    }

    pub fn stop(&self) {
        self.fail();
        let _ = self.tx.send(WriterRequest::Stop);
    }
}

pub struct Permit {
    dispatch: Arc<AofDispatch>,
    bytes: usize,
}

impl Permit {
    /// Hands off an already published mutation while holding the commit fence.
    pub(crate) fn submit(mut self, bytes: Vec<u8>) -> Result<Option<Receiver<()>>, AdmissionError> {
        let dispatch = self.dispatch.clone();
        let (done, wait) = if dispatch.policy == AppendFsync::Always {
            let (send, receive) = bounded(1);
            (Some(send), Some(receive))
        } else {
            (None, None)
        };

        assert!(
            bytes.len() <= self.bytes,
            "AOF admission budget underestimated"
        );
        let sent = {
            let mut state = dispatch.state.lock().unwrap();
            if state.failed {
                return Err(AdmissionError::Uncertain);
            }
            state.used_bytes -= self.bytes - bytes.len();
            self.bytes = bytes.len();
            let result = dispatch.tx.send(WriterRequest::Record {
                bytes,
                done,
                permit: self,
            });
            if result.is_ok() {
                state.sequence += 1;
            }
            result
        };
        dispatch.available.notify_all();
        if sent.is_err() {
            dispatch.fail();
            return Err(AdmissionError::Uncertain);
        }
        Ok(wait)
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut state = self.dispatch.state.lock().unwrap();
        state.used_bytes -= self.bytes;
        state.used_records -= 1;
        self.dispatch.available.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asynchronous_admission_bounds_bytes_and_records() {
        for policy in [AppendFsync::EverySec, AppendFsync::No] {
            let checkpoint = Checkpoint {
                generation: [3; 16],
                sequence: 17,
            };
            let (dispatch, requests) = AofDispatch::with_limits(checkpoint, policy, 8, 2);
            let first = dispatch.reserve(6).unwrap();
            assert!(matches!(
                dispatch.reserve(3),
                Err(AdmissionError::Saturated)
            ));
            let second = dispatch.reserve(2).unwrap();
            assert!(matches!(
                dispatch.reserve(0),
                Err(AdmissionError::Saturated)
            ));
            drop(second);
            assert!(first.submit(vec![1; 5]).unwrap().is_none());
            assert_eq!(dispatch.checkpoint().unwrap().0.sequence, 18);
            assert!(matches!(
                dispatch.reserve(4),
                Err(AdmissionError::Saturated)
            ));
            let WriterRequest::Record { bytes, permit, .. } = requests.recv().unwrap() else {
                panic!("record must precede checkpoint");
            };
            assert_eq!(bytes, [1; 5]);
            drop(permit);
            assert!(dispatch.reserve(8).is_ok());
            assert!(matches!(
                dispatch.reserve(9),
                Err(AdmissionError::Oversized)
            ));
        }
    }

    #[test]
    fn synchronous_admission_times_out_and_reports_writer_failure() {
        let checkpoint = Checkpoint {
            generation: [3; 16],
            sequence: 17,
        };
        let (dispatch, requests) = AofDispatch::with_limits(checkpoint, AppendFsync::Always, 8, 1);
        let first = dispatch.reserve(8).unwrap();
        let waiting_since = Instant::now();
        assert!(matches!(
            dispatch.reserve(1),
            Err(AdmissionError::Saturated)
        ));
        assert!(waiting_since.elapsed() >= ADMISSION_TIMEOUT);
        let ack = first.submit(vec![1]).unwrap().unwrap();
        assert!(matches!(
            ack.try_recv(),
            Err(crossbeam_channel::TryRecvError::Empty)
        ));
        let request = requests.recv().unwrap();
        dispatch.fail();
        drop(request);
        assert!(ack.recv().is_err());
        assert!(matches!(
            dispatch.reserve(1),
            Err(AdmissionError::Unavailable)
        ));
    }
}
