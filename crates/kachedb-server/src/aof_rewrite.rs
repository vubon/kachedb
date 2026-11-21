//! Captures an AOF base in the background and hands installation to the writer.

use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::{RecvTimeoutError, bounded};
use kachedb_hash::ShardedSwissTable;
use kachedb_net::aof_encode::encode_checked_frame;
use kachedb_net::durability::{self, AofDispatch, Checkpoint, RewriteFile};
use kachedb_vector::VectorIndexRegistry;

use crate::aof::{AofError, AofOp};
use crate::snapshot;

pub const BASE_KIND: u8 = 2;
const BASE_HEADER_BYTES: usize = 37;
pub(super) const DELTA_BYTES: usize = 64 * 1024 * 1024;
const DELTA_RECORDS: usize = 65_536;

#[derive(Clone, Copy)]
pub struct BaseHeader {
    pub checkpoint: Checkpoint,
    pub size: u64,
    pub checksum: u32,
}

impl BaseHeader {
    fn encode(self) -> Vec<u8> {
        let mut metadata = [0; BASE_HEADER_BYTES];
        metadata[..16].copy_from_slice(&self.checkpoint.generation);
        metadata[16] = BASE_KIND;
        metadata[17..25].copy_from_slice(&self.checkpoint.sequence.to_le_bytes());
        metadata[25..33].copy_from_slice(&self.size.to_le_bytes());
        metadata[33..37].copy_from_slice(&self.checksum.to_le_bytes());
        encode_checked_frame(AofOp::Generation, &[], &metadata, 0)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AofError> {
        if bytes.len() != BASE_HEADER_BYTES {
            return Err(AofError::InvalidRecord(
                "invalid embedded base header length",
            ));
        }
        let size = u64::from_le_bytes(bytes[25..33].try_into().unwrap());
        if size < 8 {
            return Err(AofError::InvalidRecord("embedded base is too short"));
        }
        Ok(Self {
            checkpoint: Checkpoint {
                generation: bytes[..16].try_into().unwrap(),
                sequence: u64::from_le_bytes(bytes[17..25].try_into().unwrap()),
            },
            size,
            checksum: u32::from_le_bytes(bytes[33..37].try_into().unwrap()),
        })
    }
}

pub fn start(
    path: PathBuf,
    dispatch: Arc<AofDispatch>,
    table: Arc<ShardedSwissTable>,
    vectors: &'static VectorIndexRegistry,
) -> io::Result<JoinHandle<()>> {
    let (trigger, requests) = bounded(1);
    let worker_dispatch = dispatch.clone();
    let worker = thread::Builder::new()
        .name("kachedb-aof-rewrite".into())
        .spawn(move || {
            while worker_dispatch.is_healthy() {
                match requests.recv_timeout(Duration::from_millis(100)) {
                    Ok(()) => {
                        match rewrite(&path, &worker_dispatch, &table, vectors) {
                            Ok(()) => log::info!("BGREWRITEAOF: installed replacement at {path:?}"),
                            Err(error) => {
                                worker_dispatch.abort_rewrite();
                                log::warn!("BGREWRITEAOF: rewrite stopped: {error}");
                            }
                        }
                        worker_dispatch.finish_rewrite();
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
        })?;
    dispatch.set_rewrite_trigger(trigger);
    Ok(worker)
}

fn rewrite(
    path: &Path,
    dispatch: &AofDispatch,
    table: &ShardedSwissTable,
    vectors: &VectorIndexRegistry,
) -> Result<(), Box<dyn Error>> {
    let cancelled = Arc::new(AtomicBool::new(false));
    let (view, sequence) = durability::with_commit_fence(|| {
        let view = snapshot::capture_view(table, vectors, 0)?;
        let sequence = dispatch.begin_rewrite(cancelled.clone())?;
        Ok::<_, snapshot::SnapshotError>((view, sequence))
    })?;
    let mut generation = [0; 16];
    crate::crypto::generate_random_bytes(&mut generation)?;
    let checkpoint = Checkpoint {
        generation,
        sequence,
    };
    let (temporary, replacement) = prepare(path, view, checkpoint, &cancelled)?;
    let result = durability::with_commit_fence(|| dispatch.install_rewrite(replacement))?.recv()?;
    drop(temporary);
    if !result {
        return Err("writer abandoned the replacement".into());
    }
    Ok(())
}

struct Temporary(PathBuf);

impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn prepare(
    target: &Path,
    view: snapshot::SnapshotView,
    checkpoint: Checkpoint,
    cancelled: &AtomicBool,
) -> Result<(Temporary, RewriteFile), Box<dyn Error>> {
    let mut suffix = [0; 8];
    crate::crypto::generate_random_bytes(&mut suffix)?;
    let mut path = target.as_os_str().to_owned();
    path.push(format!(".rewrite-{:016x}.tmp", u64::from_le_bytes(suffix)));
    let path = PathBuf::from(path);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)?;
    let temporary = Temporary(path.clone());
    file.try_lock().map_err(io::Error::from)?;

    let placeholder = BaseHeader {
        checkpoint,
        size: 0,
        checksum: 0,
    }
    .encode();
    file.write_all(&placeholder)?;
    let (size, checksum) = snapshot::write_rewrite_base(
        &mut CancellableWrite {
            file: &mut file,
            cancelled,
        },
        view,
    )?;
    file.rewind()?;
    file.write_all(
        &BaseHeader {
            checkpoint,
            size,
            checksum,
        }
        .encode(),
    )?;
    file.seek(io::SeekFrom::End(0))?;
    Ok((
        temporary,
        RewriteFile {
            file,
            path,
            checkpoint,
        },
    ))
}

struct CancellableWrite<'a> {
    file: &'a mut File,
    cancelled: &'a AtomicBool,
}

impl Write for CancellableWrite<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(io::Error::other(
                "delta budget exceeded or AOF writer stopped",
            ));
        }
        self.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

pub struct Deltas {
    pub sequence: u64,
    records: Vec<Vec<u8>>,
    bytes: usize,
    limit: usize,
    cancelled: Arc<AtomicBool>,
}

impl Deltas {
    pub fn new(sequence: u64, cancelled: Arc<AtomicBool>, limit: usize) -> Self {
        Self {
            sequence,
            records: Vec::new(),
            bytes: 0,
            limit,
            cancelled,
        }
    }

    pub fn push(&mut self, record: Vec<u8>) -> bool {
        if record.len() > self.limit - self.bytes || self.records.len() == DELTA_RECORDS {
            return false;
        }
        self.bytes += record.len();
        self.records.push(record);
        true
    }

    pub fn reaches(&self, sequence: u64) -> bool {
        sequence.checked_sub(self.sequence) == Some(self.records.len() as u64)
    }
}

impl Drop for Deltas {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
    }
}

pub enum InstallError {
    BeforeRename(io::Error),
    AfterRename(io::Error),
}

pub fn install(
    target: &Path,
    active: &mut File,
    replacement: RewriteFile,
    deltas: &Deltas,
    sync_directory: impl FnOnce(&File) -> io::Result<()>,
) -> Result<(), InstallError> {
    install_with_rename(
        target,
        active,
        replacement,
        deltas,
        |from, to| std::fs::rename(from, to),
        sync_directory,
    )
}

fn install_with_rename(
    target: &Path,
    active: &mut File,
    mut replacement: RewriteFile,
    deltas: &Deltas,
    rename: impl FnOnce(&Path, &Path) -> io::Result<()>,
    sync_directory: impl FnOnce(&File) -> io::Result<()>,
) -> Result<(), InstallError> {
    let parent = target
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let directory = File::open(parent).map_err(InstallError::BeforeRename)?;
    for record in &deltas.records {
        replacement
            .file
            .write_all(record)
            .map_err(InstallError::BeforeRename)?;
    }
    replacement
        .file
        .sync_all()
        .map_err(InstallError::BeforeRename)?;
    rename(&replacement.path, target).map_err(InstallError::BeforeRename)?;
    // Retain the new inode lock while syncing the rename. The writer never appends
    // again if this final sync fails.
    *active = replacement.file;
    sync_directory(&directory).map_err(InstallError::AfterRename)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::process::ExitStatusExt;
    use std::process::Command;

    use crossbeam_channel::unbounded;
    use kachedb_core::SlabPool;
    use kachedb_hash::{VALUE_TYPE_STRING, hash_key};
    use kachedb_net::durability::{AppendFsync, WriterRequest};

    use super::*;
    use crate::{
        aof::{AofWriter, WriterOptions},
        aof_recovery,
    };

    const OLD: Checkpoint = Checkpoint {
        generation: [1; 16],
        sequence: 1,
    };
    const NEW: Checkpoint = Checkpoint {
        generation: [2; 16],
        sequence: 1,
    };

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let mut id = [0; 8];
            crate::crypto::generate_random_bytes(&mut id).unwrap();
            let path = std::env::temp_dir().join(format!(
                "kachedb-rewrite-test-{:016x}",
                u64::from_le_bytes(id),
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn journal(&self) -> PathBuf {
            self.0.join("appendonly.aof")
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn put(key: &[u8], value: &[u8], deadline: u32) -> Vec<u8> {
        let mut payload = vec![VALUE_TYPE_STRING];
        payload.extend_from_slice(&deadline.to_le_bytes());
        payload.extend_from_slice(value);
        encode_checked_frame(AofOp::Put, key, &payload, 100)
    }

    fn old_journal(path: &Path) -> File {
        let mut file = crate::aof::open_locked(path, true).unwrap();
        let mut generation = OLD.generation.to_vec();
        generation.push(0);
        file.write_all(&encode_checked_frame(
            AofOp::Generation,
            &[],
            &generation,
            0,
        ))
        .unwrap();
        file.write_all(&put(b"key", b"base", 500)).unwrap();
        file.sync_all().unwrap();
        file
    }

    fn staged(path: &Path) -> (Temporary, RewriteFile) {
        let table = ShardedSwissTable::new();
        let mut pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();
        aof_recovery::restore_value(
            &table,
            &mut pool,
            hash_key(b"key"),
            b"key",
            VALUE_TYPE_STRING,
            b"base",
            500,
        )
        .unwrap();
        let view = snapshot::capture_view(&table, &VectorIndexRegistry::new(), 0).unwrap();
        prepare(path, view, NEW, &AtomicBool::new(false)).unwrap()
    }

    fn assert_recovery(path: &Path, checkpoint: Checkpoint, embedded: Option<u64>, value: &[u8]) {
        let inspected = aof_recovery::inspect(path).unwrap();
        assert_eq!(inspected.generation, Some(checkpoint.generation));
        assert_eq!(inspected.embedded_base, embedded);
        assert_eq!(inspected.sequence, checkpoint.sequence);
        let table = ShardedSwissTable::new();
        let mut pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();
        let replayed =
            aof_recovery::replay_from(path, 0, &table, &mut pool, &VectorIndexRegistry::new())
                .unwrap();
        assert_eq!(replayed, inspected);
        let actual = unsafe {
            table.with_read_lock_entry(hash_key(b"key"), b"key", 100, |entry, value| {
                (value.to_vec(), entry.expire_at_secs)
            })
        };
        assert_eq!(actual, Some((value.to_vec(), 500)));
    }

    #[test]
    fn writer_installs_deltas_before_acknowledging_a_new_checkpoint() {
        let directory = TestDirectory::new();
        let path = directory.journal();
        let mut old = old_journal(&path);
        let (_temporary, replacement) = staged(&path);
        let (dispatch, _) = AofDispatch::new(OLD, AppendFsync::Always);
        let (send, receive) = unbounded();
        let (installed, result) = bounded(1);
        let (synced, checkpoint) = bounded(1);
        let (done, acknowledged) = bounded(1);
        send.send(WriterRequest::BeginRewrite {
            sequence: 1,
            cancelled: Arc::new(AtomicBool::new(false)),
        })
        .unwrap();
        let bytes = put(b"key", b"delta", 500);
        let permit = dispatch.reserve(bytes.len()).unwrap();
        send.send(WriterRequest::Record {
            bytes,
            done: Some(done),
            permit,
        })
        .unwrap();
        send.send(WriterRequest::InstallRewrite {
            replacement,
            installed,
        })
        .unwrap();
        send.send(WriterRequest::Sync(synced)).unwrap();
        send.send(WriterRequest::Stop).unwrap();

        AofWriter::writer_loop(
            &mut old,
            &path,
            WriterOptions {
                policy: AppendFsync::Always,
                delta_bytes: DELTA_BYTES,
            },
            &receive,
            &dispatch,
            OLD,
            File::sync_all,
        )
        .unwrap();
        assert!(acknowledged.recv().is_ok());
        assert!(result.recv().unwrap());
        assert_eq!(
            checkpoint.recv().unwrap(),
            Checkpoint { sequence: 2, ..NEW }
        );
        assert_recovery(&path, Checkpoint { sequence: 2, ..NEW }, Some(1), b"delta");
    }

    #[test]
    fn directory_sync_failure_after_rename_marks_persistence_unavailable() {
        let directory = TestDirectory::new();
        let path = directory.journal();
        let mut old = old_journal(&path);
        let (_temporary, replacement) = staged(&path);
        let (dispatch, _) = AofDispatch::new(OLD, AppendFsync::Always);
        let (send, receive) = unbounded();
        let (installed, result) = bounded(1);
        send.send(WriterRequest::BeginRewrite {
            sequence: 1,
            cancelled: Arc::new(AtomicBool::new(false)),
        })
        .unwrap();
        let bytes = put(b"key", b"delta", 500);
        let permit = dispatch.reserve(bytes.len()).unwrap();
        send.send(WriterRequest::Record {
            bytes,
            done: None,
            permit,
        })
        .unwrap();
        send.send(WriterRequest::InstallRewrite {
            replacement,
            installed,
        })
        .unwrap();
        let error = AofWriter::writer_loop(
            &mut old,
            &path,
            WriterOptions {
                policy: AppendFsync::Always,
                delta_bytes: DELTA_BYTES,
            },
            &receive,
            &dispatch,
            OLD,
            |_| Err(io::Error::other("injected directory sync failure")),
        )
        .unwrap_err();
        assert!(matches!(error, AofError::RewriteInstallUncertain(_)));
        assert!(result.recv().is_err());
        assert!(matches!(
            dispatch.reserve(1),
            Err(durability::AdmissionError::Unavailable)
        ));
        assert_recovery(&path, Checkpoint { sequence: 2, ..NEW }, Some(1), b"delta");
    }

    #[test]
    fn aborting_on_delta_exhaustion_preserves_the_active_journal() {
        let directory = TestDirectory::new();
        let path = directory.journal();
        let mut old = old_journal(&path);
        let (_temporary, replacement) = staged(&path);
        let (dispatch, _) = AofDispatch::new(OLD, AppendFsync::No);
        let (send, receive) = unbounded();
        let (installed, result) = bounded(1);
        let cancelled = Arc::new(AtomicBool::new(false));
        send.send(WriterRequest::BeginRewrite {
            sequence: 1,
            cancelled: cancelled.clone(),
        })
        .unwrap();

        let records = [put(b"key", b"first", 500), put(b"key", b"overflow", 500)];
        let limit = records[0].len();
        for bytes in records {
            let permit = dispatch.reserve(bytes.len()).unwrap();
            send.send(WriterRequest::Record {
                bytes,
                done: None,
                permit,
            })
            .unwrap();
        }
        send.send(WriterRequest::InstallRewrite {
            replacement,
            installed,
        })
        .unwrap();
        let bytes = put(b"key", b"after-abort", 500);
        let permit = dispatch.reserve(bytes.len()).unwrap();
        send.send(WriterRequest::Record {
            bytes,
            done: None,
            permit,
        })
        .unwrap();
        send.send(WriterRequest::Stop).unwrap();

        AofWriter::writer_loop(
            &mut old,
            &path,
            WriterOptions {
                policy: AppendFsync::No,
                delta_bytes: limit,
            },
            &receive,
            &dispatch,
            OLD,
            File::sync_all,
        )
        .unwrap();
        assert!(!result.recv().unwrap());
        assert!(cancelled.load(Ordering::Acquire));
        assert!(dispatch.is_healthy());
        assert_recovery(
            &path,
            Checkpoint { sequence: 4, ..OLD },
            None,
            b"after-abort",
        );
    }

    #[test]
    fn damaged_embedded_base_is_preserved_by_startup_and_repair() {
        let directory = TestDirectory::new();
        let path = directory.journal();
        let mut active = old_journal(&path);
        let (_temporary, replacement) = staged(&path);
        let deltas = Deltas::new(1, Arc::new(AtomicBool::new(false)), DELTA_BYTES);
        assert!(install(&path, &mut active, replacement, &deltas, File::sync_all).is_ok());
        drop(active);
        let mut bytes = fs::read(&path).unwrap();
        bytes[kachedb_net::aof_encode::CHECKED_FRAME_OVERHEAD + BASE_HEADER_BYTES] ^= 1;
        fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            aof_recovery::inspect(&path),
            Err(AofError::InvalidBase(_))
        ));
        assert!(matches!(
            aof_recovery::repair(&path),
            Err(AofError::InvalidBase(_))
        ));
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    // Invoked in a child test process, so the simulated SIGKILL leaves actual
    // on-disk files and exercises cleanup without running destructors.
    #[test]
    fn cutover_crash_child() {
        let (Some(path), Ok(phase)) = (
            std::env::var_os("KACHEDB_TEST_REWRITE_PATH"),
            std::env::var("KACHEDB_TEST_REWRITE_PHASE"),
        ) else {
            return;
        };
        let path = PathBuf::from(path);
        let mut active = old_journal(&path);
        let (_temporary, replacement) = staged(&path);
        let mut deltas = Deltas::new(1, Arc::new(AtomicBool::new(false)), DELTA_BYTES);
        let delta = put(b"key", b"delta", 500);
        active.write_all(&delta).unwrap();
        active.sync_all().unwrap();
        assert!(deltas.push(delta));
        let _ = install_with_rename(
            &path,
            &mut active,
            replacement,
            &deltas,
            |from, to| {
                if phase == "before-rename" {
                    kill_self();
                }
                fs::rename(from, to)
            },
            |directory| {
                if phase == "before-directory-sync" {
                    kill_self();
                }
                directory.sync_all()?;
                kill_self()
            },
        );
        panic!("expected crash at a cutover boundary");
    }

    fn kill_self() -> ! {
        assert_eq!(unsafe { libc::kill(libc::getpid(), libc::SIGKILL) }, 0);
        loop {
            thread::park();
        }
    }

    #[test]
    fn crash_boundaries_leave_a_recoverable_authoritative_journal() {
        for phase in [
            "before-rename",
            "before-directory-sync",
            "after-directory-sync",
        ] {
            let directory = TestDirectory::new();
            let path = directory.journal();
            let child = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "aof_rewrite::tests::cutover_crash_child"])
                .env("KACHEDB_TEST_REWRITE_PATH", &path)
                .env("KACHEDB_TEST_REWRITE_PHASE", phase)
                .output()
                .unwrap();
            assert_eq!(
                child.status.signal(),
                Some(libc::SIGKILL),
                "{phase}: {:?}",
                child
            );
            if phase == "before-rename" {
                assert_recovery(&path, Checkpoint { sequence: 2, ..OLD }, None, b"delta");
            } else {
                assert_recovery(&path, Checkpoint { sequence: 2, ..NEW }, Some(1), b"delta");
            }
        }
    }
}
