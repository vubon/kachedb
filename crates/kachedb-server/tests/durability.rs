use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static NEXT_PORT_OFFSET: AtomicU16 = AtomicU16::new(0);

struct Store(PathBuf);

impl Store {
    fn new(test: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("kachedb-{test}-{}-{nanos}", std::process::id()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn server(&self, port: u16, snapshots: bool) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_kachedb-server"));
        command
            .current_dir(&self.0)
            .args([
                "--port",
                &port.to_string(),
                "--workers",
                "2",
                "--pool-mb",
                "8",
            ])
            .args([
                "--aof",
                "--aof-file",
                "journal.kaof",
                "--appendfsync",
                "always",
                "--no-shm",
            ])
            .args(["--snapshot-file", "snapshot.kdb"])
            .arg(if snapshots {
                "--snapshot"
            } else {
                "--no-snapshot"
            })
            .stdout(Stdio::null());
        command
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Running(Child);

impl Running {
    fn start(command: &mut Command, port: u16) -> (Self, Client) {
        let mut server = Self(command.spawn().unwrap());
        let end = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(client) =
                Client::connect(port).and_then(|mut client| match client.try_request(&[b"PING"]) {
                    Ok(Reply::Simple(ref pong)) if pong == "PONG" => Ok(client),
                    _ => Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "not ready",
                    )),
                })
            {
                return (server, client);
            }
            assert!(
                server.0.try_wait().unwrap().is_none(),
                "server exited before readiness"
            );
            assert!(Instant::now() < end, "server did not reach readiness");
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn exit(&mut self, signal: libc::c_int) -> std::process::ExitStatus {
        assert_eq!(unsafe { libc::kill(self.0.id() as libc::pid_t, signal) }, 0);
        let end = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                return status;
            }
            assert!(Instant::now() < end, "server did not exit");
            thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[derive(Debug, PartialEq)]
enum Reply {
    Simple(String),
    Error(String),
    Count(i64),
    Bulk(Option<Vec<u8>>),
    List(Vec<Reply>),
}

struct Client(BufReader<TcpStream>);

impl Client {
    fn connect(port: u16) -> std::io::Result<Self> {
        let stream = TcpStream::connect(("127.0.0.1", port))?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        Ok(Self(BufReader::new(stream)))
    }

    fn try_request(&mut self, args: &[&[u8]]) -> std::io::Result<Reply> {
        let mut wire = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            write!(&mut wire, "${}\r\n", arg.len())?;
            wire.extend_from_slice(arg);
            wire.extend_from_slice(b"\r\n");
        }
        self.0.get_mut().write_all(&wire)?;
        self.try_read_reply()
    }

    fn request(&mut self, args: &[&[u8]]) -> Reply {
        self.try_request(args).unwrap()
    }

    fn try_read_reply(&mut self) -> std::io::Result<Reply> {
        let mut line = String::new();
        let bytes_read = self.0.read_line(&mut line)?;
        if bytes_read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed by server",
            ));
        }
        let line = line.strip_suffix("\r\n").ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "missing CRLF in reply")
        })?;
        let (kind, value) = line.split_at(1);
        match kind {
            "+" => Ok(Reply::Simple(value.into())),
            "-" => Ok(Reply::Error(value.into())),
            ":" => value
                .parse()
                .map(Reply::Count)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
            "$" if value == "-1" => Ok(Reply::Bulk(None)),
            "$" => {
                let size = value
                    .parse::<usize>()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                let mut bytes = vec![0; size + 2];
                self.0.read_exact(&mut bytes)?;
                if &bytes[size..] != b"\r\n" {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "bulk reply missing trailing CRLF",
                    ));
                }
                bytes.truncate(size);
                Ok(Reply::Bulk(Some(bytes)))
            }
            "*" => {
                let count = value
                    .parse::<usize>()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                let mut list = Vec::with_capacity(count);
                for _ in 0..count {
                    list.push(self.try_read_reply()?);
                }
                Ok(Reply::List(list))
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unexpected RESP reply {line:?}"),
            )),
        }
    }
}

fn free_port() -> u16 {
    for _ in 0..500 {
        let offset = NEXT_PORT_OFFSET.fetch_add(1, Ordering::Relaxed);
        let base = 25000 + ((std::process::id() as u32 * 37) % 20000) as u16;
        let port = base + (offset % 15000);
        if let Ok(listener) = TcpListener::bind(("127.0.0.1", port)) {
            drop(listener);
            return port;
        }
    }
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[test]
fn restart_replays_only_the_checkpoint_suffix() {
    let store = Store::new("checkpoint");
    let port1 = free_port();
    let vector = [1.0_f32.to_ne_bytes(), 0.0_f32.to_ne_bytes()].concat();
    let (mut server, mut client) = Running::start(&mut store.server(port1, true), port1);
    let ok = Reply::Simple("OK".into());

    assert_eq!(
        client.request(&[b"SET", b"expired", b"old", b"EX", b"30"]),
        ok
    );
    assert_eq!(
        client.request(&[b"HSET", b"hash", b"before", b"checkpoint"]),
        Reply::Count(1)
    );
    assert_eq!(
        client.request(&[
            b"VINDEX",
            b"CREATE",
            b"ann",
            b"DIM",
            b"2",
            b"M",
            b"4",
            b"QUANTIZATION",
            b"SQ8"
        ]),
        ok
    );
    assert_eq!(
        client.request(&[b"VADD", b"ann", b"root", b"2", &vector]),
        Reply::Count(1)
    );
    assert_eq!(
        client.request(&[b"VADD", b"ann", b"tombstone", b"2", &vector]),
        Reply::Count(1)
    );
    assert_eq!(
        client.request(&[b"VDEL", b"ann", b"tombstone"]),
        Reply::Count(1)
    );
    let index_info = client.request(&[b"VINDEX", b"INFO", b"ann"]);
    assert!(matches!(index_info, Reply::List(_)));
    drop(client);
    assert!(server.exit(libc::SIGTERM).success());

    let snapshot = fs::read(store.0.join("snapshot.kdb")).unwrap();
    assert_eq!(&snapshot[..4], b"KDB\x07");
    assert_eq!(snapshot[16], 1);
    assert_eq!(u64::from_le_bytes(snapshot[33..41].try_into().unwrap()), 6);
    let port2 = free_port();
    let (mut server, mut client) = Running::start(&mut store.server(port2, true), port2);
    assert_eq!(client.request(&[b"VINDEX", b"INFO", b"ann"]), index_info);
    assert_eq!(client.request(&[b"MSET", b"one", b"1", b"two", b"2"]), ok);
    assert_eq!(client.request(&[b"APPEND", b"two", b"2"]), Reply::Count(2));
    assert_eq!(client.request(&[b"INCRBY", b"one", b"3"]), Reply::Count(4));
    assert_eq!(client.request(&[b"PERSIST", b"expired"]), Reply::Count(1));
    assert_eq!(
        client.request(&[b"HDEL", b"hash", b"before"]),
        Reply::Count(1)
    );
    assert_eq!(
        client.request(&[
            b"VADD_BATCH",
            b"flat",
            b"keep",
            &vector,
            b"payload",
            b"delete",
            &vector,
            b"discard"
        ]),
        Reply::Count(2)
    );
    assert_eq!(
        client.request(&[b"VDEL", b"flat", b"delete"]),
        Reply::Count(1)
    );
    assert_eq!(
        client.request(&[b"VINDEX", b"DROP", b"ann"]),
        Reply::Count(1)
    );
    drop(client);
    let _ = server.exit(libc::SIGKILL);
    assert_eq!(fs::read(store.0.join("snapshot.kdb")).unwrap(), snapshot);

    let port3 = free_port();
    let (_server, mut client) = Running::start(&mut store.server(port3, true), port3);
    assert_eq!(
        client.request(&[b"MGET", b"one", b"two"]),
        Reply::List(vec![
            Reply::Bulk(Some(b"4".to_vec())),
            Reply::Bulk(Some(b"22".to_vec())),
        ])
    );
    assert_eq!(client.request(&[b"TTL", b"expired"]), Reply::Count(-1));
    assert_eq!(client.request(&[b"HLEN", b"hash"]), Reply::Count(0));
    assert!(matches!(
        client.request(&[b"VINDEX", b"INFO", b"ann"]),
        Reply::Error(_)
    ));
    assert_eq!(
        client.request(&[b"VSEARCH", b"flat", &vector]),
        Reply::List(vec![Reply::List(vec![
            Reply::Bulk(Some(b"keep".to_vec())),
            Reply::Bulk(Some(b"1.000000".to_vec())),
            Reply::Bulk(Some(b"payload".to_vec())),
        ])])
    );
}

#[test]
fn write_failure_reports_uncertainty_and_rejects_later_mutations() {
    let store = Store::new("write-failure");
    let port1 = free_port();
    let mut command = store.server(port1, false);
    // The limit also applies to stderr when the test runner redirects it to a file.
    command.stderr(Stdio::null());
    // Leave enough room for the generation marker and a checked header only.
    unsafe {
        command.pre_exec(|| {
            let limit = libc::rlimit {
                rlim_cur: 96,
                rlim_max: 96,
            };
            if libc::signal(libc::SIGXFSZ, libc::SIG_IGN) == libc::SIG_ERR
                || libc::setrlimit(libc::RLIMIT_FSIZE, &limit) != 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let (mut server, mut client) = Running::start(&mut command, port1);
    let value = [b'x'; 128];
    let reply = client.request(&[b"SET", b"published", &value]);
    assert!(matches!(reply, Reply::Error(message) if message.contains("outcome is uncertain")));
    assert_eq!(
        client.request(&[b"GET", b"published"]),
        Reply::Bulk(Some(value.to_vec()))
    );
    let reply = client.request(&[b"SET", b"rejected", b"value"]);
    assert!(matches!(reply, Reply::Error(message) if message.contains("writer unavailable")));
    assert_eq!(client.request(&[b"GET", b"rejected"]), Reply::Bulk(None));
    drop(client);
    assert_eq!(server.exit(libc::SIGTERM).code(), Some(1));

    let port2 = free_port();
    let (_server, mut client) = Running::start(&mut store.server(port2, false), port2);
    assert_eq!(client.request(&[b"GET", b"published"]), Reply::Bulk(None));
}

#[test]
fn aof_only_restart_preserves_cross_worker_order_and_flushes() {
    let store = Store::new("ordered");
    let port1 = free_port();
    let (mut server, client) = Running::start(&mut store.server(port1, false), port1);
    let second = Client::connect(port1).unwrap();
    let mut replies = thread::scope(|scope| {
        let increment = |mut client: Client| {
            (0..32)
                .map(|_| match client.request(&[b"INCR", b"counter"]) {
                    Reply::Count(n) => n,
                    reply => panic!("increment failed: {reply:?}"),
                })
                .collect::<Vec<_>>()
        };
        let concurrent = scope.spawn(move || increment(second));
        let mut replies = increment(client);
        replies.extend(concurrent.join().unwrap());
        replies
    });
    replies.sort_unstable();
    assert_eq!(replies, (1..=64).collect::<Vec<_>>());
    let _ = server.exit(libc::SIGKILL);

    let port2 = free_port();
    let (mut server, mut recovered) = Running::start(&mut store.server(port2, false), port2);
    let ok = Reply::Simple("OK".into());
    assert_eq!(
        recovered.request(&[b"GET", b"counter"]),
        Reply::Bulk(Some(b"64".to_vec()))
    );
    assert_eq!(recovered.request(&[b"FLUSHDB"]), ok);
    assert_eq!(
        recovered.request(&[b"SET", b"after-first-flush", b"value"]),
        ok
    );
    assert_eq!(recovered.request(&[b"FLUSHALL"]), ok);
    assert_eq!(recovered.request(&[b"SET", b"retained", b"value"]), ok);
    drop(recovered);
    let _ = server.exit(libc::SIGKILL);

    let port3 = free_port();
    let (_server, mut recovered) = Running::start(&mut store.server(port3, false), port3);
    assert_eq!(recovered.request(&[b"DBSIZE"]), Reply::Count(1));
    assert_eq!(
        recovered.request(&[b"GET", b"retained"]),
        Reply::Bulk(Some(b"value".to_vec()))
    );
}

#[test]
fn invalid_persistence_sources_fail_before_startup() {
    let store = Store::new("startup");
    let port = free_port();
    let journal = store.0.join("journal.kaof");
    fs::create_dir(&journal).unwrap();
    let failure = store.server(port, false).output().unwrap();
    assert!(!failure.status.success());
    assert!(String::from_utf8_lossy(&failure.stderr).contains("Persistence startup failed"));
    fs::remove_dir(&journal).unwrap();

    let mut bad_frame =
        kachedb_net::aof_encode::encode_frame(kachedb_net::AofOp::Set, b"key", b"value", 1);
    *bad_frame.last_mut().unwrap() ^= 1;
    fs::write(&journal, &bad_frame).unwrap();
    let failure = store.server(port, false).output().unwrap();
    assert!(!failure.status.success());
    assert!(String::from_utf8_lossy(&failure.stderr).contains("checksum mismatch"));
    assert_eq!(fs::read(&journal).unwrap(), bad_frame);
}
