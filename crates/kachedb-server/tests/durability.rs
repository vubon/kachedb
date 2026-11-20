use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
            if let Ok(mut client) = Client::connect(port) {
                assert_eq!(client.request(&[b"PING"]), Reply::Simple("PONG".into()));
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

    fn request(&mut self, args: &[&[u8]]) -> Reply {
        let mut wire = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            write!(&mut wire, "${}\r\n", arg.len()).unwrap();
            wire.extend_from_slice(arg);
            wire.extend_from_slice(b"\r\n");
        }
        self.0.get_mut().write_all(&wire).unwrap();
        self.read_reply()
    }

    fn read_reply(&mut self) -> Reply {
        let mut line = String::new();
        self.0.read_line(&mut line).unwrap();
        let line = line.strip_suffix("\r\n").unwrap();
        let (kind, value) = line.split_at(1);
        match kind {
            "+" => Reply::Simple(value.into()),
            "-" => Reply::Error(value.into()),
            ":" => Reply::Count(value.parse().unwrap()),
            "$" if value == "-1" => Reply::Bulk(None),
            "$" => {
                let size = value.parse::<usize>().unwrap();
                let mut bytes = vec![0; size + 2];
                self.0.read_exact(&mut bytes).unwrap();
                assert_eq!(&bytes[size..], b"\r\n");
                bytes.truncate(size);
                Reply::Bulk(Some(bytes))
            }
            "*" => Reply::List(
                (0..value.parse::<usize>().unwrap())
                    .map(|_| self.read_reply())
                    .collect(),
            ),
            _ => panic!("unexpected RESP reply {line:?}"),
        }
    }
}

fn free_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[test]
fn restart_replays_only_the_checkpoint_suffix() {
    let store = Store::new("checkpoint");
    let port = free_port();
    let vector = [1.0_f32.to_ne_bytes(), 0.0_f32.to_ne_bytes()].concat();
    let (mut server, mut client) = Running::start(&mut store.server(port, true), port);
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
    assert!(server.exit(libc::SIGTERM).success());

    let snapshot = fs::read(store.0.join("snapshot.kdb")).unwrap();
    assert_eq!(&snapshot[..4], b"KDB\x07");
    assert_eq!(snapshot[16], 1);
    assert_eq!(u64::from_le_bytes(snapshot[33..41].try_into().unwrap()), 6);
    let (mut server, mut client) = Running::start(&mut store.server(port, true), port);
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
    let _ = server.exit(libc::SIGKILL);
    assert_eq!(fs::read(store.0.join("snapshot.kdb")).unwrap(), snapshot);

    let (_server, mut client) = Running::start(&mut store.server(port, true), port);
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
    let port = free_port();
    let mut command = store.server(port, false);
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
    let (mut server, mut client) = Running::start(&mut command, port);
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
    assert!(!server.exit(libc::SIGTERM).success());

    let (_server, mut client) = Running::start(&mut store.server(port, false), port);
    assert_eq!(client.request(&[b"GET", b"published"]), Reply::Bulk(None));
}

#[test]
fn aof_only_restart_preserves_cross_worker_order_and_flushes() {
    let store = Store::new("ordered");
    let port = free_port();
    let (mut server, client) = Running::start(&mut store.server(port, false), port);
    let second = Client::connect(port).unwrap();
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

    let (mut server, mut recovered) = Running::start(&mut store.server(port, false), port);
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
    let _ = server.exit(libc::SIGKILL);

    let (_server, mut recovered) = Running::start(&mut store.server(port, false), port);
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
