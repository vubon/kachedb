use super::*;
use kachedb_proto_resp::ClientSubcommand;

#[test]
fn execute_ping_and_get_set_flow() {
    let mut conn = Connection::new();
    let table = ShardedSwissTable::new();
    let mut pool = SlabPool::new(0, 16 * 1024 * 1024).unwrap();

    // 1. PING -> PONG
    Connection::execute_command(
        Command::Ping { message: None },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"+PONG\r\n");
    conn.write_buf.clear();

    // 2. SET key1 "hello_world"
    Connection::execute_command(
        Command::Set {
            key: b"key1",
            value: b"hello_world",
            ttl_ms: None,
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"+OK\r\n");
    conn.write_buf.clear();

    // 3. GET key1
    Connection::execute_command(
        Command::Get { key: b"key1" },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"$11\r\nhello_world\r\n");
    conn.write_buf.clear();

    // 4. EXISTS key1
    let mut keys = smallvec::SmallVec::new();
    keys.push(&b"key1"[..]);
    Connection::execute_command(
        Command::Exists { keys },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":1\r\n");
    conn.write_buf.clear();

    // 5. DEL key1
    let mut del_keys = smallvec::SmallVec::new();
    del_keys.push(&b"key1"[..]);
    Connection::execute_command(
        Command::Del { keys: del_keys },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":1\r\n");
    conn.write_buf.clear();

    // 6. GET key1 (now deleted -> null)
    Connection::execute_command(
        Command::Get { key: b"key1" },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"$-1\r\n");
}

#[test]
fn execute_set_with_ttl_and_expiry_flow() {
    let mut conn = Connection::new();
    let table = ShardedSwissTable::new();
    let mut pool = SlabPool::new(0, 16 * 1024 * 1024).unwrap();

    // 1. SET key_ttl "temp" EX 10 (at epoch 1000s -> expires at 1010s)
    Connection::execute_command_with_time(
        Command::Set {
            key: b"key_ttl",
            value: b"temp",
            ttl_ms: Some(10_000), // 10 seconds
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        1000,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"+OK\r\n");
    conn.write_buf.clear();

    // 2. GET at epoch 1005s -> active
    Connection::execute_command_with_time(
        Command::Get { key: b"key_ttl" },
        &mut conn.write_buf,
        &table,
        &mut pool,
        1005,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"$4\r\ntemp\r\n");
    conn.write_buf.clear();

    // 3. EXISTS at epoch 1005s -> 1
    let mut keys = smallvec::SmallVec::new();
    keys.push(&b"key_ttl"[..]);
    Connection::execute_command_with_time(
        Command::Exists { keys },
        &mut conn.write_buf,
        &table,
        &mut pool,
        1005,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":1\r\n");
    conn.write_buf.clear();

    // 4. GET at epoch 1011s -> expired -> $-1\r\n
    Connection::execute_command_with_time(
        Command::Get { key: b"key_ttl" },
        &mut conn.write_buf,
        &table,
        &mut pool,
        1011,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"$-1\r\n");
    conn.write_buf.clear();

    // 5. EXISTS at epoch 1011s -> 0
    let mut keys2 = smallvec::SmallVec::new();
    keys2.push(&b"key_ttl"[..]);
    Connection::execute_command_with_time(
        Command::Exists { keys: keys2 },
        &mut conn.write_buf,
        &table,
        &mut pool,
        1011,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":0\r\n");
}

#[test]
fn execute_vector_commands_flow() {
    let mut conn = Connection::new();
    let table = ShardedSwissTable::new();
    let mut pool = SlabPool::new(0, 16 * 1024 * 1024).unwrap();
    let vectors = VectorIndexRegistry::new();

    // 1. VADD faq doc1 3 <floats> PAYLOAD "answer1"
    let v1 = [1.0f32, 0.0, 0.0];
    let mut v1_bytes = Vec::new();
    for f in &v1 {
        v1_bytes.extend_from_slice(&f.to_ne_bytes());
    }

    Connection::execute_command_with_vectors(
        Command::VAdd {
            index: b"faq",
            id: b"doc1",
            dim: 3,
            vector_bytes: &v1_bytes,
            payload: Some(b"answer1"),
            ttl_sec: None,
            tag_mask: 0,
            parent_key: None,
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &vectors,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":1\r\n");
    conn.write_buf.clear();

    // 2. VSEARCH faq <v1_bytes> TOPK 1 THRESHOLD 0.8
    Connection::execute_command_with_vectors(
        Command::VSearch {
            index: b"faq",
            query_bytes: &v1_bytes,
            top_k: 1,
            threshold: 0.8,
            filter_mask: 0,
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &vectors,
    )
    .unwrap();

    // Expected array with 1 item: [doc1, "1.000000", "answer1"]
    let resp_str = String::from_utf8_lossy(&conn.write_buf);
    assert!(resp_str.contains("*1\r\n*3\r\n$4\r\ndoc1\r\n"));
    assert!(resp_str.contains("answer1"));
    conn.write_buf.clear();

    // 2b. VADD faq doc2 3 <v1_bytes> TAGS 2 PARENT parent:faq
    Connection::execute_command_with_vectors(
        Command::VAdd {
            index: b"faq",
            id: b"doc2",
            dim: 3,
            vector_bytes: &v1_bytes,
            payload: Some(b"answer2"),
            ttl_sec: None,
            tag_mask: 0b10,
            parent_key: Some(b"parent:faq"),
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &vectors,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":1\r\n");
    conn.write_buf.clear();

    // 2c. VSEARCH faq with FILTER 2 (matches doc2 only)
    Connection::execute_command_with_vectors(
        Command::VSearch {
            index: b"faq",
            query_bytes: &v1_bytes,
            top_k: 5,
            threshold: 0.8,
            filter_mask: 0b10,
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &vectors,
    )
    .unwrap();
    let filtered_str = String::from_utf8_lossy(&conn.write_buf);
    assert!(filtered_str.contains("doc2"));
    assert!(!filtered_str.contains("doc1"));
    conn.write_buf.clear();

    // 2d. VSEARCH faq with FILTER 1 (matches neither doc1 nor doc2)
    Connection::execute_command_with_vectors(
        Command::VSearch {
            index: b"faq",
            query_bytes: &v1_bytes,
            top_k: 5,
            threshold: 0.8,
            filter_mask: 0b01,
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &vectors,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"*0\r\n");
    conn.write_buf.clear();

    // 3. VSTATS faq
    Connection::execute_command_with_vectors(
        Command::VStats { index: b"faq" },
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &vectors,
    )
    .unwrap();
    let stats_resp = String::from_utf8_lossy(&conn.write_buf);
    assert!(stats_resp.contains("total_vectors"));
    conn.write_buf.clear();

    // 4. VDEL faq doc1
    Connection::execute_command_with_vectors(
        Command::VDel {
            index: b"faq",
            id: b"doc1",
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &vectors,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":1\r\n");
}

#[test]
fn execute_batch_vector_commands_flow() {
    let mut conn = Connection::new();
    let table = ShardedSwissTable::new();
    let mut pool = SlabPool::new(0, 16 * 1024 * 1024).unwrap();
    let vectors = VectorIndexRegistry::new();

    // 1. VADD_BATCH articles with 2 items
    let v1 = vec![1.0f32, 0.0f32, 0.0f32, 0.0f32];
    let mut v1_bytes = Vec::new();
    for f in &v1 {
        v1_bytes.extend_from_slice(&f.to_ne_bytes());
    }

    let v2 = vec![0.0f32, 1.0f32, 0.0f32, 0.0f32];
    let mut v2_bytes = Vec::new();
    for f in &v2 {
        v2_bytes.extend_from_slice(&f.to_ne_bytes());
    }

    let mut items = smallvec::SmallVec::new();
    items.push(kachedb_proto_resp::command::BatchVectorItem {
        id: b"art1",
        vector_bytes: &v1_bytes,
        payload: Some(b"Rust Vector Engine"),
        ttl_sec: None,
    });
    items.push(kachedb_proto_resp::command::BatchVectorItem {
        id: b"art2",
        vector_bytes: &v2_bytes,
        payload: Some(b"Python DMA Client"),
        ttl_sec: None,
    });

    Connection::execute_command_with_vectors(
        Command::VAddBatch {
            index: b"articles",
            items,
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &vectors,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":2\r\n");
    conn.write_buf.clear();

    // 2. VSEARCH_BATCH articles with 2 queries
    let mut queries = smallvec::SmallVec::new();
    queries.push(&v1_bytes[..]);
    queries.push(&v2_bytes[..]);

    Connection::execute_command_with_vectors(
        Command::VSearchBatch {
            index: b"articles",
            queries,
            top_k: 1,
            threshold: 0.8,
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &vectors,
    )
    .unwrap();

    let resp_str = String::from_utf8_lossy(&conn.write_buf);
    assert!(resp_str.starts_with("*2\r\n"));
    assert!(resp_str.contains("art1"));
    assert!(resp_str.contains("Rust Vector Engine"));
    assert!(resp_str.contains("art2"));
    assert!(resp_str.contains("Python DMA Client"));
}

#[test]
fn execute_expire_ttl_persist_flow() {
    let mut conn = Connection::new();
    let table = ShardedSwissTable::new();
    let mut pool = SlabPool::new(0, 16 * 1024 * 1024).unwrap();

    // 1. SET user "alice" (persistent)
    Connection::execute_command_with_time(
        Command::Set {
            key: b"user",
            value: b"alice",
            ttl_ms: None,
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"+OK\r\n");
    conn.write_buf.clear();

    // 2. TTL user -> -1 (persistent)
    Connection::execute_command_with_time(
        Command::Ttl { key: b"user" },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":-1\r\n");
    conn.write_buf.clear();

    // 3. EXPIRE user 50 -> :1
    Connection::execute_command_with_time(
        Command::Expire {
            key: b"user",
            seconds: 50,
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":1\r\n");
    conn.write_buf.clear();

    // 4. TTL user at epoch 120 -> 30s remaining
    Connection::execute_command_with_time(
        Command::Ttl { key: b"user" },
        &mut conn.write_buf,
        &table,
        &mut pool,
        120,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":30\r\n");
    conn.write_buf.clear();

    // 5. PTTL user at epoch 120 -> 30000 ms remaining
    Connection::execute_command_with_time(
        Command::PTtl { key: b"user" },
        &mut conn.write_buf,
        &table,
        &mut pool,
        120,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":30000\r\n");
    conn.write_buf.clear();

    // 6. PERSIST user -> :1
    Connection::execute_command_with_time(
        Command::Persist { key: b"user" },
        &mut conn.write_buf,
        &table,
        &mut pool,
        120,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":1\r\n");
    conn.write_buf.clear();

    // 7. TTL user -> -1 (persistent again)
    Connection::execute_command_with_time(
        Command::Ttl { key: b"user" },
        &mut conn.write_buf,
        &table,
        &mut pool,
        120,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":-1\r\n");
    conn.write_buf.clear();

    // 8. EXPIRE with negative seconds -> immediate deletion (Redis 7 semantics)
    Connection::execute_command_with_time(
        Command::Expire {
            key: b"user",
            seconds: -5,
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        120,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":1\r\n");
    conn.write_buf.clear();

    // 9. TTL user -> -2 (missing)
    Connection::execute_command_with_time(
        Command::Ttl { key: b"user" },
        &mut conn.write_buf,
        &table,
        &mut pool,
        120,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":-2\r\n");
}

#[test]
fn execute_extended_primitives_flow() {
    let table = ShardedSwissTable::new();
    let mut pool = SlabPool::new(0, 16 * 1024 * 1024).unwrap();
    let mut conn = Connection::new();

    // 1. MSET k1 v1 k2 v2 k3 v3 -> +OK
    use smallvec::smallvec;
    Connection::execute_command(
        Command::MSet {
            pairs: smallvec![
                (b"k1".as_slice(), b"v1".as_slice()),
                (b"k2".as_slice(), b"v2".as_slice()),
                (b"k3".as_slice(), b"v3".as_slice())
            ],
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"+OK\r\n");
    conn.write_buf.clear();

    // 2. STRLEN k1 -> :2
    Connection::execute_command(
        Command::Strlen { key: b"k1" },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":2\r\n");
    conn.write_buf.clear();

    // 3. APPEND k1 _append -> :9 ("v1_append")
    Connection::execute_command(
        Command::Append {
            key: b"k1",
            value: b"_append",
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":9\r\n");
    conn.write_buf.clear();

    // 4. GET k1 -> $9\r\nv1_append\r\n
    Connection::execute_command(
        Command::Get { key: b"k1" },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"$9\r\nv1_append\r\n");
    conn.write_buf.clear();

    // 5. INCR counter (new key) -> :1
    Connection::execute_command(
        Command::Incr { key: b"counter" },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":1\r\n");
    conn.write_buf.clear();

    // 6. INCRBY counter 10 -> :11
    Connection::execute_command(
        Command::IncrBy {
            key: b"counter",
            delta: 10,
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":11\r\n");
    conn.write_buf.clear();

    // 7. DECR counter -> :10
    Connection::execute_command(
        Command::Decr { key: b"counter" },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":10\r\n");
    conn.write_buf.clear();

    // 8. DECRBY counter 5 -> :5
    Connection::execute_command(
        Command::DecrBy {
            key: b"counter",
            delta: 5,
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":5\r\n");
    conn.write_buf.clear();

    // 9. INCR non-integer key (k1 is "v1_append") -> error
    Connection::execute_command(
        Command::Incr { key: b"k1" },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    assert_eq!(
        conn.write_buf,
        b"-ERR value is not an integer or out of range\r\n"
    );
    conn.write_buf.clear();
}

#[test]
fn execute_hello_client_and_info_flow() {
    let table = ShardedSwissTable::new();
    let mut pool = SlabPool::new(0, 16 * 1024 * 1024).unwrap();
    let mut conn = Connection::new();

    // 1. HELLO 3 SETNAME my-python-app
    Connection::execute_command_full(
        Command::Hello {
            protover: Some(3),
            auth: None,
            setname: Some(b"my-python-app"),
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        Some(&mut conn.client_state),
    )
    .unwrap();
    assert!(conn.write_buf.starts_with(b"*14\r\n"));
    assert_eq!(conn.client_state.proto_version, 3);
    assert_eq!(
        conn.client_state.name.as_deref(),
        Some(b"my-python-app".as_slice())
    );
    conn.write_buf.clear();

    // 2. CLIENT GETNAME -> $13\r\nmy-python-app\r\n
    Connection::execute_command_full(
        Command::Client {
            subcommand: ClientSubcommand::GetName,
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        Some(&mut conn.client_state),
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"$13\r\nmy-python-app\r\n");
    conn.write_buf.clear();

    // 3. CLIENT SETNAME new-worker -> +OK\r\n
    Connection::execute_command_full(
        Command::Client {
            subcommand: ClientSubcommand::SetName(b"new-worker"),
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        Some(&mut conn.client_state),
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"+OK\r\n");
    assert_eq!(
        conn.client_state.name.as_deref(),
        Some(b"new-worker".as_slice())
    );
    conn.write_buf.clear();

    // 4. INFO server -> contains "# Server"
    Connection::execute_command(
        Command::Info {
            section: Some(b"server"),
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    let resp_str = std::str::from_utf8(&conn.write_buf).unwrap();
    assert!(resp_str.contains("# Server"));
    assert!(resp_str.contains("kachedb_version:0.2.0"));
    assert!(resp_str.contains("# Memory"));
    conn.write_buf.clear();
}

#[test]
fn test_hnsw_vindex_command_flow() {
    let mut conn = Connection::new();
    let table = ShardedSwissTable::new();
    let mut pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();

    // 1. VINDEX CREATE hnsw_kb DIM 3 METRIC COSINE QUANTIZATION SQ8
    Connection::execute_command(
        Command::VIndexCreate {
            name: b"hnsw_kb",
            dim: 3,
            m: Some(16),
            ef_construction: Some(100),
            ef_search: Some(50),
            metric: Some(b"COSINE"),
            quantization: Some(b"SQ8"),
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"+OK\r\n");
    conn.write_buf.clear();

    // 2. VADD hnsw_kb doc1 3 <vec_bytes>
    let v1 = [1.0f32, 0.0, 0.0];
    let mut v1_bytes = Vec::new();
    for f in &v1 {
        v1_bytes.extend_from_slice(&f.to_ne_bytes());
    }
    Connection::execute_command(
        Command::VAdd {
            index: b"hnsw_kb",
            id: b"doc1",
            dim: 3,
            vector_bytes: &v1_bytes,
            payload: Some(b"doc1_payload"),
            ttl_sec: None,
            tag_mask: 0,
            parent_key: None,
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":1\r\n");
    conn.write_buf.clear();

    // 3. VSEARCH hnsw_kb <vec_bytes> TOPK 1 THRESHOLD 0.8
    Connection::execute_command(
        Command::VSearch {
            index: b"hnsw_kb",
            query_bytes: &v1_bytes,
            top_k: 1,
            threshold: 0.8,
            filter_mask: 0,
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    let resp_str = String::from_utf8_lossy(&conn.write_buf);
    assert!(resp_str.contains("doc1"));
    assert!(resp_str.contains("doc1_payload"));
    conn.write_buf.clear();

    // 4. VINDEX INFO hnsw_kb
    Connection::execute_command(
        Command::VIndexInfo { name: b"hnsw_kb" },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    let info_str = String::from_utf8_lossy(&conn.write_buf);
    assert!(info_str.contains("hnsw"));
    conn.write_buf.clear();

    // 5. VINDEX DROP hnsw_kb
    Connection::execute_command(
        Command::VIndexDrop { name: b"hnsw_kb" },
        &mut conn.write_buf,
        &table,
        &mut pool,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":1\r\n");
    conn.write_buf.clear();
}

#[test]
fn test_auth_requirepass_flow() {
    let table = ShardedSwissTable::new();
    let mut pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();
    let mut conn = Connection::new();

    // 1. Set password on client state
    conn.client_state = ClientState::with_password(Some(Arc::new(b"secret123".to_vec())));

    // 2. Command before auth -> -NOAUTH
    Connection::execute_command_full(
        Command::Get { key: b"foo" },
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &DEFAULT_VECTORS,
        None,
        Some(&mut conn.client_state),
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"-NOAUTH Authentication required.\r\n");
    conn.write_buf.clear();

    // 3. Wrong password -> -ERR invalid password
    Connection::execute_command_full(
        Command::Auth {
            username: None,
            password: b"wrong",
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &DEFAULT_VECTORS,
        None,
        Some(&mut conn.client_state),
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"-ERR invalid password\r\n");
    conn.write_buf.clear();

    // 4. Correct password -> +OK
    Connection::execute_command_full(
        Command::Auth {
            username: None,
            password: b"secret123",
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &DEFAULT_VECTORS,
        None,
        Some(&mut conn.client_state),
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"+OK\r\n");
    assert!(conn.client_state.authenticated);
    conn.write_buf.clear();

    // 5. Subsequent command now succeeds
    Connection::execute_command_full(
        Command::Ping { message: None },
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &DEFAULT_VECTORS,
        None,
        Some(&mut conn.client_state),
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"+PONG\r\n");
    conn.write_buf.clear();
}

#[test]
fn execute_dbsize_type_flush_flow() {
    let mut pool = SlabPool::new(0, 16 * 1024 * 1024).unwrap();
    let table = ShardedSwissTable::new();
    let mut conn = Connection::new();

    // 1. Initial DBSIZE -> 0
    Connection::execute_command_full(
        Command::DBSize,
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":0\r\n");
    conn.write_buf.clear();

    // 2. TYPE on non-existent key -> +none
    Connection::execute_command_full(
        Command::Type {
            key: b"nonexistent",
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"+none\r\n");
    conn.write_buf.clear();

    // 3. SET two keys
    Connection::execute_command_full(
        Command::Set {
            key: b"k1",
            value: b"v1",
            ttl_ms: None,
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    conn.write_buf.clear();

    Connection::execute_command_full(
        Command::Set {
            key: b"k2",
            value: b"v2",
            ttl_ms: None,
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    conn.write_buf.clear();

    // 4. DBSIZE -> 2
    Connection::execute_command_full(
        Command::DBSize,
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":2\r\n");
    conn.write_buf.clear();

    // 5. TYPE on existing key -> +string
    Connection::execute_command_full(
        Command::Type { key: b"k1" },
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"+string\r\n");
    conn.write_buf.clear();

    // 6. FLUSHDB -> +OK
    Connection::execute_command_full(
        Command::FlushDb,
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"+OK\r\n");
    conn.write_buf.clear();

    // 7. DBSIZE -> 0
    Connection::execute_command_full(
        Command::DBSize,
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":0\r\n");
    conn.write_buf.clear();

    // 8. TYPE on flushed key -> +none
    Connection::execute_command_full(
        Command::Type { key: b"k1" },
        &mut conn.write_buf,
        &table,
        &mut pool,
        0,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"+none\r\n");
    conn.write_buf.clear();
}

#[test]
fn execute_hash_primitives_flow() {
    use smallvec::smallvec;

    let table = ShardedSwissTable::new();
    let mut pool = SlabPool::new(0, 16 * 1024 * 1024).unwrap();
    let mut conn = Connection::new();

    // 1. HSET myhash field1 "hello" -> :1
    Connection::execute_command_full(
        Command::HSet {
            key: b"myhash",
            pairs: smallvec![(b"field1" as &[u8], b"hello" as &[u8])],
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":1\r\n");
    conn.write_buf.clear();

    // 2. HGET myhash field1 -> $5\r\nhello\r\n
    Connection::execute_command_full(
        Command::HGet {
            key: b"myhash",
            field: b"field1",
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"$5\r\nhello\r\n");
    conn.write_buf.clear();

    // 3. HGET myhash missing_field -> $-1\r\n
    Connection::execute_command_full(
        Command::HGet {
            key: b"myhash",
            field: b"missing",
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"$-1\r\n");
    conn.write_buf.clear();

    // 4. HGET missing_key field1 -> $-1\r\n
    Connection::execute_command_full(
        Command::HGet {
            key: b"nohash",
            field: b"field1",
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"$-1\r\n");
    conn.write_buf.clear();

    // 5. HEXISTS myhash field1 -> :1, missing -> :0
    Connection::execute_command_full(
        Command::HExists {
            key: b"myhash",
            field: b"field1",
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":1\r\n");
    conn.write_buf.clear();

    Connection::execute_command_full(
        Command::HExists {
            key: b"myhash",
            field: b"missing",
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":0\r\n");
    conn.write_buf.clear();

    // 6. HLEN myhash -> :1
    Connection::execute_command_full(
        Command::HLen { key: b"myhash" },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":1\r\n");
    conn.write_buf.clear();

    // 7. Multi-field HSET: field2="world" (new), field1="updated" (overwrite) -> :1 new field
    Connection::execute_command_full(
        Command::HSet {
            key: b"myhash",
            pairs: smallvec![
                (b"field2" as &[u8], b"world" as &[u8]),
                (b"field1" as &[u8], b"updated" as &[u8]),
            ],
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":1\r\n");
    conn.write_buf.clear();

    // 8. HLEN -> :2
    Connection::execute_command_full(
        Command::HLen { key: b"myhash" },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":2\r\n");
    conn.write_buf.clear();

    // 9. HGET updated fields
    Connection::execute_command_full(
        Command::HGet {
            key: b"myhash",
            field: b"field1",
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"$7\r\nupdated\r\n");
    conn.write_buf.clear();

    Connection::execute_command_full(
        Command::HGet {
            key: b"myhash",
            field: b"field2",
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"$5\r\nworld\r\n");
    conn.write_buf.clear();

    // 10. HGETALL myhash -> *4 array containing 2 pairs
    Connection::execute_command_full(
        Command::HGetAll { key: b"myhash" },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert!(conn.write_buf.starts_with(b"*4\r\n"));
    conn.write_buf.clear();

    // 11. TYPE myhash -> +hash
    Connection::execute_command_full(
        Command::Type { key: b"myhash" },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"+hash\r\n");
    conn.write_buf.clear();

    // 12. WRONGTYPE: GET myhash -> error
    Connection::execute_command_full(
        Command::Get { key: b"myhash" },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert!(conn.write_buf.starts_with(b"-WRONGTYPE"));
    conn.write_buf.clear();

    // 13. WRONGTYPE: HGET on string key
    Connection::execute_command_full(
        Command::Set {
            key: b"strkey",
            value: b"somevalue",
            ttl_ms: None,
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    conn.write_buf.clear();

    Connection::execute_command_full(
        Command::HGet {
            key: b"strkey",
            field: b"field1",
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert!(conn.write_buf.starts_with(b"-WRONGTYPE"));
    conn.write_buf.clear();

    // 14. HDEL field1 -> :1
    Connection::execute_command_full(
        Command::HDel {
            key: b"myhash",
            fields: smallvec![b"field1" as &[u8]],
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":1\r\n");
    conn.write_buf.clear();

    // 15. HDEL field1 again -> :0
    Connection::execute_command_full(
        Command::HDel {
            key: b"myhash",
            fields: smallvec![b"field1" as &[u8]],
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":0\r\n");
    conn.write_buf.clear();

    // 16. HDEL field2 -> :1 (hash becomes empty, key reclaimed from SwissTable)
    Connection::execute_command_full(
        Command::HDel {
            key: b"myhash",
            fields: smallvec![b"field2" as &[u8]],
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":1\r\n");
    conn.write_buf.clear();

    // 17. TYPE myhash -> +none, EXISTS -> :0
    Connection::execute_command_full(
        Command::Type { key: b"myhash" },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b"+none\r\n");
    conn.write_buf.clear();

    Connection::execute_command_full(
        Command::Exists {
            keys: smallvec![b"myhash" as &[u8]],
        },
        &mut conn.write_buf,
        &table,
        &mut pool,
        100,
        &DEFAULT_VECTORS,
        None,
        None,
    )
    .unwrap();
    assert_eq!(conn.write_buf, b":0\r\n");
    conn.write_buf.clear();
}
