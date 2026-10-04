//! Administrative, server, and session command execution handlers (`PING`, `ECHO`, `HELLO`, `CLIENT`, `INFO`, `DBSIZE`, `TYPE`, `FLUSHDB`, `FLUSHALL`).

use kachedb_core::SlabPool;
use kachedb_hash::{ShardedSwissTable, VALUE_TYPE_HASH, hash_key};
use kachedb_proto_resp::{
    ClientSubcommand, Command, encode_array_header, encode_bulk_string, encode_error,
    encode_integer, encode_null, encode_simple_string,
};
use kachedb_vector::VectorIndexRegistry;

use super::ClientState;
use crate::error::NetError;

/// Executes administrative, diagnostic, and server-level commands.
pub fn handle_admin(
    cmd: Command<'_>,
    write_buf: &mut Vec<u8>,
    table: &ShardedSwissTable,
    pool: &mut SlabPool,
    vectors: &VectorIndexRegistry,
    now_sec: u32,
    mut client_state: Option<&mut ClientState>,
) -> Result<bool, NetError> {
    match cmd {
        Command::Ping { message } => match message {
            Some(msg) => encode_bulk_string(write_buf, msg),
            None => encode_simple_string(write_buf, "PONG"),
        },

        Command::Hello {
            protover,
            auth: _,
            setname,
        } => {
            let ver = protover.unwrap_or(2);
            if ver != 2 && ver != 3 {
                encode_error(write_buf, "NOPROTO unsupported protocol version");
                return Ok(true);
            }

            if let Some(ref mut state) = client_state {
                state.proto_version = ver as u8;
                if let Some(name) = setname {
                    state.name = Some(name.to_vec());
                }
            }

            let client_id = client_state
                .as_ref()
                .map(|s| s.client_id as i64)
                .unwrap_or(1);

            encode_array_header(write_buf, 14);
            encode_bulk_string(write_buf, b"server");
            encode_bulk_string(write_buf, b"kachedb");
            encode_bulk_string(write_buf, b"version");
            encode_bulk_string(write_buf, b"0.2.0");
            encode_bulk_string(write_buf, b"proto");
            encode_integer(write_buf, ver);
            encode_bulk_string(write_buf, b"id");
            encode_integer(write_buf, client_id);
            encode_bulk_string(write_buf, b"mode");
            encode_bulk_string(write_buf, b"standalone");
            encode_bulk_string(write_buf, b"role");
            encode_bulk_string(write_buf, b"master");
            encode_bulk_string(write_buf, b"modules");
            encode_array_header(write_buf, 0);
        }
        Command::Client { subcommand } => match subcommand {
            ClientSubcommand::SetName(name) => {
                if let Some(ref mut state) = client_state {
                    state.name = Some(name.to_vec());
                }
                encode_simple_string(write_buf, "OK");
            }
            ClientSubcommand::GetName => {
                if let Some(name) = client_state.as_ref().and_then(|s| s.name.as_ref()) {
                    encode_bulk_string(write_buf, name);
                } else {
                    encode_null(write_buf);
                }
            }
            ClientSubcommand::Id => {
                let id = client_state
                    .as_ref()
                    .map(|s| s.client_id as i64)
                    .unwrap_or(1);
                encode_integer(write_buf, id);
            }
            ClientSubcommand::List => {
                let name_str = client_state
                    .as_ref()
                    .and_then(|s| s.name.as_ref())
                    .and_then(|n| std::str::from_utf8(n).ok())
                    .unwrap_or("");
                let list_info = format!(
                    "id=1 addr=127.0.0.1:0 fd=0 name={name_str} age=1 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 argv-mem=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=client\n"
                );
                encode_bulk_string(write_buf, list_info.as_bytes());
            }
            ClientSubcommand::Unrecognized(sub) => {
                let sub_str = std::str::from_utf8(sub).unwrap_or("unknown");
                encode_error(
                    write_buf,
                    &format!("ERR unknown subcommand '{sub_str}' for 'CLIENT'"),
                );
            }
        },
        Command::Info { section: _ } => {
            let info_text = format!(
                "# Server\r\n\
                 kachedb_version:0.2.0\r\n\
                 os:{os}\r\n\
                 arch_bits:64\r\n\
                 process_id:{pid}\r\n\
                 tcp_port:6379\r\n\
                 uptime_in_seconds:{uptime}\r\n\
                 \r\n\
                 # Memory\r\n\
                 used_memory:{used_mem}\r\n\
                 used_memory_human:{used_mem_human:.2}M\r\n\
                 used_memory_peak:{used_mem}\r\n\
                 megaslabs_allocated:{megaslabs}\r\n\
                 slab_slots_active:{active_slots}\r\n\
                 fragmentation_ratio:1.00\r\n\
                 \r\n\
                 # Stats\r\n\
                 total_connections_received:1\r\n\
                 total_commands_processed:1\r\n\
                 instantaneous_ops_per_sec:0\r\n\
                 keyspace_hits:1\r\n\
                 keyspace_misses:0\r\n\
                 \r\n\
                 # Keyspace\r\n\
                 db0:keys={total_keys},expires=0,avg_ttl=0\r\n\
                 \r\n                 \r\n\
                  # VectorEngine\r\n\
                  active_indices:{vec_indices}\r\n\
                  total_vectors:{vec_total}\r\n\
                  vector_memory_bytes:{vec_mem}\r\n\
                  simd_kernel:auto\r\n",
                os = std::env::consts::OS,
                pid = std::process::id(),
                uptime = now_sec,
                used_mem = pool.total_allocated_bytes(),
                used_mem_human = pool.total_allocated_bytes() as f64 / (1024.0 * 1024.0),
                megaslabs = pool.arena_count(),
                active_slots = table.len(),
                total_keys = table.len(),
                vec_indices = vectors.overall_stats(now_sec).0,
                vec_total = vectors.overall_stats(now_sec).1,
                vec_mem = vectors.overall_stats(now_sec).2,
            );
            encode_bulk_string(write_buf, info_text.as_bytes());
        }
        Command::CommandDoc => {
            encode_simple_string(write_buf, "OK");
        }
        Command::BgRewriteAof => {
            encode_simple_string(write_buf, "Background append only file rewriting started");
        }
        Command::DBSize => {
            encode_integer(write_buf, table.len() as i64);
        }
        Command::Type { key } => {
            let h = hash_key(key);
            if let Some(entry) = table.lookup_checked(h, now_sec) {
                if entry.value_type == VALUE_TYPE_HASH {
                    encode_simple_string(write_buf, "hash");
                } else {
                    encode_simple_string(write_buf, "string");
                }
            } else {
                encode_simple_string(write_buf, "none");
            }
        }
        Command::FlushDb | Command::FlushAll => {
            let removed_blocks = table.clear();
            for id in removed_blocks {
                let _ = pool.deallocate(id);
            }
            encode_simple_string(write_buf, "OK");
        }
        Command::Quit => {
            encode_simple_string(write_buf, "OK");
            return Ok(false);
        }
        Command::Unknown { name } => {
            let name_str = std::str::from_utf8(name).unwrap_or("unknown");
            encode_error(write_buf, &format!("ERR unknown command '{name_str}'"));
        }
        _ => unreachable!("handle_admin called with non-admin command"),
    }

    Ok(true)
}
