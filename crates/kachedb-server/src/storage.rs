//! Chooses an authoritative persistence source before starting workers.

use std::error::Error;
use std::io::ErrorKind;
use std::os::unix::fs::MetadataExt;
use std::time::{SystemTime, UNIX_EPOCH};

use kachedb_hash::ShardedSwissTable;
use kachedb_net::durability::{self, Checkpoint};
use kachedb_vector::VectorIndexRegistry;

use crate::aof::{self, AofError, AofWriter};
use crate::aof_recovery::{self, LogState};
use crate::config::ServerConfig;
use crate::recovery_pool::RecoveryPools;
use crate::snapshot::{self, SnapshotEncryptionConfig};

pub fn start(
    config: &ServerConfig,
    table: &ShardedSwissTable,
    vectors: &VectorIndexRegistry,
    encryption: &SnapshotEncryptionConfig,
) -> Result<(RecoveryPools, Option<AofWriter>), Box<dyn Error>> {
    let bytes = config
        .pool_mb_per_core
        .checked_mul(1024 * 1024)
        .ok_or("Worker pool size is out of range")?;
    let mut pools = RecoveryPools::new(config.num_workers, bytes)?;
    if config.snapshot_enabled {
        durability::enable_snapshot_fence();
    }
    if !config.aof_enabled {
        if config.snapshot_enabled {
            snapshot::load_snapshot(
                &config.snapshot_path,
                table,
                &mut pools,
                vectors,
                Some(encryption),
            )?;
        }
        return Ok((pools, None));
    }

    let file = aof::open_locked(&config.aof_path, true)?;
    if config.snapshot_enabled {
        match std::fs::metadata(&config.snapshot_path) {
            Ok(snapshot) => {
                let journal = file.metadata()?;
                if snapshot.dev() == journal.dev() && snapshot.ino() == journal.ino() {
                    return Err("Snapshot and AOF must be distinct files".into());
                }
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }

    let log = aof_recovery::inspect(&config.aof_path)?;
    let snapshot = if config.snapshot_enabled {
        snapshot::load_snapshot_for_recovery(
            &config.snapshot_path,
            table,
            &mut pools,
            vectors,
            Some(encryption),
            |checkpoint| Ok(select_snapshot(log, checkpoint)?),
        )?
    } else {
        None
    };
    if log.requires_snapshot && snapshot.is_none() {
        return Err(AofError::Migration("journal requires its snapshot base").into());
    }
    let base = snapshot.as_ref().and_then(|stats| stats.checkpoint);
    let promote_snapshot = snapshot.is_some() && base.is_none();
    let after_sequence = base.map_or(0, |checkpoint| checkpoint.sequence);

    let replayed =
        aof_recovery::replay_from(&config.aof_path, after_sequence, table, &mut pools, vectors)?;
    if replayed != log {
        return Err(AofError::InvalidRecord("journal changed during recovery").into());
    }
    aof_recovery::trim_incomplete_tail(&config.aof_path, log)?;
    log::info!(
        "Replayed {} AOF records after checkpoint {after_sequence} from {:?}",
        replayed.sequence - after_sequence,
        config.aof_path,
    );

    let generation = match log.generation {
        Some(generation) => generation,
        None => {
            let mut generation = [0; 16];
            crate::crypto::generate_random_bytes(&mut generation)?;
            generation
        }
    };
    let checkpoint = Checkpoint {
        generation,
        sequence: log.sequence,
    };
    let initialize = log.generation.is_none().then_some(promote_snapshot);
    let mut writer = AofWriter::start(
        file,
        &config.aof_path,
        config.appendfsync,
        checkpoint,
        initialize,
    )?;
    durability::set_dispatch(writer.dispatch());
    // Persist the new pairing before admitting a single mutation against a snapshot base.
    if promote_snapshot
        && let Err(error) = snapshot::save_snapshot(
            &config.snapshot_path,
            table,
            vectors,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .try_into()
                .unwrap_or(u32::MAX),
            Some(encryption),
        )
    {
        let _ = writer.stop();
        return Err(error.into());
    }
    Ok((pools, Some(writer)))
}

fn select_snapshot(log: LogState, checkpoint: Option<Checkpoint>) -> Result<bool, AofError> {
    if let Some(checkpoint) = checkpoint {
        if log.generation != Some(checkpoint.generation) || checkpoint.sequence > log.sequence {
            return Err(AofError::Migration(
                "snapshot checkpoint does not match the available journal generation and sequence",
            ));
        }
        return Ok(true);
    }
    if log.generation.is_some() && !log.requires_snapshot {
        return Ok(false);
    }
    if log.sequence == 0 {
        return Ok(true);
    }
    Err(AofError::Migration(
        "snapshot has no checkpoint and the journal cannot establish an independent base; migrate from a known complete source",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_selection_requires_a_proven_base() {
        let empty = LogState::default();
        let legacy = LogState {
            sequence: 4,
            ..empty
        };
        let complete = LogState {
            generation: Some([1; 16]),
            ..legacy
        };
        let dependent = LogState {
            requires_snapshot: true,
            ..complete
        };
        let checkpoint = Checkpoint {
            generation: [1; 16],
            sequence: 3,
        };

        assert!(select_snapshot(empty, None).unwrap());
        assert!(select_snapshot(legacy, None).is_err());
        assert!(!select_snapshot(complete, None).unwrap());
        assert!(select_snapshot(dependent, None).is_err());
        assert!(select_snapshot(dependent, Some(checkpoint)).unwrap());
        assert!(
            select_snapshot(
                complete,
                Some(Checkpoint {
                    sequence: 5,
                    ..checkpoint
                })
            )
            .is_err()
        );
        assert!(
            select_snapshot(
                complete,
                Some(Checkpoint {
                    generation: [2; 16],
                    ..checkpoint
                })
            )
            .is_err()
        );
    }
}
