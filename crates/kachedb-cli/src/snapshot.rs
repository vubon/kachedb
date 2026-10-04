//! Offline snapshot inspection and diagnostic utilities for KacheDB.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfo {
    pub format_version: String,
    pub created_ts: u64,
    pub flags: Option<u32>,
    pub is_encrypted: bool,
    pub cipher_suite: Option<String>,
    pub salt_hex: Option<String>,
    pub nonce_hex: Option<String>,
    pub crc_valid: bool,
    pub expected_crc: u32,
    pub calculated_crc: u32,
    pub file_size: usize,
}

pub fn parse_snapshot_info(data: &[u8]) -> Result<SnapshotInfo, String> {
    if data.len() < 20 {
        return Err(format!(
            "Corrupt or truncated snapshot file (too small: {} bytes)",
            data.len()
        ));
    }

    let content_len = data.len() - 4;
    let expected_crc = u32::from_le_bytes([
        data[content_len],
        data[content_len + 1],
        data[content_len + 2],
        data[content_len + 3],
    ]);
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&data[..content_len]);
    let calculated_crc = hasher.finalize();
    let crc_valid = expected_crc == calculated_crc;

    let magic = &data[0..4];

    if magic == b"KDB\x02" {
        let ts = u64::from_le_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
        ]);
        Ok(SnapshotInfo {
            format_version: "v2 (Plaintext Legacy)".to_string(),
            created_ts: ts,
            flags: None,
            is_encrypted: false,
            cipher_suite: None,
            salt_hex: None,
            nonce_hex: None,
            crc_valid,
            expected_crc,
            calculated_crc,
            file_size: data.len(),
        })
    } else if magic == b"KDB\x03" || magic == b"KDB\x04" {
        let ts = u64::from_le_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
        ]);
        let flags = u32::from_le_bytes([data[12], data[13], data[14], data[15]]);
        let is_encrypted = (flags & 0x01) != 0;

        let (cipher_suite, salt_hex, nonce_hex) = if is_encrypted {
            let cipher_code = (flags >> 1) & 0x07;
            let cipher_str = match cipher_code {
                0 => "AES-256-GCM (Hardware Accelerated)",
                1 => "ChaCha20-Poly1305 (Streaming AEAD)",
                _ => "Unknown Cipher Code",
            };
            let (salt, nonce) = if content_len >= 16 + 32 + 12 {
                let salt = &data[16..48];
                let nonce = &data[48..60];
                (Some(hex_encode(salt)), Some(hex_encode(nonce)))
            } else {
                (None, None)
            };
            (Some(cipher_str.to_string()), salt, nonce)
        } else {
            (None, None, None)
        };

        let format_version = if magic == b"KDB\x04" {
            "v4 (Typed KV + Redis Hashes)".to_string()
        } else {
            "v3 (Authenticated Snapshot)".to_string()
        };

        Ok(SnapshotInfo {
            format_version,
            created_ts: ts,
            flags: Some(flags),
            is_encrypted,
            cipher_suite,
            salt_hex,
            nonce_hex,
            crc_valid,
            expected_crc,
            calculated_crc,
            file_size: data.len(),
        })
    } else {
        Ok(SnapshotInfo {
            format_version: format!("Unknown Magic Header: {:?}", magic),
            created_ts: 0,
            flags: None,
            is_encrypted: false,
            cipher_suite: None,
            salt_hex: None,
            nonce_hex: None,
            crc_valid,
            expected_crc,
            calculated_crc,
            file_size: data.len(),
        })
    }
}

pub fn inspect_snapshot(path_str: &str) {
    let path = std::path::Path::new(path_str);
    if !path.exists() {
        eprintln!("❌ Snapshot file not found: {:?}", path);
        std::process::exit(1);
    }

    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("❌ Failed to read snapshot file: {e}");
            std::process::exit(1);
        }
    };

    let info = match parse_snapshot_info(&data) {
        Ok(info) => info,
        Err(err) => {
            eprintln!("❌ {err}");
            std::process::exit(1);
        }
    };

    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📦 KacheDB Snapshot Inspection: {:?}", path);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!(
        "File Size:          {} bytes ({:.2} KB)",
        info.file_size,
        info.file_size as f64 / 1024.0
    );
    println!(
        "Checksum (CRC32):   {:#010x} [{}]",
        info.calculated_crc,
        if info.crc_valid {
            "VALID"
        } else {
            "CORRUPTED!"
        }
    );
    println!("Format Version:     {}", info.format_version);
    if info.created_ts > 0 {
        println!("Created Timestamp:  {} (Unix Epoch)", info.created_ts);
    }
    if let Some(flags) = info.flags {
        println!("Flags:              {:#010x}", flags);
    }
    println!(
        "Encryption:         {}",
        if info.is_encrypted {
            "ENABLED"
        } else {
            "Disabled (Plaintext)"
        }
    );
    if let Some(cipher) = info.cipher_suite {
        println!("Cipher Suite:       {}", cipher);
    }
    if let Some(salt) = info.salt_hex {
        println!("Salt (256-bit):     {}", salt);
    }
    if let Some(nonce) = info.nonce_hex {
        println!("Master Nonce:       {}", nonce);
    }
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_snapshot_info_v4_plaintext() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"KDB\x04");
        buf.extend_from_slice(&1728000000u64.to_le_bytes()); // ts
        buf.extend_from_slice(&0u32.to_le_bytes()); // flags: unencrypted
        buf.extend_from_slice(b"sample-payload-data");
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.extend_from_slice(&crc.to_le_bytes());

        let info = parse_snapshot_info(&buf).expect("should parse v4 snapshot");
        assert_eq!(info.format_version, "v4 (Typed KV + Redis Hashes)");
        assert_eq!(info.created_ts, 1728000000);
        assert!(!info.is_encrypted);
        assert!(info.cipher_suite.is_none());
        assert!(info.crc_valid);
    }

    #[test]
    fn test_parse_snapshot_info_v4_encrypted() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"KDB\x04");
        buf.extend_from_slice(&1728000000u64.to_le_bytes()); // ts
        let flags: u32 = 0x01; // encrypted (bit 0 = 1), AES-256-GCM (bits 1..4 = 0)
        buf.extend_from_slice(&flags.to_le_bytes());
        buf.extend_from_slice(&[0xAA; 32]); // salt
        buf.extend_from_slice(&[0xBB; 12]); // nonce
        buf.extend_from_slice(b"sample-ciphertext");
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.extend_from_slice(&crc.to_le_bytes());

        let info = parse_snapshot_info(&buf).expect("should parse v4 encrypted snapshot");
        assert_eq!(info.format_version, "v4 (Typed KV + Redis Hashes)");
        assert_eq!(info.created_ts, 1728000000);
        assert!(info.is_encrypted);
        assert_eq!(
            info.cipher_suite.as_deref(),
            Some("AES-256-GCM (Hardware Accelerated)")
        );
        let expected_salt = "aa".repeat(32);
        let expected_nonce = "bb".repeat(12);
        assert_eq!(info.salt_hex.as_deref(), Some(expected_salt.as_str()));
        assert_eq!(info.nonce_hex.as_deref(), Some(expected_nonce.as_str()));
        assert!(info.crc_valid);
    }

    #[test]
    fn test_parse_snapshot_info_v2_legacy() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"KDB\x02");
        buf.extend_from_slice(&1700000000u64.to_le_bytes()); // ts
        buf.extend_from_slice(&[0u8; 4]); // legacy alignment/reserved
        buf.extend_from_slice(b"legacy-kv-data");
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.extend_from_slice(&crc.to_le_bytes());

        let info = parse_snapshot_info(&buf).expect("should parse v2 snapshot");
        assert_eq!(info.format_version, "v2 (Plaintext Legacy)");
        assert_eq!(info.created_ts, 1700000000);
        assert!(!info.is_encrypted);
        assert!(info.crc_valid);
    }

    #[test]
    fn test_parse_snapshot_info_corrupted_crc() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"KDB\x04");
        buf.extend_from_slice(&1728000000u64.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(b"payload");
        buf.extend_from_slice(&0xDEADBEEFu32.to_le_bytes()); // wrong CRC

        let info = parse_snapshot_info(&buf).expect("should parse header but flag invalid crc");
        assert!(!info.crc_valid);
    }
}
