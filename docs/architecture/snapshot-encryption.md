# 🔐 Snapshot Encryption-at-Rest (`dump.kdb`)

KacheDB provides zero-overhead, authenticated streaming encryption-at-rest for binary snapshots (`dump.kdb`). This guarantees cryptographic confidentiality and tamper-proof integrity for stored vectors, payloads, and key-value pairs without compromising in-memory query performance.

---

## 🏛️ Architecture & Threat Model

### 1. Zero Hot-Path Invariant
In-memory SwissTable reads/writes and SIMD vector dot products operate **strictly on plaintext in memory**. Encryption and decryption occur entirely out-of-band:
* **Background Writing:** Handled exclusively by the asynchronous `SnapshotWorker` thread.
* **Boot Hydration:** Decryption streams sequentially through 64 KiB buffers before populating in-memory SwissTable slots and vector registries.

Zero CPU cycles or lock contention are added to client queries.

```text
[ Client Query ] ──► [ SwissTable / SIMD Vector Index ] (Plaintext Memory, 3.09ns)
                                 │
                     (Async Snapshot Thread)
                                 ▼
                     [ 64 KiB Chunk Buffering ]
                                 │
                   (AES-256-GCM / ChaCha20-Poly1305)
                                 ▼
              [ KDB\x03 Authenticated Ciphertext Stream ]
                                 │
                      [ dump.kdb on Disk ]
```

### 2. Threat Mitigation
* **Data Exfiltration / Cold Storage Theft:** An adversary stealing `dump.kdb` or disk backups cannot extract vector embeddings, user payloads, parent keys, or cached data without the 256-bit master key.
* **Tampering & Bit-Flipping:** Every 64 KiB chunk contains an authenticated 16-byte Poly1305 / GHASH tag. Any modified byte fails AEAD authentication, aborting snapshot hydration immediately.
* **Block Reordering & Truncation:** Per-chunk nonces are derived as $Nonce_i = MasterNonce \oplus i$, and chunk index $i$ is bound to the Additional Authenticated Data ($AAD = i$). Any attempt to reorder or omit chunks results in an authentication failure.

---

## ⚡ Supported Cipher Suites

KacheDB supports two symmetric Authenticated Encryption with Associated Data (AEAD) cipher suites:

| Cipher Suite | Flag Code | Implementation | Performance | Best Use Case |
| :--- | :---: | :--- | :--- | :--- |
| **`aes-256-gcm`** *(Default)* | `0` | Hardware-accelerated AES-NI / ARMv8 Crypto | > 4 GB/sec streaming | Production on modern x86_64 and aarch64 (Apple Silicon, AWS Graviton) |
| **`chacha20-poly1305`** | `1` | Constant-time software streaming AEAD | ~1.5 GB/sec | Embedded architectures or platforms lacking hardware AES acceleration |

---

## 🔑 Key Ingestion & Management

KacheDB supports three methods of key configuration:

### 1. Raw 32-Byte Master Key File (Recommended for Production)
The most secure method for production deployments. Point KacheDB directly to a 32-byte cryptographic random binary file:

```bash
# Generate a cryptographically secure 32-byte random key file
openssl rand -out /etc/kachedb/snapshot.key 32
chmod 600 /etc/kachedb/snapshot.key
```

Configure in `kachedb.conf`:
```conf
snapshot-encryption yes
snapshot-encryption-cipher aes-256-gcm
snapshot-encryption-key-file /etc/kachedb/snapshot.key
```

### 2. 64-Character Hexadecimal String
Provide a raw 256-bit key formatted as 64 hexadecimal characters:

```conf
snapshot-encryption yes
snapshot-encryption-key "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
```

### 3. Passphrase with HKDF-SHA256 Key Derivation
If a variable-length user passphrase is provided, KacheDB derives a 256-bit master key using **HKDF-SHA256**:
* **Salt:** A cryptographically secure 32-byte random salt generated per-snapshot and written into the file header.
* **Info Label:** `b"kachedb-snapshot-v1"`
* **Zeroization:** The in-memory `EncryptionKey` is securely zeroized via `std::ptr::write_volatile` upon drop.

```conf
snapshot-encryption yes
snapshot-encryption-key "your_strong_secret_passphrase_here"
```

---

## 📦 Wire Format Specification (`KDB\x03`)

Binary snapshots use the `KDB\x03` file format:

```text
+---------------------------------------------------------------------------------+
| Offset | Field                  | Size (Bytes) | Description                    |
+--------+------------------------+--------------+--------------------------------+
| 0      | Magic Header           | 4            | b"KDB\x03"                     |
| 4      | Timestamp              | 8            | Unix Epoch seconds (u64 LE)    |
| 12     | Flags                  | 4            | Bit 0: Encrypted (1=yes, 0=no) |
|        |                        |              | Bits 1..3: Cipher Suite Code   |
+--------+------------------------+--------------+--------------------------------+
| IF ENCRYPTED (Flags & 0x01 != 0):                                              |
| 16     | Salt                   | 32           | HKDF 256-bit salt              |
| 48     | Master Nonce           | 12           | 96-bit base nonce              |
| 60..N  | Encrypted Stream       | Variable     | Sequence of 64 KiB AEAD chunks |
|        | - Chunk Length         | 4            | Ciphertext + 16-byte tag (u32) |
|        | - Ciphertext + Tag     | Chunk Length | AEAD encrypted block           |
|        | - EOF Sentinel         | 4            | 0x00000000 (u32 LE)            |
+--------+------------------------+--------------+--------------------------------+
| IF UNENCRYPTED (Flags & 0x01 == 0):                                            |
| 16..N  | Plaintext Stream       | Variable     | Vector & SwissTable sections   |
+--------+------------------------+--------------+--------------------------------+
| N..N+4 | Checksum Trailer       | 4            | IEEE 802.3 CRC32 (u32 LE)      |
+---------------------------------------------------------------------------------+
```

### Backward Compatibility
* Legacy **`KDB\x02`** unencrypted snapshots automatically detect the `b"KDB\x02"` magic header and hydrate state without key prompts.
* Unencrypted **`KDB\x03`** snapshots (`flags = 0`) hydrate state directly with zero decryption overhead.

---

## 🔍 Offline Diagnostic Inspection (`kachedb-cli`)

Use `kachedb-cli snapshot-info` to inspect and verify snapshot files on disk without launching the full server:

```bash
kachedb-cli snapshot-info /var/lib/kachedb/dump.kdb
```

### Example Inspection Output
```text
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📦 KacheDB Snapshot Inspection: "/var/lib/kachedb/dump.kdb"
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
File Size:          430 bytes (0.42 KB)
Checksum (CRC32):   0x295f2f0d [VALID]
Format Version:     v3 (Authenticated Snapshot)
Created Timestamp:  1790481552 (Unix Epoch)
Flags:              0x00000001
Encryption:         ENABLED
Cipher Suite:       AES-256-GCM (Hardware Accelerated)
Salt (256-bit):     dafd90590dc19b358f8e76fd11a0430b07dc0b5881c063987a3c7619379147ff
Master Nonce:       b8fe98eb980ea4d551497618
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```
