//! `kachedb-server` — Zero-overhead Snapshot Cryptographic Engine.
//!
//! Provides hardware-accelerated authenticated encryption (AEAD) for KacheDB
//! binary snapshots (`dump.kdb`) with AES-256-GCM and ChaCha20-Poly1305.

use std::fs::File;
use std::io::Read;
use std::path::Path;

use ring::aead::{AES_256_GCM, Aad, Algorithm, CHACHA20_POLY1305, LessSafeKey, Nonce, UnboundKey};
use ring::hkdf;
use ring::rand::{SecureRandom, SystemRandom};
use thiserror::Error;

/// Chunk size for streaming AEAD encryption (64 KiB).
pub const ENCRYPTION_CHUNK_SIZE: usize = 64 * 1024;

/// Tag size in bytes for AES-256-GCM and ChaCha20-Poly1305 (16 bytes = 128 bits).
pub const TAG_LEN: usize = 16;

/// Master Nonce length (12 bytes = 96 bits).
pub const NONCE_LEN: usize = 12;

/// Salt length for key derivation (32 bytes = 256 bits).
pub const SALT_LEN: usize = 32;

/// HKDF application context label.
const HKDF_INFO: &[u8] = b"kachedb-snapshot-v1";

#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Unsupported cipher: '{0}'. Supported: 'aes-256-gcm', 'chacha20-poly1305'")]
    UnsupportedCipher(String),
    #[error("Authentication failed: snapshot tag mismatch or corrupted ciphertext")]
    AuthenticationFailed,
    #[error("Key derivation failed: {0}")]
    KeyDerivationFailed(String),
    #[error("Random generator failure")]
    RngFailed,
    #[error("Invalid key format: expected 32 raw bytes or 64 hex characters")]
    InvalidKeyFormat,
}

/// Supported symmetric AEAD cipher suites.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CipherSuite {
    #[default]
    Aes256Gcm = 0,
    ChaCha20Poly1305 = 1,
}

impl CipherSuite {
    /// Maps the cipher suite to the underlying ring AEAD algorithm.
    pub fn algorithm(&self) -> &'static Algorithm {
        match self {
            CipherSuite::Aes256Gcm => &AES_256_GCM,
            CipherSuite::ChaCha20Poly1305 => &CHACHA20_POLY1305,
        }
    }

    /// Parses a string into a `CipherSuite`.
    pub fn from_str_name(s: &str) -> Result<Self, CryptoError> {
        match s.trim().to_lowercase().as_str() {
            "aes-256-gcm" | "aes-gcm" | "aes256" | "aes" => Ok(CipherSuite::Aes256Gcm),
            "chacha20-poly1305" | "chacha20" | "chacha" => Ok(CipherSuite::ChaCha20Poly1305),
            other => Err(CryptoError::UnsupportedCipher(other.to_string())),
        }
    }

    /// Returns the canonical configuration name for the cipher suite.
    pub fn as_str(&self) -> &'static str {
        match self {
            CipherSuite::Aes256Gcm => "aes-256-gcm",
            CipherSuite::ChaCha20Poly1305 => "chacha20-poly1305",
        }
    }

    /// Converts from u32 flag representation (bits 1..3).
    pub fn from_flag_code(code: u32) -> Result<Self, CryptoError> {
        match code {
            0 => Ok(CipherSuite::Aes256Gcm),
            1 => Ok(CipherSuite::ChaCha20Poly1305),
            c => Err(CryptoError::UnsupportedCipher(format!("code {c}"))),
        }
    }

    /// Returns the flag code for snapshot header.
    pub fn flag_code(&self) -> u32 {
        *self as u32
    }
}

/// Secure container for a 256-bit symmetric encryption key.
pub struct EncryptionKey {
    bytes: [u8; 32],
}

impl EncryptionKey {
    /// Creates an `EncryptionKey` from raw 32 bytes.
    pub fn new(bytes: [u8; 32]) -> Self {
        Self { bytes }
    }

    /// Returns a reference to the raw 32-byte key.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.bytes
    }
}

impl Drop for EncryptionKey {
    fn drop(&mut self) {
        // Securely zeroize key memory on drop
        for b in self.bytes.iter_mut() {
            unsafe {
                std::ptr::write_volatile(b, 0);
            }
        }
    }
}

/// Generates cryptographically secure random bytes.
pub fn generate_random_bytes(buf: &mut [u8]) -> Result<(), CryptoError> {
    let rng = SystemRandom::new();
    rng.fill(buf).map_err(|_| CryptoError::RngFailed)
}

/// Derives a 256-bit encryption key from a user passphrase or hex string.
pub fn derive_key(key_input: &str, salt: &[u8; SALT_LEN]) -> Result<EncryptionKey, CryptoError> {
    let trimmed = key_input.trim();

    // 1. Check if input is a 64-character hex string (32 raw bytes)
    if trimmed.len() == 64 && trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
        let mut key_bytes = [0u8; 32];
        for i in 0..32 {
            let byte_hex = &trimmed[i * 2..i * 2 + 2];
            key_bytes[i] =
                u8::from_str_radix(byte_hex, 16).map_err(|_| CryptoError::InvalidKeyFormat)?;
        }
        return Ok(EncryptionKey::new(key_bytes));
    }

    // 2. Otherwise derive key using HKDF-SHA256 from the passphrase
    struct OkmKey;
    impl hkdf::KeyType for OkmKey {
        fn len(&self) -> usize {
            32
        }
    }

    let salt = hkdf::Salt::new(hkdf::HKDF_SHA256, salt);
    let prk = salt.extract(trimmed.as_bytes());
    let mut okm_bytes = [0u8; 32];
    prk.expand(&[HKDF_INFO], OkmKey)
        .map_err(|e| CryptoError::KeyDerivationFailed(format!("{e:?}")))?
        .fill(&mut okm_bytes)
        .map_err(|e| CryptoError::KeyDerivationFailed(format!("{e:?}")))?;

    Ok(EncryptionKey::new(okm_bytes))
}

/// Reads a master key file (raw 32 bytes or 64 hex chars or passphrase string).
pub fn load_key_from_file(
    path: &Path,
    salt: &[u8; SALT_LEN],
) -> Result<EncryptionKey, CryptoError> {
    let mut file = File::open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;

    if buf.len() == 32 {
        let mut raw = [0u8; 32];
        raw.copy_from_slice(&buf);
        return Ok(EncryptionKey::new(raw));
    }

    let text = String::from_utf8_lossy(&buf);
    derive_key(&text, salt)
}

/// Computes the deterministic 96-bit nonce for chunk `chunk_idx`:
/// `Nonce_i = MasterNonce ^ chunk_idx` (preventing nonce reuse while allowing random chunk access).
#[inline]
pub fn derive_chunk_nonce(master_nonce: &[u8; NONCE_LEN], chunk_idx: u32) -> [u8; NONCE_LEN] {
    let mut nonce = *master_nonce;
    let base_counter = u32::from_le_bytes([nonce[8], nonce[9], nonce[10], nonce[11]]);
    let chunk_counter = base_counter ^ chunk_idx;
    nonce[8..12].copy_from_slice(&chunk_counter.to_le_bytes());
    nonce
}

/// Streaming AEAD Encryptor.
pub struct ChunkEncryptor {
    key: LessSafeKey,
    master_nonce: [u8; NONCE_LEN],
    chunk_index: u32,
}

impl ChunkEncryptor {
    /// Creates a new `ChunkEncryptor` with the specified cipher, key, and master nonce.
    pub fn new(
        cipher: CipherSuite,
        key: &EncryptionKey,
        master_nonce: [u8; NONCE_LEN],
    ) -> Result<Self, CryptoError> {
        let unbound_key = UnboundKey::new(cipher.algorithm(), key.as_bytes())
            .map_err(|_| CryptoError::AuthenticationFailed)?;
        let key = LessSafeKey::new(unbound_key);

        Ok(Self {
            key,
            master_nonce,
            chunk_index: 0,
        })
    }

    /// Encrypts a chunk payload in-place and returns the ciphertext bytes (including 16-byte auth tag).
    pub fn encrypt_chunk(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let nonce_bytes = derive_chunk_nonce(&self.master_nonce, self.chunk_index);
        let nonce = Nonce::try_assume_unique_for_key(&nonce_bytes)
            .map_err(|_| CryptoError::AuthenticationFailed)?;

        let mut in_out = plaintext.to_vec();
        let aad = Aad::from(self.chunk_index.to_le_bytes());

        self.key
            .seal_in_place_append_tag(nonce, aad, &mut in_out)
            .map_err(|_| CryptoError::AuthenticationFailed)?;

        self.chunk_index += 1;
        Ok(in_out)
    }
}

/// Streaming AEAD Decryptor.
pub struct ChunkDecryptor {
    key: LessSafeKey,
    master_nonce: [u8; NONCE_LEN],
    chunk_index: u32,
}

impl ChunkDecryptor {
    /// Creates a new `ChunkDecryptor` with the specified cipher, key, and master nonce.
    pub fn new(
        cipher: CipherSuite,
        key: &EncryptionKey,
        master_nonce: [u8; NONCE_LEN],
    ) -> Result<Self, CryptoError> {
        let unbound_key = UnboundKey::new(cipher.algorithm(), key.as_bytes())
            .map_err(|_| CryptoError::AuthenticationFailed)?;
        let key = LessSafeKey::new(unbound_key);

        Ok(Self {
            key,
            master_nonce,
            chunk_index: 0,
        })
    }

    /// Decrypts a chunk in-place and authenticates the 16-byte tag.
    pub fn decrypt_chunk(&mut self, ciphertext_with_tag: &[u8]) -> Result<Vec<u8>, CryptoError> {
        if ciphertext_with_tag.len() < TAG_LEN {
            return Err(CryptoError::AuthenticationFailed);
        }

        let nonce_bytes = derive_chunk_nonce(&self.master_nonce, self.chunk_index);
        let nonce = Nonce::try_assume_unique_for_key(&nonce_bytes)
            .map_err(|_| CryptoError::AuthenticationFailed)?;

        let mut in_out = ciphertext_with_tag.to_vec();
        let aad = Aad::from(self.chunk_index.to_le_bytes());

        let plaintext = self
            .key
            .open_in_place(nonce, aad, &mut in_out)
            .map_err(|_| CryptoError::AuthenticationFailed)?;

        let result = plaintext.to_vec();
        self.chunk_index += 1;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cipher_suite_parsing() {
        assert_eq!(
            CipherSuite::from_str_name("aes-256-gcm").unwrap(),
            CipherSuite::Aes256Gcm
        );
        assert_eq!(
            CipherSuite::from_str_name("chacha20-poly1305").unwrap(),
            CipherSuite::ChaCha20Poly1305
        );
        assert!(CipherSuite::from_str_name("des").is_err());
    }

    #[test]
    fn test_key_derivation_hex_vs_passphrase() {
        let salt = [0x42u8; 32];
        let hex_key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let k1 = derive_key(hex_key, &salt).unwrap();
        assert_eq!(k1.as_bytes()[0], 0x01);
        assert_eq!(k1.as_bytes()[31], 0xef);

        let pass_key = derive_key("my-secret-passphrase", &salt).unwrap();
        assert_eq!(pass_key.as_bytes().len(), 32);
    }

    #[test]
    fn test_aes_gcm_chunk_encrypt_decrypt_roundtrip() {
        let salt = [1u8; 32];
        let key = derive_key("super-secure-password-12345", &salt).unwrap();
        let mut master_nonce = [0u8; 12];
        generate_random_bytes(&mut master_nonce).unwrap();

        let mut encryptor =
            ChunkEncryptor::new(CipherSuite::Aes256Gcm, &key, master_nonce).unwrap();
        let mut decryptor =
            ChunkDecryptor::new(CipherSuite::Aes256Gcm, &key, master_nonce).unwrap();

        let p1 = b"Hello, KacheDB Snapshot Chunk 0!";
        let p2 = b"Vector embeddings payload in Chunk 1.";

        let c1 = encryptor.encrypt_chunk(p1).unwrap();
        let c2 = encryptor.encrypt_chunk(p2).unwrap();

        assert_eq!(c1.len(), p1.len() + TAG_LEN);
        assert_eq!(c2.len(), p2.len() + TAG_LEN);

        let d1 = decryptor.decrypt_chunk(&c1).unwrap();
        let d2 = decryptor.decrypt_chunk(&c2).unwrap();

        assert_eq!(d1, p1);
        assert_eq!(d2, p2);
    }

    #[test]
    fn test_chacha20_chunk_encrypt_decrypt_roundtrip() {
        let salt = [2u8; 32];
        let key = derive_key("chacha-password", &salt).unwrap();
        let master_nonce = [7u8; 12];

        let mut encryptor =
            ChunkEncryptor::new(CipherSuite::ChaCha20Poly1305, &key, master_nonce).unwrap();
        let mut decryptor =
            ChunkDecryptor::new(CipherSuite::ChaCha20Poly1305, &key, master_nonce).unwrap();

        let data = vec![0xABu8; 1000];
        let encrypted = encryptor.encrypt_chunk(&data).unwrap();
        let decrypted = decryptor.decrypt_chunk(&encrypted).unwrap();
        assert_eq!(decrypted, data);
    }

    #[test]
    fn test_tamper_detection_fails_authentication() {
        let salt = [3u8; 32];
        let key = derive_key("tamper-proof", &salt).unwrap();
        let master_nonce = [9u8; 12];

        let mut encryptor =
            ChunkEncryptor::new(CipherSuite::Aes256Gcm, &key, master_nonce).unwrap();
        let mut decryptor =
            ChunkDecryptor::new(CipherSuite::Aes256Gcm, &key, master_nonce).unwrap();

        let mut encrypted = encryptor.encrypt_chunk(b"Sensitive payload").unwrap();

        // Flip a bit in ciphertext
        encrypted[5] ^= 0xFF;

        let err = decryptor.decrypt_chunk(&encrypted).unwrap_err();
        assert!(matches!(err, CryptoError::AuthenticationFailed));
    }

    #[test]
    fn test_wrong_key_fails_authentication() {
        let salt = [4u8; 32];
        let key1 = derive_key("correct-key", &salt).unwrap();
        let key2 = derive_key("wrong-key", &salt).unwrap();
        let master_nonce = [11u8; 12];

        let mut encryptor =
            ChunkEncryptor::new(CipherSuite::Aes256Gcm, &key1, master_nonce).unwrap();
        let mut decryptor =
            ChunkDecryptor::new(CipherSuite::Aes256Gcm, &key2, master_nonce).unwrap();

        let encrypted = encryptor.encrypt_chunk(b"Sensitive payload").unwrap();
        let err = decryptor.decrypt_chunk(&encrypted).unwrap_err();
        assert!(matches!(err, CryptoError::AuthenticationFailed));
    }
}
