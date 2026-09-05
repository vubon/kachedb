//! `kachedb-net` — TLS 1.3 encryption and configuration engine.
//!
//! Provides TLS termination using `rustls` (Ring crypto provider), supporting standard
//! server authentication and mutual TLS (mTLS) with client certificates.

use std::fs::File;
use std::io::{BufReader, Error as IoError, ErrorKind};
use std::path::Path;
use std::sync::Arc;

pub use rustls::ServerConnection;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
pub use rustls::server::ServerConfig;

/// Initializes the default crypto provider (Ring) if not already set.
pub fn init_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Loads a TLS server configuration from PEM certificate and key files.
pub fn load_server_config(
    cert_path: &Path,
    key_path: &Path,
    ca_path: Option<&Path>,
) -> Result<Arc<ServerConfig>, IoError> {
    init_crypto_provider();

    let cert_file = File::open(cert_path).map_err(|e| {
        IoError::new(
            ErrorKind::NotFound,
            format!("TLS certificate file not found at {cert_path:?}: {e}"),
        )
    })?;
    let mut reader = BufReader::new(cert_file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            IoError::new(
                ErrorKind::InvalidInput,
                format!("Failed to parse cert: {e}"),
            )
        })?;

    if certs.is_empty() {
        return Err(IoError::new(
            ErrorKind::InvalidInput,
            "No valid certificates found in certificate file",
        ));
    }

    let key_file = File::open(key_path).map_err(|e| {
        IoError::new(
            ErrorKind::NotFound,
            format!("TLS private key file not found at {key_path:?}: {e}"),
        )
    })?;
    let mut reader = BufReader::new(key_file);
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut reader)
        .map_err(|e| IoError::new(ErrorKind::InvalidInput, format!("Failed to parse key: {e}")))?
        .ok_or_else(|| {
            IoError::new(
                ErrorKind::InvalidInput,
                "No valid private key found in key file",
            )
        })?;

    let builder = ServerConfig::builder();

    let config = if let Some(ca) = ca_path {
        let ca_file = File::open(ca).map_err(|e| {
            IoError::new(
                ErrorKind::NotFound,
                format!("TLS CA file not found at {ca:?}: {e}"),
            )
        })?;
        let mut reader = BufReader::new(ca_file);
        let mut root_store = rustls::RootCertStore::empty();
        for cert_result in rustls_pemfile::certs(&mut reader) {
            let cert = cert_result.map_err(|e| {
                IoError::new(ErrorKind::InvalidInput, format!("Invalid CA cert: {e}"))
            })?;
            root_store.add(cert).map_err(|e| {
                IoError::new(ErrorKind::InvalidInput, format!("CA store error: {e}"))
            })?;
        }
        let client_verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| IoError::new(ErrorKind::InvalidInput, format!("Verifier error: {e}")))?;
        builder
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certs, key)
            .map_err(|e| IoError::new(ErrorKind::InvalidInput, format!("Config error: {e}")))?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| IoError::new(ErrorKind::InvalidInput, format!("Config error: {e}")))?
    };

    Ok(Arc::new(config))
}

use crate::connection::Connection;
use crate::error::NetError;

/// Active per-connection TLS 1.3 state wrapping a `rustls::ServerConnection`.
pub struct TlsState {
    pub session: ServerConnection,
}

impl TlsState {
    /// Creates a new TLS state from a shared server configuration.
    pub fn new(config: Arc<ServerConfig>) -> Result<Self, rustls::Error> {
        let session = ServerConnection::new(config)?;
        Ok(Self { session })
    }

    /// Reads encrypted TLS records from the underlying socket, decrypts them,
    /// and feeds the decrypted plaintext into the `Connection` buffer.
    pub fn read_and_decrypt<R: std::io::Read>(
        &mut self,
        stream: &mut R,
        conn: &mut Connection,
    ) -> Result<usize, NetError> {
        match self.session.read_tls(stream) {
            Ok(0) => return Err(NetError::ConnectionClosed),
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(NetError::Io(e)),
        }

        let _ = self
            .session
            .process_new_packets()
            .map_err(|e| NetError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?;

        let mut reader = self.session.reader();
        let bytes_read = match conn.read_from(&mut reader) {
            Ok(n) => n,
            Err(NetError::Io(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => 0,
            Err(e) => return Err(e),
        };

        Ok(bytes_read)
    }

    /// Takes staged plaintext responses from the `Connection` buffer, encrypts them into
    /// TLS records, and writes the ciphertext to the underlying socket.
    pub fn encrypt_and_write<W: std::io::Write>(
        &mut self,
        stream: &mut W,
        conn: &mut Connection,
    ) -> Result<usize, NetError> {
        let mut writer = self.session.writer();
        let _ = conn.flush_to_stream(&mut writer)?;

        let mut written = 0;
        while self.session.wants_write() {
            match self.session.write_tls(stream) {
                Ok(n) => written += n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(NetError::Io(e)),
            }
        }
        Ok(written)
    }

    /// Flushes any pending TLS handshake records to the network stream.
    pub fn flush_handshake<W: std::io::Write>(&mut self, stream: &mut W) -> Result<(), NetError> {
        while self.session.wants_write() {
            match self.session.write_tls(stream) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(NetError::Io(e)),
            }
        }
        Ok(())
    }
}
