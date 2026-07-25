use std::{io::Cursor, sync::Arc};

use rustls::{
    ServerConfig,
    crypto::ring,
    version::{TLS12, TLS13},
};
use thiserror::Error;

/// Builds the server side of an AMQPS listener from PEM-encoded credentials.
///
/// Service Bus port 5671 wraps the TCP stream in TLS before any AMQP protocol
/// header is exchanged. The listener uses this configuration outside fe2o3 so
/// it follows that ordering rather than AMQP's in-band TLS upgrade.
pub fn tls_server_config(
    certificate_chain_pem: &[u8],
    private_key_pem: &[u8],
) -> Result<ServerConfig, TlsConfigurationError> {
    let certificate_chain = rustls_pemfile::certs(&mut Cursor::new(certificate_chain_pem))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| TlsConfigurationError::MalformedCertificate(error.to_string()))?;
    if certificate_chain.is_empty() {
        return Err(TlsConfigurationError::MissingCertificate);
    }

    let private_key = rustls_pemfile::private_key(&mut Cursor::new(private_key_pem))
        .map_err(|error| TlsConfigurationError::MalformedPrivateKey(error.to_string()))?
        .ok_or(TlsConfigurationError::MissingPrivateKey)?;

    ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_protocol_versions(&[&TLS13, &TLS12])
        .map_err(|error| TlsConfigurationError::InvalidIdentity(error.to_string()))?
        .with_no_client_auth()
        .with_single_cert(certificate_chain, private_key)
        .map_err(|error| TlsConfigurationError::InvalidIdentity(error.to_string()))
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TlsConfigurationError {
    #[error("the TLS certificate file contains no certificates")]
    MissingCertificate,
    #[error("the TLS private-key file contains no private key")]
    MissingPrivateKey,
    #[error("the TLS certificate PEM is malformed: {0}")]
    MalformedCertificate(String),
    #[error("the TLS private-key PEM is malformed: {0}")]
    MalformedPrivateKey(String),
    #[error("the TLS certificate and private key do not form a usable identity: {0}")]
    InvalidIdentity(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_certificate_chain_is_refused() {
        assert_eq!(
            tls_server_config(b"", b"").unwrap_err(),
            TlsConfigurationError::MissingCertificate
        );
    }

    #[test]
    fn a_certificate_without_a_private_key_is_refused() {
        let certificate = b"-----BEGIN CERTIFICATE-----\nAA==\n-----END CERTIFICATE-----\n";
        assert_eq!(
            tls_server_config(certificate, b"").unwrap_err(),
            TlsConfigurationError::MissingPrivateKey
        );
    }
}
