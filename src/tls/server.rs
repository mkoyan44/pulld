use crate::config::TlsConfig;
use crate::error::{DockerProxyError, Result};
use axum_server::tls_rustls::RustlsConfig;
use rustls::crypto::{ring::default_provider, CryptoProvider};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls_pemfile::{certs, private_key};
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor};
use std::path::Path;
use std::sync::Arc;

fn parse_private_key(mut reader: impl BufRead) -> Result<PrivateKeyDer<'static>> {
    private_key(&mut reader)
        .map_err(|e| DockerProxyError::Tls(format!("Failed to parse private key: {}", e)))?
        .ok_or_else(|| DockerProxyError::Tls("No private keys found".to_string()))
}

/// Create TLS config from PEM strings (from vault)
pub async fn create_server_tls_config_from_pem(
    server_cert_pem: &str,
    server_key_pem: &str,
) -> Result<RustlsConfig> {
    // Ensure crypto provider is installed (rustls 0.23+ requirement)
    let _ = CryptoProvider::install_default(default_provider());

    // Parse certificate from PEM string
    let mut cert_reader = Cursor::new(server_cert_pem.as_bytes());
    let cert_chain: Vec<CertificateDer> = certs(&mut cert_reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| DockerProxyError::Tls(format!("Failed to parse certificates: {}", e)))?;

    if cert_chain.is_empty() {
        return Err(DockerProxyError::Tls("No certificates found".to_string()));
    }

    // Parse private key from PEM string
    let key = parse_private_key(Cursor::new(server_key_pem.as_bytes()))?;

    // Build TLS config (no client auth)
    let rustls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .map_err(|e| DockerProxyError::Tls(format!("Failed to build TLS config: {}", e)))?;

    Ok(RustlsConfig::from_config(Arc::new(rustls_config)))
}

/// Create TLS config from file paths (backward compatibility)
pub async fn create_server_tls_config(tls_config: &TlsConfig) -> Result<RustlsConfig> {
    // Ensure crypto provider is installed (rustls 0.23+ requirement)
    let _ = CryptoProvider::install_default(default_provider());

    // If cert_path and key_path are provided, use file-based loading (backward compatibility)
    if !tls_config.cert_path.is_empty() && !tls_config.key_path.is_empty() {
        let cert_path = Path::new(&tls_config.cert_path);
        let key_path = Path::new(&tls_config.key_path);

        if !cert_path.exists() {
            return Err(DockerProxyError::Tls(format!(
                "Certificate file not found: {}",
                cert_path.display()
            )));
        }

        if !key_path.exists() {
            return Err(DockerProxyError::Tls(format!(
                "Key file not found: {}",
                key_path.display()
            )));
        }

        // Load certificate from file
        let cert_file = File::open(cert_path)
            .map_err(|e| DockerProxyError::Tls(format!("Failed to open cert file: {}", e)))?;
        let mut cert_reader = BufReader::new(cert_file);
        let cert_chain: Vec<CertificateDer> = certs(&mut cert_reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| DockerProxyError::Tls(format!("Failed to parse certificates: {}", e)))?;

        if cert_chain.is_empty() {
            return Err(DockerProxyError::Tls("No certificates found".to_string()));
        }

        // Load private key from file
        let key_file = File::open(key_path)
            .map_err(|e| DockerProxyError::Tls(format!("Failed to open key file: {}", e)))?;
        let key = parse_private_key(BufReader::new(key_file))?;

        // Build TLS config
        let rustls_config = if tls_config.client_auth {
            // Client authentication enabled
            if let Some(ref client_ca_path) = tls_config.client_ca_path {
                let ca_file = File::open(client_ca_path)
                    .map_err(|e| DockerProxyError::Tls(format!("Failed to open CA file: {}", e)))?;
                let mut ca_reader = BufReader::new(ca_file);
                let ca_certs: Vec<CertificateDer> = certs(&mut ca_reader)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| {
                        DockerProxyError::Tls(format!("Failed to parse CA certificates: {}", e))
                    })?;

                let mut root_store = rustls::RootCertStore::empty();
                for cert in &ca_certs {
                    root_store.add(cert.clone()).map_err(|e| {
                        DockerProxyError::Tls(format!("Failed to add CA certificate: {}", e))
                    })?;
                }

                let client_verifier =
                    rustls::server::WebPkiClientVerifier::builder(root_store.into())
                        .build()
                        .map_err(|e| {
                            DockerProxyError::Tls(format!("Failed to build client verifier: {}", e))
                        })?;

                rustls::ServerConfig::builder()
                    .with_client_cert_verifier(client_verifier)
                    .with_single_cert(cert_chain, key)
                    .map_err(|e| {
                        DockerProxyError::Tls(format!("Failed to build TLS config: {}", e))
                    })?
            } else {
                return Err(DockerProxyError::Tls(
                    "client_auth enabled but client_ca_path not specified".to_string(),
                ));
            }
        } else {
            // No client authentication
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(cert_chain, key)
                .map_err(|e| DockerProxyError::Tls(format!("Failed to build TLS config: {}", e)))?
        };

        Ok(RustlsConfig::from_config(Arc::new(rustls_config)))
    } else {
        // No cert paths provided - should use PEM strings from vault instead
        // This function is for backward compatibility only
        Err(DockerProxyError::Tls(
            "TLS enabled but no certificate paths provided. Use create_server_tls_config_from_pem() with vault certificates.".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RSA_PKCS1_PRIVATE_KEY: &str = r#"-----BEGIN RSA PRIVATE KEY-----
MIIBOwIBAAJBAKeVcMWfTRrsqVBOHbEaRoeLxsMLXc19yDEyTc9FuOmIA8hTVs4e
vjWqgVhQ63REq5Tne9YnJIo/zK4uscIwmxkCAwEAAQJAAhQzgvAX98aJzyo46hKG
X3YXcCH69uqhiiKynmiiA5ucNNBjKQ0GZv7vCc/kpvGYbPzQdoJ9xxwSeQ9bESdC
kQIhAM/1D9+tBNpidn8Fjoy4YUZ6G86J85f2J+SqsdVbVfbNAiEAzkyfGrsFR6Lr
kz/jSV+Mr4xkkXxklJOCyHvTuoyJfX0CIEfiUDBjYHAU5R0XUKU3/vgbsYz9hqSa
xEN49avovJhpAiEAx6kYg4JlxcNERCsdCrJTMsOpwbSmk7WAahCOBopltvECIQCr
j+txOvnb0LWjS5bIyhRvVW6AED3TQMv/8p7/nJv7zg==
-----END RSA PRIVATE KEY-----"#;

    #[test]
    fn parse_private_key_accepts_pkcs1_rsa_keys() {
        let key = parse_private_key(Cursor::new(RSA_PKCS1_PRIVATE_KEY.as_bytes())).unwrap();

        assert!(matches!(key, PrivateKeyDer::Pkcs1(_)));
    }
}
