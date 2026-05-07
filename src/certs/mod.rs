use crate::error::{DockerProxyError, Result};
use chrono::{DateTime, Utc};
use rcgen::{CertificateParams, DistinguishedName, KeyPair};
use std::time::{Duration, SystemTime};

/// Certificate bundle containing all certificates and keys
#[derive(Debug, Clone)]
pub struct CertificateBundle {
    pub ca_cert_pem: String,
    pub ca_key_pem: String,
    pub server_cert_pem: String,
    pub server_key_pem: String,
    pub ca_expiry: DateTime<Utc>,
    pub server_expiry: DateTime<Utc>,
}

/// Generate CA certificate and key (valid 10 years)
fn generate_ca() -> Result<(String, String, DateTime<Utc>)> {
    // Create certificate parameters
    let mut params = CertificateParams::new(vec![]).map_err(|e| {
        DockerProxyError::Tls(format!("Failed to create certificate params: {}", e))
    })?;

    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "Docker Proxy CA");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);

    // 10 years validity
    let not_before = SystemTime::now();
    let not_after = not_before + Duration::from_secs(10 * 365 * 24 * 60 * 60);
    params.not_before = not_before.into();
    params.not_after = not_after.into();

    // Generate key pair first
    let ca_key_pair = KeyPair::generate()
        .map_err(|e| DockerProxyError::Tls(format!("Failed to generate CA key pair: {}", e)))?;

    // Generate self-signed CA certificate
    let cert = params
        .self_signed(&ca_key_pair)
        .map_err(|e| DockerProxyError::Tls(format!("Failed to generate CA certificate: {}", e)))?;

    let ca_cert_pem = cert.pem();
    let ca_key_pem = ca_key_pair.serialize_pem();

    let expiry = DateTime::from_timestamp(
        not_after
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| DockerProxyError::Tls(format!("Failed to calculate expiry: {}", e)))?
            .as_secs() as i64,
        0,
    )
    .ok_or_else(|| DockerProxyError::Tls("Invalid expiry timestamp".to_string()))?;

    Ok((ca_cert_pem, ca_key_pem, expiry))
}

/// Generate server certificate signed by CA (valid 1 year)
/// CA cert is recreated from the key for signing
fn generate_server_cert(
    _ca_cert_pem: &str,
    ca_key_pem: &str,
    hostnames: Vec<String>,
    ip_addresses: Vec<String>,
) -> Result<(String, String, DateTime<Utc>)> {
    // Parse CA key
    let ca_key_pair = KeyPair::from_pem(ca_key_pem)
        .map_err(|e| DockerProxyError::Tls(format!("Failed to parse CA key: {}", e)))?;

    // Reconstruct CA certificate for signing
    // We need to recreate the CA cert with the same key to sign server certs
    let mut ca_params = CertificateParams::new(vec![])
        .map_err(|e| DockerProxyError::Tls(format!("Failed to create CA params: {}", e)))?;
    ca_params.distinguished_name = DistinguishedName::new();
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "Docker Proxy CA");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);

    // Recreate CA cert with the stored key for signing
    let ca_cert = ca_params.self_signed(&ca_key_pair).map_err(|e| {
        DockerProxyError::Tls(format!("Failed to recreate CA cert for signing: {}", e))
    })?;

    // Create server certificate parameters
    let mut params = CertificateParams::new(hostnames.clone())
        .map_err(|e| DockerProxyError::Tls(format!("Failed to create server params: {}", e)))?;
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "pulld");

    // Add IP addresses to SAN
    for ip in &ip_addresses {
        params
            .subject_alt_names
            .push(rcgen::SanType::IpAddress(ip.parse().map_err(|e| {
                DockerProxyError::Tls(format!("Invalid IP address {}: {}", ip, e))
            })?));
    }

    // 1 year validity
    let not_before = SystemTime::now();
    let not_after = not_before + Duration::from_secs(365 * 24 * 60 * 60);
    params.not_before = not_before.into();
    params.not_after = not_after.into();

    // Generate server key pair
    let server_key_pair = KeyPair::generate()
        .map_err(|e| DockerProxyError::Tls(format!("Failed to generate server key pair: {}", e)))?;

    // Sign server certificate with CA using signed_by method
    let server_cert = params
        .signed_by(&server_key_pair, &ca_cert, &ca_key_pair)
        .map_err(|e| DockerProxyError::Tls(format!("Failed to sign server certificate: {}", e)))?;

    let server_cert_pem = server_cert.pem();
    let server_key_pem = server_key_pair.serialize_pem();

    let expiry = DateTime::from_timestamp(
        not_after
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| DockerProxyError::Tls(format!("Failed to calculate expiry: {}", e)))?
            .as_secs() as i64,
        0,
    )
    .ok_or_else(|| DockerProxyError::Tls("Invalid expiry timestamp".to_string()))?;

    Ok((server_cert_pem, server_key_pem, expiry))
}

/// Check if certificate needs renewal
/// CA: renew if <1 year remaining
/// Server: renew if <30 days remaining
fn needs_renewal(expiry: DateTime<Utc>, threshold_days: i64) -> bool {
    let now = Utc::now();
    let days_remaining = (expiry - now).num_days();
    days_remaining < threshold_days
}

/// Ensure certificates exist and are valid
/// This is the main entry point - it will be called with vault access callbacks
pub fn ensure_certificates<F1, F2>(
    load_from_vault: F1,
    store_to_vault: F2,
    hostnames: Vec<String>,
    ip_addresses: Vec<String>,
) -> Result<CertificateBundle>
where
    F1: FnOnce() -> Result<Option<CertificateBundle>>,
    F2: Fn(&CertificateBundle) -> Result<()>,
{
    // Try to load from vault
    let mut bundle = match load_from_vault()? {
        Some(b) => b,
        None => {
            // Generate new certificates
            tracing::info!("[pulld] Generating new CA certificate...");
            let (ca_cert_pem, ca_key_pem, ca_expiry) = generate_ca()?;

            tracing::info!("[pulld] Generating new server certificate...");
            let (server_cert_pem, server_key_pem, server_expiry) = generate_server_cert(
                &ca_cert_pem,
                &ca_key_pem,
                hostnames.clone(),
                ip_addresses.clone(),
            )?;

            let new_bundle = CertificateBundle {
                ca_cert_pem,
                ca_key_pem,
                server_cert_pem,
                server_key_pem,
                ca_expiry,
                server_expiry,
            };

            // Store in vault
            store_to_vault(&new_bundle)?;
            tracing::info!("[pulld] Certificates generated and stored in vault");
            new_bundle
        }
    };

    // Check if CA needs renewal (<1 year remaining)
    if needs_renewal(bundle.ca_expiry, 365) {
        tracing::info!("[pulld] CA certificate expires soon, renewing...");
        let (ca_cert_pem, ca_key_pem, ca_expiry) = generate_ca()?;

        // Regenerate server cert with new CA
        let (server_cert_pem, server_key_pem, server_expiry) = generate_server_cert(
            &ca_cert_pem,
            &ca_key_pem,
            hostnames.clone(),
            ip_addresses.clone(),
        )?;

        bundle = CertificateBundle {
            ca_cert_pem,
            ca_key_pem,
            server_cert_pem,
            server_key_pem,
            ca_expiry,
            server_expiry,
        };

        store_to_vault(&bundle)?;
        tracing::info!("[pulld] CA certificate renewed");
    }
    // Check if server cert needs renewal (<30 days remaining)
    else if needs_renewal(bundle.server_expiry, 30) {
        tracing::info!("[pulld] Server certificate expires soon, renewing...");
        let (server_cert_pem, server_key_pem, server_expiry) = generate_server_cert(
            &bundle.ca_cert_pem,
            &bundle.ca_key_pem,
            hostnames.clone(),
            ip_addresses.clone(),
        )?;

        bundle.server_cert_pem = server_cert_pem;
        bundle.server_key_pem = server_key_pem;
        bundle.server_expiry = server_expiry;

        store_to_vault(&bundle)?;
        tracing::info!("[pulld] Server certificate renewed");
    }

    Ok(bundle)
}

/// Vault key constants (exported for use in runtime service)
pub const VAULT_KEY_CA_CERT: &str = "pulld_ca_cert";
pub const VAULT_KEY_CA_KEY: &str = "pulld_ca_key";
pub const VAULT_KEY_SERVER_CERT: &str = "pulld_server_cert";
pub const VAULT_KEY_SERVER_KEY: &str = "pulld_server_key";
pub const VAULT_KEY_CA_EXPIRY: &str = "pulld_ca_expiry";
pub const VAULT_KEY_SERVER_EXPIRY: &str = "pulld_server_expiry";
