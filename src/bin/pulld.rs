use pulld::{start_server, Config};
use std::path::PathBuf;
use tokio::time::{sleep, Duration};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("pulld=info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cache_dir = PathBuf::from(
        std::env::args()
            .nth(1)
            .unwrap_or_else(|| "./cache/pulld".to_string()),
    );

    // Use compile-time default config with focused environment overrides.
    let mut config = Config::default();
    if let Ok(port) = std::env::var("PULLD_HTTPS_PORT") {
        config.server.port = port.parse()?;
    }
    if let Ok(http_port) = std::env::var("PULLD_HTTP_PORT") {
        config.server.http_port = Some(http_port.parse()?);
    }
    if let (Ok(cert_path), Ok(key_path)) = (
        std::env::var("PULLD_TLS_CERT_PATH"),
        std::env::var("PULLD_TLS_KEY_PATH"),
    ) {
        config.server.tls = Some(pulld::config::TlsConfig {
            enabled: true,
            cert_path,
            key_path,
            client_auth: false,
            client_ca_path: None,
        });
    }

    tracing::info!(
        "Starting pulld server on {}:{}",
        config.server.bind_address,
        config.server.port
    );
    tracing::info!("Cache directory: {:?}", cache_dir);
    tracing::info!("Using compile-time default configuration");
    if config
        .server
        .tls
        .as_ref()
        .map(|tls| tls.enabled)
        .unwrap_or(false)
    {
        tracing::info!(
            "TLS enabled on {}:{}; HTTP port: {:?}",
            config.server.bind_address,
            config.server.port,
            config.server.http_port
        );
    }
    if let Ok(endpoint) = std::env::var("PULLD_ADMISSION_REGISTRY_ENDPOINT") {
        tracing::info!("Admission image rewrite endpoint: {}", endpoint);
    }
    tracing::info!("Server endpoints:");
    tracing::info!(
        "  Health: http://{}:{}/health",
        config.server.bind_address,
        config.server.port
    );
    tracing::info!(
        "  API: http://{}:{}/v2/",
        config.server.bind_address,
        config.server.port
    );
    tracing::info!("Press Ctrl+C to stop the server.");

    // Start server
    let _handle = start_server(cache_dir, config, None, None).await?;

    // Keep running
    loop {
        sleep(Duration::from_secs(1)).await;
    }
}
