pub mod cache;
pub mod certs;
pub mod config;
pub mod dns;
pub mod error;
pub mod helm;
pub mod prepull;
pub mod registry;
pub mod server;
pub mod tls;

pub use cache::CacheStorage;
pub use certs::{ensure_certificates, CertificateBundle};
pub use config::{Config, MirrorStrategy, RegistryConfig};
pub use error::{DockerProxyError, Result};

/// Start the pulld server with the given configuration
pub async fn start_server(
    cache_dir: std::path::PathBuf,
    config: Config,
    server_cert_pem: Option<String>,
    server_key_pem: Option<String>,
) -> Result<tokio::task::JoinHandle<()>> {
    server::start_server(cache_dir, config, server_cert_pem, server_key_pem).await
}

/// Run pre-pull based on configuration
pub async fn run_pre_pull(cache_dir: std::path::PathBuf, config: &Config) -> Result<()> {
    prepull::run_pre_pull(cache_dir, config).await
}

// ---------------------------------------------------------------------------
// PulldService — host-level singleton that owns the pulld server and
// pre-pull lifecycle.  Used by 4lock-agent on all platforms.
// ---------------------------------------------------------------------------

use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

static PULLD_SERVICE: OnceLock<PulldService> = OnceLock::new();

pub struct PulldService {
    cache_dir: PathBuf,
    config: Config,
    server_handle: Mutex<Option<JoinHandle<()>>>,
}

impl PulldService {
    /// Initialize the singleton with the given app directory.
    /// Creates `<app_dir>/pulld-cache` if it does not exist.
    pub fn init_singleton(app_dir: PathBuf) {
        let cache_dir = app_dir.join("pulld-cache");
        if let Err(e) = std::fs::create_dir_all(&cache_dir) {
            tracing::error!("[PulldService] Failed to create pulld-cache dir: {}", e);
        }
        PULLD_SERVICE.get_or_init(|| PulldService {
            cache_dir,
            config: Config::default(),
            server_handle: Mutex::new(None),
        });
    }

    /// Get the singleton (panics if `init_singleton` was not called).
    pub fn instance() -> &'static PulldService {
        PULLD_SERVICE
            .get()
            .expect("PulldService not initialized — call init_singleton() first")
    }

    /// Start the pulld HTTP server on port 5050 (plain HTTP, no TLS).
    /// Blocks until the health endpoint responds or `timeout` expires.
    pub async fn start(&self) -> std::result::Result<(), String> {
        let cache_dir = self.cache_dir.clone();
        let config = self.config.clone();

        let handle = start_server(cache_dir, config, None, None)
            .await
            .map_err(|e| format!("Pulld server failed to start: {}", e))?;

        *self.server_handle.lock().await = Some(handle);

        self.wait_for_healthy(Duration::from_secs(15)).await?;
        tracing::info!("[PulldService] Pulld server started on 0.0.0.0:5050");
        Ok(())
    }

    /// Kick off background pre-pull (returns immediately).
    pub fn start_pre_pull(&self) {
        let cache_dir = self.cache_dir.clone();
        let config = self.config.clone();
        tokio::spawn(async move {
            tracing::info!("[PulldService] Starting background pre-pull...");
            match run_pre_pull(cache_dir, &config).await {
                Ok(()) => tracing::info!("[PulldService] Pre-pull completed successfully"),
                Err(e) => tracing::warn!("[PulldService] Pre-pull completed with errors: {}", e),
            }
        });
    }

    /// Returns `true` when the server's `/health` endpoint is reachable.
    pub async fn health_check(&self) -> bool {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(500))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        client
            .get("http://127.0.0.1:5050/health")
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }

    /// Abort the server task.
    pub async fn stop(&self) {
        if let Some(handle) = self.server_handle.lock().await.take() {
            handle.abort();
            tracing::info!("[PulldService] Pulld server stopped");
        }
    }

    /// Poll `/health` until ready or timeout.
    async fn wait_for_healthy(&self, timeout: Duration) -> std::result::Result<(), String> {
        let deadline = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < deadline {
            if self.health_check().await {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Err(format!(
            "Pulld server did not become healthy within {}ms",
            timeout.as_millis()
        ))
    }
}
