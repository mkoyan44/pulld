use pulld::{start_server, Config};
use std::path::PathBuf;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let cache_dir = PathBuf::from(
        std::env::args()
            .nth(1)
            .unwrap_or_else(|| "./cache/pulld".to_string()),
    );

    // Use compile-time default config (no runtime file loading)
    let config = Config::default();

    tracing::info!(
        "Starting pulld server on {}:{}",
        config.server.bind_address,
        config.server.port
    );
    tracing::info!("Cache directory: {:?}", cache_dir);
    tracing::info!("Using compile-time default configuration");
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
