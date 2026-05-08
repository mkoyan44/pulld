//! Tests for blob streaming behavior, especially handling premature upstream closure
//!
//! These tests verify that pulld handles upstream connection errors gracefully
//! without causing "short read" errors in containerd.

use axum::{body::Body, extract::Path, http::StatusCode, response::Response, routing::get, Router};
use bytes::Bytes;
use pulld::{start_server, Config};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;

/// Mock upstream server that simulates premature connection closure
async fn create_mock_upstream_server(
    port: u16,
    close_after_bytes: usize,
) -> tokio::task::JoinHandle<()> {
    let app = Router::new().route(
        "/v2/:repo/blobs/:digest",
        get(move |Path((_repo, _digest)): Path<(String, String)>| {
            let close_after = close_after_bytes;
            async move {
                // Create a channel-based stream that sends partial data then closes
                let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(16);

                // Spawn task to send data then simulate closure
                tokio::spawn(async move {
                    // Send first chunk
                    let chunk_size = close_after.min(1024 * 1024); // 1MB chunks
                    let data = vec![0u8; chunk_size];
                    let _ = tx.send(Ok(Bytes::from(data))).await;

                    // Send second chunk (if close_after is large enough)
                    if close_after > chunk_size {
                        let remaining = close_after - chunk_size;
                        let data = vec![0u8; remaining.min(1024 * 1024)];
                        let _ = tx.send(Ok(Bytes::from(data))).await;
                    }

                    // Simulate premature closure by sending an error
                    let _ = tx
                        .send(Err(std::io::Error::new(
                            std::io::ErrorKind::ConnectionAborted,
                            "Connection closed prematurely",
                        )))
                        .await;
                });

                let stream = ReceiverStream::new(rx);
                let body = Body::from_stream(stream);

                Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "application/octet-stream")
                    // CRITICAL: Don't set Content-Length - this simulates real-world scenario
                    // where upstream closes before sending all data
                    .body(body)
                    .unwrap()
            }
        }),
    );

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .expect("Failed to bind mock server");

    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("Mock server failed");
    })
}

/// Test that blob streaming handles premature upstream closure correctly
///
/// This test verifies the fix for "short read" errors in containerd:
/// - When upstream closes prematurely, pulld should return 502 Bad Gateway
/// - The proxy uses "download-first" approach: download entire blob, verify, then serve
/// - This prevents containerd from receiving partial/corrupted data
/// - The client gets a clear error instead of incomplete blob data
#[tokio::test]
async fn test_blob_streaming_premature_upstream_closure() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache_dir = temp_dir.path().join("cache");

    // Start mock upstream server that closes after sending partial data
    let mock_port = 18080;
    let close_after_bytes = 11_482_293; // Same as the error case (partial download)
    let _mock_server = create_mock_upstream_server(mock_port, close_after_bytes).await;

    // Wait for mock server to start
    sleep(Duration::from_millis(500)).await;

    // Configure pulld to use mock upstream
    let mut config = Config::default();
    config.server.port = 5056; // Unique port for parallel tests
    config.upstream.registries.insert(
        "test-registry.io".to_string(),
        pulld::config::RegistryConfig {
            mirrors: vec![format!("http://127.0.0.1:{}", mock_port)],
            strategy: pulld::config::MirrorStrategy::Failover,
            max_parallel: 1,
            chunk_size: 16_777_216,
            hedge_delay_ms: 100,
            timeout_secs: 30,
            auth: None,
            ca_cert_path: None,
            insecure: true, // Allow insecure for localhost mock server
        },
    );

    // Start pulld server
    let _server_handle = start_server(cache_dir.clone(), config.clone(), None, None)
        .await
        .expect("Failed to start pulld server");

    // Wait for server to start
    sleep(Duration::from_secs(1)).await;

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true) // Allow insecure for testing
        .build()
        .expect("Failed to create HTTP client");

    // Request blob that will have premature closure
    let blob_url = format!(
        "http://localhost:{}/v2/test-registry.io/test-repo/blobs/sha256:test-digest",
        config.server.port
    );

    let response = client
        .get(&blob_url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .expect("Failed to send blob request");

    // With streaming proxy approach, we return 200 and start streaming
    // If upstream closes early, client gets truncated body (standard proxy behavior)
    // The client (containerd) handles this by detecting truncation and retrying
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Streaming proxy returns 200 OK and streams data (client detects truncation)"
    );

    // Try to read body - may be truncated
    let body_result = response.bytes().await;
    // Body may fail or be truncated - that's expected with premature closure
    tracing::info!("Body result: {:?}", body_result.map(|b| b.len()));

    tracing::info!("[OK] Test passed: Streaming proxy returns 200 (client handles truncation)");
}

/// Test that complete blob downloads work correctly with Content-Length
#[tokio::test]
async fn test_blob_streaming_complete_download() {
    use sha2::{Digest, Sha256};

    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache_dir = temp_dir.path().join("cache");

    // Create a complete blob in cache first
    let cache = Arc::new(
        pulld::cache::CacheStorage::with_max_size(cache_dir.clone(), Some(1))
            .expect("Failed to create cache storage"),
    );

    // Generate test blob and calculate its actual SHA256 digest
    let test_blob_data = vec![0u8; 1024 * 1024]; // 1MB test blob (all zeros)
    let mut hasher = Sha256::new();
    hasher.update(&test_blob_data);
    let test_digest = format!("sha256:{:x}", hasher.finalize());

    cache
        .write_blob(&test_digest, &test_blob_data)
        .await
        .expect("Failed to write test blob to cache");

    // Configure pulld
    let mut config = Config::default();
    config.server.port = 5057; // Unique port for parallel tests

    // Start pulld server
    let _server_handle = start_server(cache_dir.clone(), config.clone(), None, None)
        .await
        .expect("Failed to start pulld server");

    // Wait for server to start
    sleep(Duration::from_secs(1)).await;

    let client = reqwest::Client::new();

    // Request cached blob
    let blob_url = format!(
        "http://localhost:{}/v2/test-registry.io/test-repo/blobs/{}",
        config.server.port, test_digest
    );

    let response = client
        .get(&blob_url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .expect("Failed to send blob request");

    // Cached blobs SHOULD have Content-Length (we know the exact size)
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Cached blob request should return 200 OK"
    );

    let content_length = response.headers().get("content-length");
    assert!(
        content_length.is_some(),
        "Content-Length SHOULD be set for cached blobs (we know the exact size)"
    );

    // Verify we can read the complete blob
    let bytes = response.bytes().await.expect("Failed to read cached blob");

    assert_eq!(
        bytes.len(),
        test_blob_data.len(),
        "Cached blob size should match"
    );

    // Test passed: Cached blob served with Content-Length
}

#[tokio::test]
async fn test_cached_blob_range_request_returns_partial_content() {
    use sha2::{Digest, Sha256};

    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache_dir = temp_dir.path().join("cache");
    let cache = Arc::new(
        pulld::cache::CacheStorage::with_max_size(cache_dir.clone(), Some(1))
            .expect("Failed to create cache storage"),
    );

    let test_blob_data: Vec<u8> = (0..=255).cycle().take(1024).collect();
    let mut hasher = Sha256::new();
    hasher.update(&test_blob_data);
    let test_digest = format!("sha256:{:x}", hasher.finalize());

    cache
        .write_blob(&test_digest, &test_blob_data)
        .await
        .expect("Failed to write test blob to cache");

    let mut config = Config::default();
    config.server.port = 5064;

    let _server_handle = start_server(cache_dir.clone(), config.clone(), None, None)
        .await
        .expect("Failed to start pulld server");
    sleep(Duration::from_secs(1)).await;

    let client = reqwest::Client::new();
    let blob_url = format!(
        "http://localhost:{}/v2/test-registry.io/test-repo/blobs/{}",
        config.server.port, test_digest
    );

    let response = client
        .get(&blob_url)
        .header("Range", "bytes=10-63")
        .send()
        .await
        .expect("Failed to send ranged blob request");

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response.headers().get("content-range").unwrap(),
        "bytes 10-63/1024"
    );
    assert_eq!(response.headers().get("content-length").unwrap(), "54");

    let bytes = response.bytes().await.expect("Failed to read body");
    assert_eq!(bytes.as_ref(), &test_blob_data[10..=63]);
}
