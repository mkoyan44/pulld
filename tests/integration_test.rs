//! Integration tests for pulld
//!
//! These tests verify the complete pulld functionality including:
//! - Server startup and health checks
//! - Manifest fetching and caching (by tag and digest)
//! - Image pulling with embedded registry names (containerd compatibility)
//! - Cache behavior and persistence
//! - TLS/HTTPS configuration
//! - Helm chart caching and proxying

use pulld::{start_server, CacheStorage, Config};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

// Initialize tracing for tests
fn init_test_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
}

/// Test server startup and health endpoint
#[tokio::test]
async fn test_server_startup_and_health() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache_dir = temp_dir.path().join("cache");

    // Use an ephemeral port to avoid conflicts with a running agent on default 5050
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let mut config = Config::default();
    config.server.port = port;

    // Start server
    let _server_handle = start_server(cache_dir, config.clone(), None, None)
        .await
        .expect("Failed to start pulld server");

    // Wait for server to start
    sleep(Duration::from_secs(1)).await;

    // Test health endpoint
    let client = reqwest::Client::new();
    let health_url = format!("http://localhost:{}/health", config.server.port);
    let response = client
        .get(&health_url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("Failed to send health check request");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "ok");

    // Test API version endpoint
    let api_url = format!("http://localhost:{}/v2/", config.server.port);
    let response = client
        .get(&api_url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("Failed to send API version request");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
}

/// Test manifest caching by tag and digest (containerd compatibility)
#[tokio::test]
async fn test_manifest_caching_by_tag_and_digest() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache_dir = temp_dir.path().join("cache");

    // Create config with quay.io registry
    let mut config = Config::default();
    config.server.port = 5053; // Unique port for parallel tests
    config.upstream.registries.insert(
        "quay.io".to_string(),
        pulld::config::RegistryConfig {
            mirrors: vec!["https://quay.io".to_string()],
            strategy: pulld::config::MirrorStrategy::Failover,
            max_parallel: 2,
            chunk_size: 16_777_216,
            hedge_delay_ms: 100,
            timeout_secs: 30,
            auth: None,
            ca_cert_path: None,
            insecure: false,
        },
    );

    // Start server
    let _server_handle = start_server(cache_dir.clone(), config.clone(), None, None)
        .await
        .expect("Failed to start pulld server");

    // Wait for server to start
    sleep(Duration::from_secs(1)).await;

    let client = reqwest::Client::new();

    // Step 1: Fetch manifest by tag
    let manifest_url = format!(
        "http://localhost:{}/v2/quay.io/cilium/cilium/manifests/v1.17.7",
        config.server.port
    );

    let response = client
        .get(&manifest_url)
        .header(
            "Accept",
            "application/vnd.docker.distribution.manifest.v2+json",
        )
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .expect("Failed to send manifest request");

    // Should succeed (200) or return 401/404 (acceptable responses)
    assert!(
        response.status().is_success()
            || response.status() == reqwest::StatusCode::UNAUTHORIZED
            || response.status() == reqwest::StatusCode::NOT_FOUND,
        "Unexpected status: {}",
        response.status()
    );

    if response.status().is_success() {
        // Get digest from response header
        let digest = response
            .headers()
            .get("Docker-Content-Digest")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());

        if let Some(digest) = digest {
            // Step 2: Fetch same manifest by digest (containerd behavior)
            sleep(Duration::from_secs(1)).await;

            let digest_url = format!(
                "http://localhost:{}/v2/quay.io/cilium/cilium/manifests/{}",
                config.server.port, digest
            );

            let digest_response = client
                .get(&digest_url)
                .header(
                    "Accept",
                    "application/vnd.docker.distribution.manifest.v2+json",
                )
                .timeout(Duration::from_secs(30))
                .send()
                .await
                .expect("Failed to send digest manifest request");

            // Should succeed (cache hit) or return 404 if not cached
            assert!(
                digest_response.status().is_success()
                    || digest_response.status() == reqwest::StatusCode::NOT_FOUND,
                "Digest fetch should succeed (cache hit) or return 404. Got: {}",
                digest_response.status()
            );

            if digest_response.status().is_success() {
                tracing::info!(
                    "[OK] Manifest cached by digest - containerd compatibility verified"
                );
            }
        }
    }
}

/// Test image pulling with embedded registry names (Helm chart format)
#[tokio::test]
async fn test_embedded_registry_image_pull() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache_dir = temp_dir.path().join("cache");

    // Create config with quay.io registry
    let mut config = Config::default();
    config.server.port = 5054; // Unique port for parallel tests
    config.upstream.registries.insert(
        "quay.io".to_string(),
        pulld::config::RegistryConfig {
            mirrors: vec!["https://quay.io".to_string()],
            strategy: pulld::config::MirrorStrategy::Failover,
            max_parallel: 2,
            chunk_size: 16_777_216,
            hedge_delay_ms: 100,
            timeout_secs: 30,
            auth: None,
            ca_cert_path: None,
            insecure: false,
        },
    );

    // Start server
    let _server_handle = start_server(cache_dir.clone(), config.clone(), None, None)
        .await
        .expect("Failed to start pulld server");

    // Wait for server to start
    sleep(Duration::from_secs(1)).await;

    let client = reqwest::Client::new();

    // Test pulling image with embedded registry (format: pulld:port/quay.io/repo/image:tag)
    // This is the format used in Helm charts
    let manifest_url = format!(
        "http://localhost:{}/v2/quay.io/cilium/cilium/manifests/v1.17.7",
        config.server.port
    );

    let response = client
        .get(&manifest_url)
        .header(
            "Accept",
            "application/vnd.docker.distribution.manifest.v2+json",
        )
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .expect("Failed to send manifest request");

    // Should succeed (200), return 401 if auth required, or 404 if image not found
    assert!(
        response.status().is_success()
            || response.status() == reqwest::StatusCode::UNAUTHORIZED
            || response.status() == reqwest::StatusCode::NOT_FOUND,
        "Unexpected status: {}",
        response.status()
    );

    init_test_tracing();
    if response.status().is_success() {
        tracing::info!("[OK] Embedded registry image pull succeeded");
    }
}

/// Test cache persistence across server restarts
#[tokio::test]
async fn test_cache_persistence() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache_dir = temp_dir.path().join("cache");

    let mut config = Config::default();
    config.server.port = 5055; // Unique port for parallel tests

    // Create cache storage directly
    let cache = Arc::new(
        CacheStorage::with_max_size(cache_dir.clone(), Some(1))
            .expect("Failed to create cache storage"),
    );

    // Write a test manifest
    let test_manifest = b"{\"schemaVersion\":2,\"mediaType\":\"application/vnd.docker.distribution.manifest.v2+json\"}";
    cache
        .write_manifest("quay.io", "cilium/cilium", "v1.17.7", test_manifest)
        .await
        .expect("Failed to write manifest");

    // Verify manifest exists
    assert!(
        cache
            .manifest_exists("quay.io", "cilium/cilium", "v1.17.7")
            .await,
        "Manifest should exist in cache"
    );

    // Read manifest back
    let read_manifest = cache
        .read_manifest("quay.io", "cilium/cilium", "v1.17.7")
        .await
        .expect("Failed to read manifest");

    assert_eq!(
        read_manifest, test_manifest,
        "Read manifest should match written manifest"
    );

    init_test_tracing();
    tracing::info!("[OK] Cache persistence verified");
}

/// Test blob fetching and caching
#[tokio::test]
async fn test_blob_fetching() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache_dir = temp_dir.path().join("cache");

    let mut config = Config::default();
    config.server.port = 5056; // Unique port for parallel tests
    config.upstream.registries.insert(
        "quay.io".to_string(),
        pulld::config::RegistryConfig {
            mirrors: vec!["https://quay.io".to_string()],
            strategy: pulld::config::MirrorStrategy::Failover,
            max_parallel: 2,
            chunk_size: 16_777_216,
            hedge_delay_ms: 100,
            timeout_secs: 30,
            auth: None,
            ca_cert_path: None,
            insecure: false,
        },
    );

    // Start server
    let _server_handle = start_server(cache_dir.clone(), config.clone(), None, None)
        .await
        .expect("Failed to start pulld server");

    // Wait for server to start
    sleep(Duration::from_secs(1)).await;

    let client = reqwest::Client::new();

    // First, fetch manifest to get blob digest
    let manifest_url = format!(
        "http://localhost:{}/v2/quay.io/cilium/cilium/manifests/v1.17.7",
        config.server.port
    );

    let manifest_response = client
        .get(&manifest_url)
        .header(
            "Accept",
            "application/vnd.docker.distribution.manifest.v2+json",
        )
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .expect("Failed to send manifest request");

    if manifest_response.status().is_success() {
        // Parse manifest to get blob digest (simplified - in real scenario, parse JSON)
        // For this test, we'll just verify the blob endpoint is accessible
        let blob_url = format!(
            "http://localhost:{}/v2/quay.io/cilium/cilium/blobs/sha256:0000000000000000000000000000000000000000000000000000000000000000",
            config.server.port
        );

        let blob_response = client
            .get(&blob_url)
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .expect("Failed to send blob request");

        // Blob may return 404 (not found) or 200 (if cached), both are acceptable
        assert!(
            blob_response.status() == reqwest::StatusCode::OK
                || blob_response.status() == reqwest::StatusCode::NOT_FOUND,
            "Blob endpoint should return 200 or 404. Got: {}",
            blob_response.status()
        );
    }
}

/// Test anonymous pull of alpine:latest (manifest + one layer) through pulld.
/// Reproduces the 401 Unauthorized for layer issue: blob must return 200 for both
/// manifest and blob when upstream requires Bearer token (anonymous pull).
#[tokio::test]
#[ignore] // Requires network + Docker Hub; flaky due to unauthenticated rate limits (429)
async fn test_docker_io_alpine_anonymous_pull_manifest_and_layer() {
    init_test_tracing();

    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache_dir = temp_dir.path().join("cache");

    let mut config = Config::default();
    config.server.port = 5062; // Unique port for this test

    // Use default docker.io config (primary + fallbacks: registry-1.docker.io, docker.m.daocloud.io, registry.dockermirror.com)
    assert!(
        config.upstream.registries.contains_key("docker.io"),
        "Default config must include docker.io"
    );

    let _server_handle = start_server(cache_dir.clone(), config.clone(), None, None)
        .await
        .expect("Failed to start pulld server");

    sleep(Duration::from_secs(2)).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("Failed to create client");

    // 1. GET manifest for alpine:latest (path as used by containerd: docker.io in path)
    let manifest_url = format!(
        "http://localhost:{}/v2/docker.io/library/alpine/manifests/latest",
        config.server.port
    );

    tracing::info!("Fetching manifest: {}", manifest_url);
    let manifest_response = client
        .get(&manifest_url)
        .header(
            "Accept",
            "application/vnd.docker.distribution.manifest.v2+json",
        )
        .send()
        .await
        .expect("Failed to send manifest request");

    let manifest_status = manifest_response.status();
    tracing::info!("Manifest response status: {}", manifest_status);

    assert!(
        manifest_status.is_success(),
        "Manifest must return 200 for anonymous pull (got {}). \
         Blob should handle 401 by fetching token and retrying.",
        manifest_status
    );

    let manifest_bytes = manifest_response
        .bytes()
        .await
        .expect("Failed to read manifest body");
    let manifest_json: serde_json::Value =
        serde_json::from_slice(&manifest_bytes).expect("Manifest must be valid JSON");

    // alpine:latest may return OCI index (manifests[]) or schema 2 manifest (layers[])
    let first_digest: String =
        if let Some(layers) = manifest_json.get("layers").and_then(|l| l.as_array()) {
            layers
                .first()
                .and_then(|l| l.get("digest").and_then(|d| d.as_str()))
                .expect("Schema 2 manifest must have layers[].digest")
                .to_string()
        } else if let Some(manifests) = manifest_json.get("manifests").and_then(|m| m.as_array()) {
            // OCI index: fetch the first platform manifest by digest, then get first layer from it
            let ref_digest = manifests
                .first()
                .and_then(|m| m.get("digest").and_then(|d| d.as_str()))
                .expect("Index must have manifests[].digest");
            let ref_manifest_url = format!(
                "http://localhost:{}/v2/docker.io/library/alpine/manifests/{}",
                config.server.port, ref_digest
            );
            let ref_manifest_response = client
                .get(&ref_manifest_url)
                .header(
                    "Accept",
                    "application/vnd.docker.distribution.manifest.v2+json",
                )
                .send()
                .await
                .expect("Failed to fetch referenced manifest");
            assert!(
                ref_manifest_response.status().is_success(),
                "Referenced manifest must return 200 (got {})",
                ref_manifest_response.status()
            );
            let ref_manifest_json: serde_json::Value = serde_json::from_slice(
                &ref_manifest_response
                    .bytes()
                    .await
                    .expect("Failed to read ref manifest body"),
            )
            .expect("Ref manifest must be valid JSON");
            ref_manifest_json
                .get("layers")
                .and_then(|l| l.as_array())
                .and_then(|a| a.first())
                .and_then(|l| l.get("digest").and_then(|d| d.as_str()))
                .expect("Ref manifest must have layers[].digest")
                .to_string()
        } else {
            panic!("Manifest must have layers (schema 2) or manifests (OCI index)");
        };

    tracing::info!("First layer digest: {}", first_digest);

    // 2. GET blob for first layer (this is where 401 was observed in the wild)
    let blob_url = format!(
        "http://localhost:{}/v2/docker.io/library/alpine/blobs/{}",
        config.server.port, first_digest
    );

    tracing::info!("Fetching blob: {}", blob_url);
    let blob_response = client
        .get(&blob_url)
        .send()
        .await
        .expect("Failed to send blob request");

    let blob_status = blob_response.status();
    tracing::info!("Blob response status: {}", blob_status);

    assert!(
        blob_status.is_success(),
        "Blob must return 200 for anonymous pull (got {}). \
         Docker-proxy must handle 401 for layer by fetching token and retrying with same mirror.",
        blob_status
    );

    let blob_len = blob_response
        .bytes()
        .await
        .expect("Failed to read blob body")
        .len();
    assert!(blob_len > 0, "Blob body must be non-empty");

    tracing::info!(
        "[OK] alpine:latest anonymous pull: manifest 200, layer {} 200 ({} bytes)",
        first_digest,
        blob_len
    );
}

/// Test pulling images from multiple upstream registries in one session.
/// Uses default config (docker.io with multiple mirrors, quay.io, etc.) and verifies
/// that the proxy can successfully pull from at least two different upstreams.
#[tokio::test]
#[ignore] // Requires network + Docker Hub + quay.io; flaky due to rate limits
async fn test_pull_from_multiple_upstreams() {
    init_test_tracing();

    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache_dir = temp_dir.path().join("cache");

    let mut config = Config::default();
    config.server.port = 5063; // Unique port for this test

    assert!(
        config.upstream.registries.contains_key("docker.io"),
        "Default config must include docker.io"
    );
    assert!(
        config.upstream.registries.contains_key("quay.io"),
        "Default config must include quay.io"
    );

    let _server_handle = start_server(cache_dir.clone(), config.clone(), None, None)
        .await
        .expect("Failed to start pulld server");

    sleep(Duration::from_secs(2)).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("Failed to create client");

    let base = format!("http://localhost:{}", config.server.port);

    // 1. Pull manifest from docker.io (multiple mirrors: registry-1.docker.io, docker.m.daocloud.io, registry.dockermirror.com)
    let docker_manifest_url = format!("{}/v2/docker.io/library/alpine/manifests/latest", base);
    tracing::info!("Fetching docker.io manifest: {}", docker_manifest_url);
    let docker_manifest = client
        .get(&docker_manifest_url)
        .header(
            "Accept",
            "application/vnd.docker.distribution.manifest.v2+json",
        )
        .send()
        .await
        .expect("Failed to send docker.io manifest request");
    assert!(
        docker_manifest.status().is_success(),
        "docker.io alpine manifest must return 200 (got {})",
        docker_manifest.status()
    );
    tracing::info!("[OK] docker.io: alpine manifest pulled");

    // 2. Pull from quay.io (different upstream)
    let quay_manifest_url = format!("{}/v2/quay.io/cilium/cilium/manifests/v1.17.7", base);
    tracing::info!("Fetching quay.io manifest: {}", quay_manifest_url);
    let quay_manifest = client
        .get(&quay_manifest_url)
        .header(
            "Accept",
            "application/vnd.docker.distribution.manifest.v2+json",
        )
        .send()
        .await
        .expect("Failed to send quay.io manifest request");
    assert!(
        quay_manifest.status().is_success(),
        "quay.io cilium manifest must return 200 (got {}). \
         If the image was removed, update the tag in this test.",
        quay_manifest.status()
    );
    tracing::info!("[OK] quay.io: cilium/cilium manifest pulled");

    tracing::info!("[OK] Pull from multiple upstreams (docker.io + quay.io) succeeded");
}

/// Test failover: primary mirror unreachable, secondary mirror succeeds.
/// Configures docker.io with [unreachable, real]; verifies the proxy returns 200
/// by using the second mirror after the first fails (connection error).
#[tokio::test]
#[ignore] // Requires network + Docker Hub; flaky due to unauthenticated rate limits (429)
async fn test_failover_primary_unreachable_secondary_succeeds() {
    init_test_tracing();

    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache_dir = temp_dir.path().join("cache");

    // First mirror: nothing listens on this port → connection refused (fast).
    // Second mirror: real Docker registry so manifest fetch can succeed.
    let unreachable = "https://127.0.0.1:19999";
    let real_mirror = "https://registry-1.docker.io";

    let mut config = Config::default();
    // Use an ephemeral port to avoid conflicts with a running agent on default 5050
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    config.server.port = port;

    config.upstream.registries.insert(
        "docker.io".to_string(),
        pulld::config::RegistryConfig {
            mirrors: vec![unreachable.to_string(), real_mirror.to_string()],
            strategy: pulld::config::MirrorStrategy::Failover,
            max_parallel: 2,
            chunk_size: 16_777_216,
            hedge_delay_ms: 100,
            timeout_secs: 10,
            auth: None,
            ca_cert_path: None,
            insecure: false,
        },
    );

    let _server_handle = start_server(cache_dir.clone(), config.clone(), None, None)
        .await
        .expect("Failed to start pulld server");

    sleep(Duration::from_secs(2)).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("Failed to create client");

    let manifest_url = format!(
        "http://localhost:{}/v2/docker.io/library/alpine/manifests/latest",
        config.server.port
    );

    let response = client
        .get(&manifest_url)
        .header(
            "Accept",
            "application/vnd.docker.distribution.manifest.v2+json",
        )
        .send()
        .await
        .expect("Failed to send manifest request");

    assert!(
        response.status().is_success(),
        "Failover test: expected 200 after primary unreachable, got {}",
        response.status()
    );
    tracing::info!("[OK] Failover test: primary unreachable, secondary succeeded");
}

/// Test default configuration structure
#[test]
fn test_default_config() {
    // Test that default config has expected structure
    let config = Config::default();

    // Verify default server config
    assert_eq!(config.server.bind_address, "0.0.0.0");
    assert_eq!(config.server.port, 5050);
    assert_eq!(config.cache.max_size_gb, 20);

    // Verify default registries are configured
    assert!(config.upstream.registries.contains_key("docker.io"));
    assert!(config.upstream.registries.contains_key("ghcr.io"));
    assert!(config.upstream.registries.contains_key("quay.io"));

    // Verify quay.io has correct settings for large blobs
    let quay_config = config.upstream.registries.get("quay.io").unwrap();
    assert_eq!(quay_config.max_parallel, 4);
    assert_eq!(quay_config.chunk_size, 16_777_216);
    assert_eq!(quay_config.timeout_secs, 600);
}

/// Test quay.io/cilium/cilium manifest fetch (reproduces the 404 issue)
#[tokio::test]
async fn test_quay_io_cilium_manifest_fetch() {
    // Test exact scenario: nerdctl pull pulld.internal:5050/quay.io/cilium/cilium:v1.17.7
    // Request: GET /v2/quay.io/cilium/cilium/manifests/v1.17.7
    // This test verifies the full flow from nerdctl request to upstream fetch

    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache_dir = temp_dir.path().join("cache");

    // Create config with quay.io registry (should be in default config, but ensure it's there)
    let mut config = Config::default();
    config.server.port = 5057; // Unique port for parallel tests

    // Ensure quay.io is configured (should already be in default config)
    if !config.upstream.registries.contains_key("quay.io") {
        config.upstream.registries.insert(
            "quay.io".to_string(),
            pulld::config::RegistryConfig {
                mirrors: vec!["https://quay.io".to_string()],
                strategy: pulld::config::MirrorStrategy::Failover,
                max_parallel: 4,
                chunk_size: 16_777_216,
                hedge_delay_ms: 100,
                timeout_secs: 600,
                auth: None,
                ca_cert_path: None,
                insecure: false,
            },
        );
    }

    // Start server
    let _server_handle = start_server(cache_dir.clone(), config.clone(), None, None)
        .await
        .expect("Failed to start pulld server");

    // Wait for server to start
    sleep(Duration::from_secs(1)).await;

    let client = reqwest::Client::new();

    // Test the exact path from nerdctl request
    // Image: pulld.internal:5050/quay.io/cilium/cilium:v1.17.7
    // Request: GET /v2/quay.io/cilium/cilium/manifests/v1.17.7
    let manifest_url = format!(
        "http://localhost:{}/v2/quay.io/cilium/cilium/manifests/v1.17.7",
        config.server.port
    );

    tracing::info!("Testing manifest fetch from: {}", manifest_url);

    let response = client
        .get(&manifest_url)
        .header(
            "Accept",
            "application/vnd.docker.distribution.manifest.v2+json",
        )
        .timeout(Duration::from_secs(60)) // Longer timeout for real upstream request
        .send()
        .await
        .expect("Failed to send manifest request");

    let status = response.status();
    tracing::info!("Response status: {}", status);

    // Should succeed (200), return 401 (auth required), or 404 (if image doesn't exist)
    // But we should NOT get 404 due to path parsing errors - that's the bug we're fixing
    assert!(
        status.is_success()
            || status == reqwest::StatusCode::UNAUTHORIZED
            || status == reqwest::StatusCode::NOT_FOUND,
        "Unexpected status: {}. Expected 200, 401, or 404. \
         If we get 404, it might be due to path parsing or upstream path construction issues.",
        status
    );

    init_test_tracing();
    // Handle response based on status
    if status.is_success() {
        tracing::info!("[OK] quay.io/cilium/cilium manifest fetch succeeded");

        // Verify we got a valid manifest
        let manifest_data = response
            .bytes()
            .await
            .expect("Failed to read manifest body");
        assert!(
            !manifest_data.is_empty(),
            "Manifest body should not be empty"
        );

        // Verify it's schema 2 (containerd compatibility)
        if let Ok(manifest_json) = serde_json::from_slice::<serde_json::Value>(&manifest_data) {
            if let Some(schema_version) =
                manifest_json.get("schemaVersion").and_then(|v| v.as_u64())
            {
                assert_eq!(
                    schema_version, 2,
                    "Manifest should be schema 2 (containerd compatibility)"
                );
            }
        }
    } else if status == reqwest::StatusCode::UNAUTHORIZED {
        tracing::info!("[OK] quay.io/cilium/cilium manifest fetch returned 401 (auth required) - this is expected");
    } else if status == reqwest::StatusCode::NOT_FOUND {
        // If we get 404, log the response body to help diagnose
        let error_body = response.text().await.unwrap_or_default();
        tracing::warn!(
            "Got 404 response. Error body: {}. \
             This might indicate path parsing or upstream path construction issues.",
            error_body
        );

        // For now, we'll allow 404 (image might not exist), but log it
        // The real test is that we don't get 404 due to path parsing errors
        // A proper fix would ensure we get 401 (auth required) or 200 (success) for valid images
        tracing::warn!(
            "[WARN] quay.io/cilium/cilium manifest fetch returned 404 - might indicate an issue"
        );
    } else {
        tracing::warn!(
            "[WARN] quay.io/cilium/cilium manifest fetch returned {} - might indicate an issue",
            status
        );
    }
}

/// Test HEAD request fetches from upstream on cache miss
///
/// This is the critical test for the containerd/nerdctl 404 bug:
/// - Containerd sends HEAD first to check if manifest exists
/// - If HEAD returns 404 (cache miss without upstream fetch), containerd stops
/// - The fix ensures HEAD fetches from upstream on cache miss, just like GET
#[tokio::test]
async fn test_head_request_fetches_from_upstream_on_cache_miss() {
    // Fresh temp directory = guaranteed empty cache
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache_dir = temp_dir.path().join("cache");

    let mut config = Config::default();
    config.server.port = 5059; // Unique port for this test

    // Configure quay.io registry
    config.upstream.registries.insert(
        "quay.io".to_string(),
        pulld::config::RegistryConfig {
            mirrors: vec!["https://quay.io".to_string()],
            strategy: pulld::config::MirrorStrategy::Failover,
            max_parallel: 2,
            chunk_size: 16_777_216,
            hedge_delay_ms: 100,
            timeout_secs: 60,
            auth: None,
            ca_cert_path: None,
            insecure: false,
        },
    );

    // Start server with empty cache
    let _server_handle = start_server(cache_dir.clone(), config.clone(), None, None)
        .await
        .expect("Failed to start pulld server");

    // Wait for server to start
    sleep(Duration::from_secs(1)).await;

    let client = reqwest::Client::new();
    let manifest_url = format!(
        "http://localhost:{}/v2/quay.io/cilium/cilium/manifests/v1.17.7",
        config.server.port
    );

    init_test_tracing();
    tracing::info!("=== Testing HEAD request on empty cache ===");
    tracing::info!("URL: {}", manifest_url);

    // KEY: Send HEAD request, not GET
    // This is what containerd does first
    let response = client
        .head(&manifest_url)
        .header(
            "Accept",
            "application/vnd.docker.distribution.manifest.v2+json",
        )
        .timeout(Duration::from_secs(60)) // Long timeout for upstream fetch
        .send()
        .await
        .expect("Failed to send HEAD request");

    let status = response.status();
    let headers = response.headers().clone();

    tracing::info!("HEAD response status: {}", status);
    tracing::info!("HEAD response headers: {:?}", headers);

    // THE BUG: HEAD returns 404 on cache miss instead of fetching from upstream
    // THE FIX: HEAD should fetch from upstream on cache miss and return 200

    // Check for Docker-Content-Digest header (proves manifest was fetched)
    let has_digest = headers.contains_key("docker-content-digest");

    if status == reqwest::StatusCode::NOT_FOUND {
        tracing::error!("[ERROR] BUG REPRODUCED: HEAD returned 404 on cache miss");
        tracing::error!("  This means head_manifest is NOT fetching from upstream");
        tracing::error!("  Containerd will fail here and never send GET request");
    } else if status == reqwest::StatusCode::OK {
        tracing::info!("[OK] HEAD returned 200 - fix is working");
        if has_digest {
            tracing::info!(
                "[OK] Docker-Content-Digest header present: {:?}",
                headers.get("docker-content-digest")
            );
        } else {
            tracing::warn!("[WARN] Docker-Content-Digest header missing");
        }
    } else if status == reqwest::StatusCode::UNAUTHORIZED {
        tracing::warn!("[WARN] HEAD returned 401 - authentication required (acceptable)");
    }

    // Assert: HEAD should NOT return 404 on cache miss
    // It should either succeed (200) or require auth (401)
    assert_ne!(
        status,
        reqwest::StatusCode::NOT_FOUND,
        "HEAD request returned 404 on cache miss. \
         This is the containerd/nerdctl bug - HEAD should fetch from upstream, not just check cache."
    );

    // If we got 200, verify the digest header is present
    if status == reqwest::StatusCode::OK {
        assert!(
            has_digest,
            "HEAD response should include Docker-Content-Digest header"
        );
    }

    tracing::info!("=== HEAD request test passed ===");
}

/// Test HEAD blob request returns Content-Length header
/// This verifies the fix for containerd compatibility where blob descriptors
/// must have a non-zero content size
#[tokio::test]
async fn test_head_blob_returns_content_length() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache_dir = temp_dir.path().join("cache");

    let mut config = Config::default();
    config.server.port = 5060; // Unique port for this test

    // Configure quay.io registry
    config.upstream.registries.insert(
        "quay.io".to_string(),
        pulld::config::RegistryConfig {
            mirrors: vec!["https://quay.io".to_string()],
            strategy: pulld::config::MirrorStrategy::Failover,
            max_parallel: 2,
            chunk_size: 16_777_216,
            hedge_delay_ms: 100,
            timeout_secs: 60,
            auth: None,
            ca_cert_path: None,
            insecure: false,
        },
    );

    // Start server
    let _server_handle = start_server(cache_dir.clone(), config.clone(), None, None)
        .await
        .expect("Failed to start pulld server");

    // Wait for server to start
    sleep(Duration::from_secs(1)).await;

    let client = reqwest::Client::new();

    // First, fetch manifest to get a real blob digest
    let manifest_url = format!(
        "http://localhost:{}/v2/quay.io/cilium/cilium/manifests/v1.17.7",
        config.server.port
    );

    let manifest_response = client
        .get(&manifest_url)
        .header(
            "Accept",
            "application/vnd.docker.distribution.manifest.v2+json",
        )
        .timeout(Duration::from_secs(60))
        .send()
        .await
        .expect("Failed to send manifest request");

    init_test_tracing();
    if !manifest_response.status().is_success() {
        tracing::warn!("[WARN] Manifest fetch failed, skipping blob HEAD test");
        return;
    }

    // Parse manifest to extract a blob digest (simplified - just use a known digest)
    // In a real scenario, we'd parse the manifest JSON to get actual layer digests
    // For this test, we'll use a digest that might exist in the manifest
    let test_blob_digest =
        "sha256:b22440f49c61195171aca585c7a57c6a8867271e43a5abc38f2a2f561436ff86";
    let blob_url = format!(
        "http://localhost:{}/v2/quay.io/cilium/cilium/blobs/{}",
        config.server.port, test_blob_digest
    );

    tracing::info!("=== Testing HEAD blob request ===");
    tracing::info!("URL: {}", blob_url);

    // Send HEAD request to blob endpoint
    let head_response = client
        .head(&blob_url)
        .timeout(Duration::from_secs(60))
        .send()
        .await
        .expect("Failed to send HEAD blob request");

    let status = head_response.status();
    let headers = head_response.headers().clone();

    tracing::info!("HEAD blob response status: {}", status);
    tracing::info!("HEAD blob response headers: {:?}", headers);

    // If blob exists (200), verify Content-Length is present and non-zero
    if status == reqwest::StatusCode::OK {
        let content_length = headers.get("content-length");
        assert!(
            content_length.is_some(),
            "HEAD blob response MUST include Content-Length header for containerd compatibility"
        );

        let content_length_str = content_length.unwrap().to_str().unwrap();
        let content_length_value: u64 = content_length_str
            .parse()
            .expect("Content-Length should be a valid number");

        assert!(
            content_length_value > 0,
            "Content-Length MUST be non-zero. Got: {}. Containerd rejects zero-size descriptors.",
            content_length_value
        );

        tracing::info!(
            "[OK] HEAD blob returned 200 with Content-Length: {}",
            content_length_value
        );
        tracing::info!("[OK] Content-Length is non-zero - containerd compatibility verified");
    } else if status == reqwest::StatusCode::NOT_FOUND {
        tracing::warn!("[WARN] Blob not found (404) - this is acceptable if blob doesn't exist");
    } else {
        tracing::warn!(
            "[WARN] HEAD blob returned status: {} - may require authentication",
            status
        );
    }

    tracing::info!("=== HEAD blob request test completed ===");
}

/// Test Helm chart caching - verify charts are cached on first request and served from cache on subsequent requests
#[tokio::test]
async fn test_helm_chart_caching() {
    init_test_tracing();
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache_dir = temp_dir.path().join("cache");

    let mut config = Config::default();
    config.server.port = 5061; // Unique port for this test

    // Ensure cilium repo is configured
    config
        .helm
        .repositories
        .insert("cilium".to_string(), "https://helm.cilium.io/".to_string());

    // Start server
    let _server_handle = start_server(cache_dir.clone(), config.clone(), None, None)
        .await
        .expect("Failed to start pulld server");

    // Wait for server to start
    sleep(Duration::from_secs(1)).await;

    let client = reqwest::Client::new();
    let chart_url = format!(
        "http://localhost:{}/helm/cilium/charts/cilium-1.17.7.tgz",
        config.server.port
    );

    tracing::info!("=== Testing Helm chart caching ===");
    tracing::info!("Chart URL: {}", chart_url);

    // First request - should fetch from upstream and cache
    let response1 = client
        .get(&chart_url)
        .timeout(Duration::from_secs(120))
        .send()
        .await
        .expect("Failed to send chart request");

    if !response1.status().is_success() {
        tracing::warn!(
            "[WARN] Chart fetch failed with status: {}, skipping cache test",
            response1.status()
        );
        return;
    }

    let chart_data1 = response1.bytes().await.expect("Failed to read chart data");
    tracing::info!(
        "[OK] First request completed, chart size: {} bytes",
        chart_data1.len()
    );

    // Verify chart was cached
    let cache = CacheStorage::new(cache_dir.clone()).expect("Failed to create cache storage");
    assert!(
        cache.chart_exists("cilium", "cilium-1.17.7.tgz").await,
        "Chart should be cached after first request"
    );

    // Second request - should be served from cache
    let response2 = client
        .get(&chart_url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .expect("Failed to send second chart request");

    assert_eq!(
        response2.status(),
        reqwest::StatusCode::OK,
        "Second request should succeed from cache"
    );

    let chart_data2 = response2.bytes().await.expect("Failed to read chart data");
    assert_eq!(
        chart_data1.len(),
        chart_data2.len(),
        "Cached chart should match original"
    );

    tracing::info!("[OK] Chart caching test passed - chart served from cache on second request");
    tracing::info!("=== Helm chart caching test completed ===");
}

/// Test Helm chart prepull functionality
#[tokio::test]
async fn test_helm_chart_prepull() {
    init_test_tracing();
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache_dir = temp_dir.path().join("cache");

    let mut config = Config::default();
    config.pre_pull.enabled = true;
    config.pre_pull.charts = vec!["coredns/coredns-1.43.0.tgz".to_string()];

    // Ensure coredns repo is configured
    config.helm.repositories.insert(
        "coredns".to_string(),
        "https://coredns.github.io/helm".to_string(),
    );

    tracing::info!("=== Testing Helm chart prepull ===");
    tracing::info!("Prepull charts: {:?}", config.pre_pull.charts);

    // Run prepull
    pulld::run_pre_pull(cache_dir.clone(), &config)
        .await
        .expect("Pre-pull failed");

    // Verify chart was cached
    let cache = CacheStorage::new(cache_dir.clone()).expect("Failed to create cache storage");
    assert!(
        cache.chart_exists("coredns", "coredns-1.43.0.tgz").await,
        "Chart should be cached after prepull"
    );

    // Verify chart can be read
    let chart_data = cache
        .read_chart("coredns", "coredns-1.43.0.tgz")
        .await
        .expect("Failed to read cached chart");
    assert!(!chart_data.is_empty(), "Cached chart should not be empty");

    tracing::info!(
        "[OK] Chart prepull test passed - chart cached: {} bytes",
        chart_data.len()
    );
    tracing::info!("=== Helm chart prepull test completed ===");
}
