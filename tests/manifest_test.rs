//! Unit tests for manifest handling
//!
//! Tests for repository parsing, path parsing, and manifest caching behavior.

use pulld::cache::CacheStorage;
use pulld::config::{UpstreamTlsConfig, DEFAULT_REGISTRY_URL};
use pulld::registry::manifest::{parse_repository, AppState, TokenCache};
use pulld::registry::upstream::UpstreamClient;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock as TokioRwLock;

#[test]
fn test_parse_repository_with_embedded_registry() {
    // Test parsing repository names with embedded registry (like quay.io/cilium/cilium)
    let test_cases = vec![
        ("quay.io/cilium/cilium", ("quay.io", "cilium/cilium")),
        ("ghcr.io/topolvm/topolvm", ("ghcr.io", "topolvm/topolvm")),
        ("docker.io/library/nginx", ("docker.io", "library/nginx")),
        ("registry.k8s.io/pause", ("registry.k8s.io", "pause")),
        // Docker Hub official images (library prefix is special)
        ("library/nginx", ("docker.io", "library/nginx")),
        // Simple name defaults to docker.io
        ("nginx", ("docker.io", "library/nginx")),
    ];

    for (input, expected) in test_cases {
        let (registry, repository) = parse_repository(input);
        assert_eq!(
            (registry.as_str(), repository.as_str()),
            expected,
            "Failed to parse: {}",
            input
        );
    }
}

#[tokio::test]
async fn test_get_upstream_client_auto_detection() {
    // Create a minimal AppState for testing
    let cache = Arc::new(
        CacheStorage::with_max_size(std::env::temp_dir().join("pulld-test"), Some(1)).unwrap(),
    );

    let upstream_tls = Arc::new(UpstreamTlsConfig {
        ca_bundle_path: None,
        use_system_ca: true,
        insecure_skip_verify: false,
    });

    let default_client = Arc::new(
        UpstreamClient::new(
            vec!["https://registry-1.docker.io".to_string()],
            &upstream_tls,
            None,
        )
        .unwrap(),
    );

    let app_state = AppState {
        cache,
        registry_clients: Arc::new(std::sync::RwLock::new(HashMap::new())),
        registry_configs: Arc::new(HashMap::new()),
        default_registry_client: default_client,
        token_cache: Arc::new(TokioRwLock::new(TokenCache::default())),
        helm_repos: Arc::new(HashMap::new()),
        pre_pull_config: None,
        upstream_tls: upstream_tls.clone(),
        mirror_selector: Arc::new(pulld::registry::mirror_racer::MirrorSelector::new(
            pulld::config::MirrorStrategy::Adaptive,
        )),
        proxy_host: "pulld.internal".to_string(),
        proxy_port: 5050,
        proxy_scheme: "http".to_string(),
    };

    // Test auto-detection for unconfigured registry
    let quay_client = app_state.get_upstream_client("quay.io");
    let mirrors = quay_client.mirrors();
    assert_eq!(mirrors.len(), 1);
    assert_eq!(mirrors[0], "https://quay.io");

    // Test that it's cached
    let quay_client2 = app_state.get_upstream_client("quay.io");
    assert_eq!(quay_client.mirrors(), quay_client2.mirrors());

    // Test another registry
    let ghcr_client = app_state.get_upstream_client("ghcr.io");
    let mirrors = ghcr_client.mirrors();
    assert_eq!(mirrors.len(), 1);
    assert_eq!(mirrors[0], "https://ghcr.io");
}

#[test]
fn test_get_registry_config_auto_detection() {
    // Create a minimal AppState for testing
    let cache = Arc::new(
        CacheStorage::with_max_size(std::env::temp_dir().join("pulld-test"), Some(1)).unwrap(),
    );

    let upstream_tls = Arc::new(UpstreamTlsConfig {
        ca_bundle_path: None,
        use_system_ca: true,
        insecure_skip_verify: false,
    });

    let default_client = Arc::new(
        UpstreamClient::new(
            vec!["https://registry-1.docker.io".to_string()],
            &upstream_tls,
            None,
        )
        .unwrap(),
    );

    let app_state = AppState {
        cache,
        registry_clients: Arc::new(std::sync::RwLock::new(HashMap::new())),
        registry_configs: Arc::new(HashMap::new()),
        default_registry_client: default_client,
        token_cache: Arc::new(TokioRwLock::new(TokenCache::default())),
        helm_repos: Arc::new(HashMap::new()),
        pre_pull_config: None,
        upstream_tls: upstream_tls.clone(),
        mirror_selector: Arc::new(pulld::registry::mirror_racer::MirrorSelector::new(
            pulld::config::MirrorStrategy::Adaptive,
        )),
        proxy_host: "pulld.internal".to_string(),
        proxy_port: 5050,
        proxy_scheme: "http".to_string(),
    };

    // Test auto-detection for unconfigured registry
    let config = app_state.get_registry_config("quay.io");
    assert_eq!(config.mirrors.len(), 1);
    assert_eq!(config.mirrors[0], "https://quay.io");

    // Test another registry
    let config = app_state.get_registry_config("ghcr.io");
    assert_eq!(config.mirrors.len(), 1);
    assert_eq!(config.mirrors[0], "https://ghcr.io");

    // Test default registry (docker.io) - should use default mirrors
    let config = app_state.get_registry_config("docker.io");
    assert_eq!(config.mirrors.len(), 1);
    assert_eq!(config.mirrors[0], DEFAULT_REGISTRY_URL);

    // Test non-domain registry name - should use default
    let config = app_state.get_registry_config("myregistry");
    assert_eq!(config.mirrors.len(), 1);
    assert_eq!(config.mirrors[0], DEFAULT_REGISTRY_URL);
}

#[test]
fn test_path_parsing_for_embedded_registry() {
    // Simulate the path parsing that happens in get_v2_wrapper
    // Path: /v2/quay.io/cilium/cilium/manifests/v1.17.7
    const V2_PREFIX: &str = "/v2/";
    const MANIFESTS_SUFFIX: &str = "/manifests/";

    let path = "/v2/quay.io/cilium/cilium/manifests/v1.17.7";
    if let Some(manifests_idx) = path.rfind(MANIFESTS_SUFFIX) {
        let name = path[V2_PREFIX.len()..manifests_idx].to_string();
        let reference = path[manifests_idx + MANIFESTS_SUFFIX.len()..].to_string();

        assert_eq!(name, "quay.io/cilium/cilium");
        assert_eq!(reference, "v1.17.7");

        // Parse the repository
        let (registry, repository) = parse_repository(&name);
        assert_eq!(registry, "quay.io");
        assert_eq!(repository, "cilium/cilium");
    } else {
        panic!("Failed to parse path");
    }

    // Test blob path: /v2/quay.io/cilium/cilium/blobs/sha256:...
    const BLOBS_SUFFIX: &str = "/blobs/";
    let blob_path = "/v2/quay.io/cilium/cilium/blobs/sha256:abc123";
    if let Some(blobs_idx) = blob_path.rfind(BLOBS_SUFFIX) {
        let name = blob_path[V2_PREFIX.len()..blobs_idx].to_string();
        let digest = blob_path[blobs_idx + BLOBS_SUFFIX.len()..].to_string();

        assert_eq!(name, "quay.io/cilium/cilium");
        assert_eq!(digest, "sha256:abc123");

        // Parse the repository
        let (registry, repository) = parse_repository(&name);
        assert_eq!(registry, "quay.io");
        assert_eq!(repository, "cilium/cilium");
    } else {
        panic!("Failed to parse blob path");
    }
}

#[test]
fn test_helm_chart_image_url_format() {
    // Test the exact format used in Helm charts:
    // repository: pulld.internal:5050/quay.io/cilium/operator
    // tag: "v1.17.7"
    // Full image: pulld.internal:5050/quay.io/cilium/operator:v1.17.7
    //
    // When containerd pulls this, it makes a request to:
    // GET /v2/quay.io/cilium/operator/manifests/v1.17.7

    const V2_PREFIX: &str = "/v2/";
    const MANIFESTS_SUFFIX: &str = "/manifests/";

    // Test cases matching actual Helm chart configurations
    let test_cases = vec![
        // Cilium operator image
        (
            "/v2/quay.io/cilium/operator/manifests/v1.17.7",
            "quay.io/cilium/operator",
            "v1.17.7",
            "quay.io",
            "cilium/operator",
        ),
        // Cilium main image
        (
            "/v2/quay.io/cilium/cilium/manifests/v1.17.7",
            "quay.io/cilium/cilium",
            "v1.17.7",
            "quay.io",
            "cilium/cilium",
        ),
        // CoreDNS image
        (
            "/v2/docker.io/coredns/coredns/manifests/1.12.1",
            "docker.io/coredns/coredns",
            "1.12.1",
            "docker.io",
            "coredns/coredns",
        ),
        // TopoLVM image
        (
            "/v2/ghcr.io/topolvm/topolvm-with-sidecar/manifests/v0.25.0",
            "ghcr.io/topolvm/topolvm-with-sidecar",
            "v0.25.0",
            "ghcr.io",
            "topolvm/topolvm-with-sidecar",
        ),
        // Cert-manager image
        (
            "/v2/quay.io/jetstack/cert-manager-controller/manifests/v1.15.3",
            "quay.io/jetstack/cert-manager-controller",
            "v1.15.3",
            "quay.io",
            "jetstack/cert-manager-controller",
        ),
    ];

    for (path, expected_name, expected_reference, expected_registry, expected_repository) in
        test_cases
    {
        // Parse path (simulating get_v2_wrapper)
        if let Some(manifests_idx) = path.rfind(MANIFESTS_SUFFIX) {
            let name = path[V2_PREFIX.len()..manifests_idx].to_string();
            let reference = path[manifests_idx + MANIFESTS_SUFFIX.len()..].to_string();

            assert_eq!(
                name, expected_name,
                "Failed to parse name from path: {}",
                path
            );
            assert_eq!(
                reference, expected_reference,
                "Failed to parse reference from path: {}",
                path
            );

            // Parse repository (simulating get_manifest)
            let (registry, repository) = parse_repository(&name);
            assert_eq!(
                registry, expected_registry,
                "Failed to parse registry from name: {}",
                name
            );
            assert_eq!(
                repository, expected_repository,
                "Failed to parse repository from name: {}",
                name
            );
        } else {
            panic!("Failed to parse path: {}", path);
        }
    }
}

#[test]
fn test_path_parsing_simulates_containerd_request() {
    // Simulate exactly what happens when containerd requests:
    // Image: pulld.internal:5050/quay.io/cilium/operator:v1.17.7
    // Request: GET /v2/quay.io/cilium/operator/manifests/v1.17.7

    const V2_PREFIX: &str = "/v2/";
    const MANIFESTS_SUFFIX: &str = "/manifests/";

    let path = "/v2/quay.io/cilium/operator/manifests/v1.17.7";

    // Simulate get_v2_wrapper parsing
    if let Some(manifests_idx) = path.rfind(MANIFESTS_SUFFIX) {
        let name = path[V2_PREFIX.len()..manifests_idx].to_string();
        let reference = path[manifests_idx + MANIFESTS_SUFFIX.len()..].to_string();

        assert_eq!(name, "quay.io/cilium/operator");
        assert_eq!(reference, "v1.17.7");

        // Simulate get_manifest parsing
        let (registry, repository) = parse_repository(&name);
        assert_eq!(registry, "quay.io");
        assert_eq!(repository, "cilium/operator");

        // Simulate upstream path construction
        let upstream_path = format!("/v2/{}/manifests/{}", repository, reference);
        assert_eq!(upstream_path, "/v2/cilium/operator/manifests/v1.17.7");

        // Verify the full URL that would be requested
        let mirror = "https://quay.io";
        let full_url = format!("{}{}", mirror, upstream_path);
        assert_eq!(
            full_url,
            "https://quay.io/v2/cilium/operator/manifests/v1.17.7"
        );
    } else {
        panic!("Failed to parse path");
    }
}

#[tokio::test]
async fn test_manifest_cached_by_tag_and_digest() {
    // Test that when a manifest is cached by tag, it's also cached by digest
    // This is critical for containerd compatibility, as containerd requests
    // manifests by digest after initially fetching by tag

    use sha2::{Digest, Sha256};
    use tokio::fs;

    // Create a temporary cache directory
    let temp_dir = std::env::temp_dir().join(format!("pulld-test-{}", std::process::id()));
    let _ = fs::remove_dir_all(&temp_dir).await; // Clean up if exists
    let cache = Arc::new(CacheStorage::with_max_size(temp_dir.clone(), Some(1)).unwrap());

    // Sample manifest data (simplified Docker manifest v2)
    let manifest_data = r#"{
        "schemaVersion": 2,
        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
        "config": {
            "mediaType": "application/vnd.docker.container.image.v1+json",
            "size": 1234,
            "digest": "sha256:config123"
        },
        "layers": [
            {
                "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                "size": 5678,
                "digest": "sha256:layer123"
            }
        ]
    }"#
    .as_bytes()
    .to_vec();

    // Calculate digest
    let digest = format!("sha256:{:x}", Sha256::digest(&manifest_data));

    // Test parameters
    let registry = "quay.io";
    let repository = "cilium/cilium";
    let tag = "v1.17.7";

    // Step 1: Cache manifest by tag (simulating what happens when fetched by tag)
    cache
        .write_manifest(registry, repository, tag, &manifest_data)
        .await
        .expect("Failed to cache manifest by tag");

    // Step 2: Verify manifest is cached by tag
    assert!(
        cache.manifest_exists(registry, repository, tag).await,
        "Manifest should be cached by tag"
    );

    let cached_by_tag = cache
        .read_manifest(registry, repository, tag)
        .await
        .expect("Failed to read manifest by tag");
    assert_eq!(
        cached_by_tag, manifest_data,
        "Cached manifest by tag should match original"
    );

    // Step 3: Also cache by digest (simulating the new behavior)
    cache
        .write_manifest(registry, repository, &digest, &manifest_data)
        .await
        .expect("Failed to cache manifest by digest");

    // Step 4: Verify manifest is also cached by digest
    assert!(
        cache.manifest_exists(registry, repository, &digest).await,
        "Manifest should also be cached by digest"
    );

    let cached_by_digest = cache
        .read_manifest(registry, repository, &digest)
        .await
        .expect("Failed to read manifest by digest");
    assert_eq!(
        cached_by_digest, manifest_data,
        "Cached manifest by digest should match original"
    );

    // Step 5: Verify both cached entries contain the same data
    assert_eq!(
        cached_by_tag, cached_by_digest,
        "Manifest cached by tag and digest should be identical"
    );

    // Cleanup
    let _ = fs::remove_dir_all(&temp_dir).await;
}

#[test]
fn test_malformed_path_with_double_v2_prefix() {
    // Test case for the actual bug: containerd is adding /v2/ to image reference
    // Image reference: pulld.internal:5050/v2/quay.io/cilium/cilium:v1.17.7
    // This results in HTTP request: GET /v2/v2/quay.io/cilium/cilium/manifests/v1.17.7
    // pulld should handle this gracefully or provide a clear error

    const V2_PREFIX: &str = "/v2/";
    const MANIFESTS_SUFFIX: &str = "/manifests/";

    // Simulate the malformed path that containerd might send
    let malformed_path = "/v2/v2/quay.io/cilium/cilium/manifests/v1.17.7";

    // Current behavior: pulld will parse this incorrectly
    if let Some(manifests_idx) = malformed_path.rfind(MANIFESTS_SUFFIX) {
        let name = malformed_path[V2_PREFIX.len()..manifests_idx].to_string();
        let reference = malformed_path[manifests_idx + MANIFESTS_SUFFIX.len()..].to_string();

        // This will incorrectly parse as:
        assert_eq!(name, "v2/quay.io/cilium/cilium"); // Wrong - has /v2/ prefix
        assert_eq!(reference, "v1.17.7");

        // When parsed, this will fail because "v2/quay.io/cilium/cilium" is not a valid repository name
        let (registry, repository) = parse_repository(&name);
        assert_eq!(registry, "v2"); // Wrong - should be "quay.io"
        assert_eq!(repository, "quay.io/cilium/cilium"); // Wrong - should be "cilium/cilium"
    } else {
        panic!("Failed to parse malformed path");
    }

    // Correct path should be:
    let correct_path = "/v2/quay.io/cilium/cilium/manifests/v1.17.7";
    if let Some(manifests_idx) = correct_path.rfind(MANIFESTS_SUFFIX) {
        let name = correct_path[V2_PREFIX.len()..manifests_idx].to_string();
        let reference = correct_path[manifests_idx + MANIFESTS_SUFFIX.len()..].to_string();

        assert_eq!(name, "quay.io/cilium/cilium"); // Correct
        assert_eq!(reference, "v1.17.7");

        let (registry, repository) = parse_repository(&name);
        assert_eq!(registry, "quay.io"); // Correct
        assert_eq!(repository, "cilium/cilium"); // Correct
    } else {
        panic!("Failed to parse correct path");
    }
}

#[tokio::test]
async fn test_schema_1_manifest_cache_validation() {
    // Test that cached schema 1 manifests are detected and rejected
    // This simulates the fix for the issue where pulld was returning cached schema 1 manifests

    let temp_dir = std::env::temp_dir().join("pulld-schema-test");
    let _ = std::fs::remove_dir_all(&temp_dir);
    let cache = CacheStorage::new(temp_dir.clone()).unwrap();

    let registry = "quay.io";
    let repository = "cilium/cilium";
    let reference = "v1.17.7";

    // Step 1: Create a schema 1 manifest (simulating old cached data)
    let schema_1_manifest = r#"{
        "schemaVersion": 1,
        "name": "cilium/cilium",
        "tag": "v1.17.7",
        "architecture": "amd64",
        "history": [
            {
                "v1Compatibility": "{\"id\":\"test\",\"created\":\"2025-01-01T00:00:00Z\"}"
            }
        ]
    }"#;

    // Write schema 1 manifest to cache
    cache
        .write_manifest(
            registry,
            repository,
            reference,
            schema_1_manifest.as_bytes(),
        )
        .await
        .expect("Failed to write schema 1 manifest to cache");

    // Verify it exists in cache
    assert!(
        cache.manifest_exists(registry, repository, reference).await,
        "Schema 1 manifest should exist in cache"
    );

    // Step 2: Read the cached manifest and validate it's schema 1
    let cached_data = cache
        .read_manifest(registry, repository, reference)
        .await
        .expect("Failed to read cached manifest");

    // Parse and validate it's schema 1
    let manifest_json: serde_json::Value =
        serde_json::from_slice(&cached_data).expect("Failed to parse cached manifest JSON");

    let schema_version = manifest_json
        .get("schemaVersion")
        .and_then(|v| v.as_u64())
        .expect("schemaVersion should exist");

    assert_eq!(schema_version, 1, "Cached manifest should be schema 1");

    // Step 3: Simulate the validation logic from get_manifest
    // This is what happens when pulld checks cached manifests
    let mut is_schema_1 = false;
    if let Ok(manifest_json) = serde_json::from_slice::<serde_json::Value>(&cached_data) {
        // Check schemaVersion
        if let Some(schema_version) = manifest_json.get("schemaVersion").and_then(|v| v.as_u64()) {
            if schema_version == 1 {
                is_schema_1 = true;
            }
        }
        // Also check mediaType for schema 1 indicators
        if !is_schema_1 {
            if let Some(media_type) = manifest_json.get("mediaType").and_then(|v| v.as_str()) {
                if media_type.contains("v1") || media_type.contains("schema1") {
                    is_schema_1 = true;
                }
            }
        }
    }

    assert!(is_schema_1, "Validation should detect schema 1 manifest");

    // Step 4: Delete the schema 1 manifest (simulating what get_manifest does)
    cache
        .delete_manifest(registry, repository, reference)
        .await
        .expect("Failed to delete schema 1 manifest");

    // Verify it's deleted
    assert!(
        !cache.manifest_exists(registry, repository, reference).await,
        "Schema 1 manifest should be deleted from cache"
    );

    // Step 5: Write a schema 2 manifest and verify it's accepted
    let schema_2_manifest = r#"{
        "schemaVersion": 2,
        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
        "config": {
            "mediaType": "application/vnd.docker.container.image.v1+json",
            "size": 1234,
            "digest": "sha256:config123"
        },
        "layers": [
            {
                "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                "size": 5678,
                "digest": "sha256:layer123"
            }
        ]
    }"#;

    cache
        .write_manifest(
            registry,
            repository,
            reference,
            schema_2_manifest.as_bytes(),
        )
        .await
        .expect("Failed to write schema 2 manifest to cache");

    // Verify schema 2 manifest exists
    assert!(
        cache.manifest_exists(registry, repository, reference).await,
        "Schema 2 manifest should exist in cache"
    );

    // Step 6: Validate schema 2 manifest is accepted
    let cached_schema_2 = cache
        .read_manifest(registry, repository, reference)
        .await
        .expect("Failed to read cached schema 2 manifest");

    let schema_2_json: serde_json::Value =
        serde_json::from_slice(&cached_schema_2).expect("Failed to parse schema 2 manifest JSON");

    let schema_version_2 = schema_2_json
        .get("schemaVersion")
        .and_then(|v| v.as_u64())
        .expect("schemaVersion should exist");

    assert_eq!(schema_version_2, 2, "Cached manifest should be schema 2");

    // Validate schema 2 is NOT rejected
    let mut is_schema_1_v2 = false;
    if let Ok(manifest_json) = serde_json::from_slice::<serde_json::Value>(&cached_schema_2) {
        if let Some(schema_version) = manifest_json.get("schemaVersion").and_then(|v| v.as_u64()) {
            if schema_version == 1 {
                is_schema_1_v2 = true;
            }
        }
        if !is_schema_1_v2 {
            if let Some(media_type) = manifest_json.get("mediaType").and_then(|v| v.as_str()) {
                if media_type.contains("v1") || media_type.contains("schema1") {
                    is_schema_1_v2 = true;
                }
            }
        }
    }

    assert!(!is_schema_1_v2, "Schema 2 manifest should NOT be rejected");

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_schema_1_manifest_media_type_detection() {
    // Test that mediaType-based schema 1 detection works
    let temp_dir = std::env::temp_dir().join("pulld-media-type-test");
    let _ = std::fs::remove_dir_all(&temp_dir);
    let cache = CacheStorage::new(temp_dir.clone()).unwrap();

    let registry = "quay.io";
    let repository = "test/repo";
    let reference = "latest";

    // Create a manifest with schema 1 mediaType (but schemaVersion might be missing)
    let schema_1_by_media_type = r#"{
        "mediaType": "application/vnd.docker.distribution.manifest.v1+json",
        "name": "test/repo",
        "tag": "latest"
    }"#;

    cache
        .write_manifest(
            registry,
            repository,
            reference,
            schema_1_by_media_type.as_bytes(),
        )
        .await
        .expect("Failed to write manifest");

    let cached_data = cache
        .read_manifest(registry, repository, reference)
        .await
        .expect("Failed to read cached manifest");

    // Validate detection by mediaType
    let mut is_schema_1 = false;
    if let Ok(manifest_json) = serde_json::from_slice::<serde_json::Value>(&cached_data) {
        if let Some(schema_version) = manifest_json.get("schemaVersion").and_then(|v| v.as_u64()) {
            if schema_version == 1 {
                is_schema_1 = true;
            }
        }
        if !is_schema_1 {
            if let Some(media_type) = manifest_json.get("mediaType").and_then(|v| v.as_str()) {
                // Only reject Docker schema 1, not OCI Image Index or OCI Image Manifest v1
                if media_type == "application/vnd.docker.distribution.manifest.v1+json"
                    || media_type.contains("schema1")
                {
                    is_schema_1 = true;
                }
            }
        }
    }

    assert!(
        is_schema_1,
        "Should detect Docker schema 1 by exact mediaType match"
    );

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_quay_io_cilium_image_pull_path_parsing() {
    // Test exact scenario: nerdctl pull pulld.internal:5050/quay.io/cilium/cilium:v1.17.7
    // Request: GET /v2/quay.io/cilium/cilium/manifests/v1.17.7
    // This test verifies path parsing and repository parsing work correctly

    const V2_PREFIX: &str = "/v2/";
    const MANIFESTS_SUFFIX: &str = "/manifests/";

    // Simulate the exact path from nerdctl request
    let path = "/v2/quay.io/cilium/cilium/manifests/v1.17.7";

    // Parse path (simulating get_v2_wrapper in server.rs)
    if let Some(manifests_idx) = path.rfind(MANIFESTS_SUFFIX) {
        let name = path[V2_PREFIX.len()..manifests_idx].to_string();
        let reference = path[manifests_idx + MANIFESTS_SUFFIX.len()..].to_string();

        // Verify path parsing
        assert_eq!(
            name, "quay.io/cilium/cilium",
            "Name should be 'quay.io/cilium/cilium'"
        );
        assert_eq!(reference, "v1.17.7", "Reference should be 'v1.17.7'");

        // Parse repository (simulating get_manifest in manifest.rs)
        let (registry, repository) = parse_repository(&name);

        // Verify repository parsing
        assert_eq!(registry, "quay.io", "Registry should be 'quay.io'");
        assert_eq!(
            repository, "cilium/cilium",
            "Repository should be 'cilium/cilium'"
        );

        // Verify upstream path construction (simulating manifest.rs line 671)
        let upstream_path = format!("/v2/{}/manifests/{}", repository, reference);
        assert_eq!(
            upstream_path, "/v2/cilium/cilium/manifests/v1.17.7",
            "Upstream path should be '/v2/cilium/cilium/manifests/v1.17.7'"
        );

        // Verify full upstream URL would be: https://quay.io/v2/cilium/cilium/manifests/v1.17.7
        let mirror_url = "https://quay.io";
        let full_upstream_url = format!("{}{}", mirror_url, upstream_path);
        assert_eq!(
            full_upstream_url, "https://quay.io/v2/cilium/cilium/manifests/v1.17.7",
            "Full upstream URL should be 'https://quay.io/v2/cilium/cilium/manifests/v1.17.7'"
        );
    } else {
        panic!("Failed to parse path: {}", path);
    }
}

#[tokio::test]
async fn test_quay_io_cilium_upstream_client_auto_detection() {
    // Test that quay.io registry is auto-detected correctly
    let cache = Arc::new(
        CacheStorage::with_max_size(std::env::temp_dir().join("pulld-quay-test"), Some(1)).unwrap(),
    );

    let upstream_tls = Arc::new(UpstreamTlsConfig {
        ca_bundle_path: None,
        use_system_ca: true,
        insecure_skip_verify: false,
    });

    let default_client = Arc::new(
        UpstreamClient::new(vec![DEFAULT_REGISTRY_URL.to_string()], &upstream_tls, None).unwrap(),
    );

    let app_state = AppState {
        cache,
        registry_clients: Arc::new(std::sync::RwLock::new(HashMap::new())),
        registry_configs: Arc::new(HashMap::new()),
        default_registry_client: default_client,
        token_cache: Arc::new(TokioRwLock::new(TokenCache::default())),
        helm_repos: Arc::new(HashMap::new()),
        pre_pull_config: None,
        upstream_tls: upstream_tls.clone(),
        mirror_selector: Arc::new(pulld::registry::mirror_racer::MirrorSelector::new(
            pulld::config::MirrorStrategy::Adaptive,
        )),
        proxy_host: "pulld.internal".to_string(),
        proxy_port: 5050,
        proxy_scheme: "http".to_string(),
    };

    // Test auto-detection for quay.io
    let upstream_client = app_state.get_upstream_client("quay.io");
    let mirrors = upstream_client.mirrors();

    // Verify auto-detected client has correct mirror URL
    assert!(
        !mirrors.is_empty(),
        "Auto-detected client should have at least one mirror"
    );
    assert_eq!(
        mirrors[0], "https://quay.io",
        "Auto-detected mirror URL should be 'https://quay.io' (without /v2/ prefix)"
    );
}
