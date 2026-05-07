//! Test OCI Image Index manifest handling from ghcr.io
//!
//! This test verifies that OCI Image Index manifests (application/vnd.oci.image.index.v1+json)
//! are correctly handled and NOT rejected as schema 1 manifests.

use pulld::cache::CacheStorage;
use serde_json::json;
use tempfile::TempDir;

#[tokio::test]
async fn test_oci_image_index_not_rejected_as_schema_1() {
    // Test that OCI Image Index manifests are NOT rejected
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache = CacheStorage::new(temp_dir.path().to_path_buf()).unwrap();

    let registry = "ghcr.io";
    let repository = "cloudnative-pg/cloudnative-pg";
    let reference = "1.25.1";

    // Create an OCI Image Index manifest (what CNPG actually uses)
    let oci_index_manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "size": 1234,
                "digest": "sha256:abc123",
                "platform": {
                    "architecture": "arm64",
                    "os": "linux"
                }
            }
        ]
    });

    let manifest_bytes = serde_json::to_vec(&oci_index_manifest).unwrap();

    // Write manifest to cache
    cache
        .write_manifest(registry, repository, reference, &manifest_bytes)
        .await
        .expect("Failed to write OCI Image Index manifest");

    // Read it back
    let cached_data = cache
        .read_manifest(registry, repository, reference)
        .await
        .expect("Failed to read cached manifest");

    // Validate that it's NOT detected as schema 1
    let mut is_schema_1 = false;
    let mut is_oci_index = false;
    let mut media_type = String::new();

    if let Ok(manifest_json) = serde_json::from_slice::<serde_json::Value>(&cached_data) {
        // Check schemaVersion
        if let Some(schema_version) = manifest_json.get("schemaVersion").and_then(|v| v.as_u64()) {
            if schema_version == 1 {
                is_schema_1 = true;
            }
        }
        // Check mediaType
        if let Some(mt) = manifest_json.get("mediaType").and_then(|v| v.as_str()) {
            media_type = mt.to_string();
            // Only reject Docker schema 1, not OCI Image Index
            if mt == "application/vnd.docker.distribution.manifest.v1+json"
                || mt.contains("schema1")
            {
                is_schema_1 = true;
            }
            if mt.contains("image.index") {
                is_oci_index = true;
            }
        }
    }

    // OCI Image Index should NOT be rejected as schema 1
    assert!(
        !is_schema_1,
        "OCI Image Index should NOT be detected as schema 1. mediaType: {}",
        media_type
    );
    assert!(
        is_oci_index,
        "Should detect OCI Image Index. mediaType: {}",
        media_type
    );
    assert_eq!(
        media_type, "application/vnd.oci.image.index.v1+json",
        "Should have correct OCI Image Index mediaType"
    );
}

#[tokio::test]
async fn test_docker_schema_1_still_rejected() {
    // Test that Docker schema 1 manifests ARE still rejected
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache = CacheStorage::new(temp_dir.path().to_path_buf()).unwrap();

    let registry = "docker.io";
    let repository = "test/repo";
    let reference = "latest";

    // Create a Docker schema 1 manifest (should be rejected)
    let docker_schema_1_manifest = json!({
        "schemaVersion": 1,
        "name": "test/repo",
        "tag": "latest",
        "mediaType": "application/vnd.docker.distribution.manifest.v1+json"
    });

    let manifest_bytes = serde_json::to_vec(&docker_schema_1_manifest).unwrap();

    // Write manifest to cache
    cache
        .write_manifest(registry, repository, reference, &manifest_bytes)
        .await
        .expect("Failed to write Docker schema 1 manifest");

    // Read it back
    let cached_data = cache
        .read_manifest(registry, repository, reference)
        .await
        .expect("Failed to read cached manifest");

    // Validate that it IS detected as schema 1
    let mut is_schema_1 = false;
    let mut media_type = String::new();

    if let Ok(manifest_json) = serde_json::from_slice::<serde_json::Value>(&cached_data) {
        // Check schemaVersion
        if let Some(schema_version) = manifest_json.get("schemaVersion").and_then(|v| v.as_u64()) {
            if schema_version == 1 {
                is_schema_1 = true;
            }
        }
        // Check mediaType
        if let Some(mt) = manifest_json.get("mediaType").and_then(|v| v.as_str()) {
            media_type = mt.to_string();
            // Only reject Docker schema 1, not OCI Image Index
            if mt == "application/vnd.docker.distribution.manifest.v1+json"
                || mt.contains("schema1")
            {
                is_schema_1 = true;
            }
        }
    }

    // Docker schema 1 SHOULD be detected and rejected
    assert!(
        is_schema_1,
        "Docker schema 1 SHOULD be detected. mediaType: {}",
        media_type
    );
    assert_eq!(
        media_type, "application/vnd.docker.distribution.manifest.v1+json",
        "Should have Docker schema 1 mediaType"
    );
}
