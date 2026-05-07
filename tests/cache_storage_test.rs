//! Unit tests for cache storage
//!
//! Tests for blob deduplication, concurrent downloads, and cache operations.

use pulld::cache::CacheStorage;
use pulld::error::DockerProxyError;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_download_with_dedupe_single_request() {
    let temp_dir = std::env::temp_dir().join("pulld-test");
    let _ = std::fs::remove_dir_all(&temp_dir);
    let cache = CacheStorage::new(temp_dir.clone()).unwrap();

    let digest = "sha256:test123";
    let download_count = Arc::new(std::sync::Mutex::new(0));

    let download_count_clone = download_count.clone();
    let result = cache
        .download_with_dedupe(digest, move || {
            let count = download_count_clone.clone();
            async move {
                *count.lock().unwrap() += 1;
                sleep(Duration::from_millis(10)).await;
                Ok(b"test data".to_vec())
            }
        })
        .await
        .unwrap();

    assert_eq!(result, b"test data");
    assert_eq!(*download_count.lock().unwrap(), 1);

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_download_with_dedupe_concurrent_requests() {
    let temp_dir = std::env::temp_dir().join("pulld-test-dedup");
    let _ = std::fs::remove_dir_all(&temp_dir);
    let cache = Arc::new(CacheStorage::new(temp_dir.clone()).unwrap());

    let digest = "sha256:test456";
    let download_count = Arc::new(std::sync::Mutex::new(0));

    // Spawn 5 concurrent requests for the same digest
    let mut handles = Vec::new();
    for _ in 0..5 {
        let cache_clone = cache.clone();
        let digest = digest.to_string();
        let count = download_count.clone();

        let handle = tokio::spawn(async move {
            cache_clone
                .download_with_dedupe(&digest, move || {
                    let count = count.clone();
                    async move {
                        *count.lock().unwrap() += 1;
                        sleep(Duration::from_millis(50)).await; // Simulate download time
                        Ok(b"concurrent data".to_vec())
                    }
                })
                .await
        });
        handles.push(handle);
    }

    // Wait for all requests
    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap().unwrap())
        .collect();

    // All should return the same data
    for result in &results {
        assert_eq!(result, b"concurrent data");
    }

    // Download function should only be called once (deduplication)
    assert_eq!(*download_count.lock().unwrap(), 1);

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_download_with_dedupe_different_digests() {
    let temp_dir = std::env::temp_dir().join("pulld-test-diff");
    let _ = std::fs::remove_dir_all(&temp_dir);
    let cache = Arc::new(CacheStorage::new(temp_dir.clone()).unwrap());

    let download_count = Arc::new(std::sync::Mutex::new(0));

    // Spawn requests for different digests - each should trigger a download
    let mut handles = Vec::new();
    for i in 0..3 {
        let cache_clone = cache.clone();
        let digest = format!("sha256:test{}", i);
        let count = download_count.clone();

        let handle = tokio::spawn(async move {
            cache_clone
                .download_with_dedupe(&digest, move || {
                    let count = count.clone();
                    async move {
                        *count.lock().unwrap() += 1;
                        sleep(Duration::from_millis(10)).await;
                        Ok(format!("data{}", i).into_bytes())
                    }
                })
                .await
        });
        handles.push(handle);
    }

    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap().unwrap())
        .collect();

    // Each should have different data
    assert_eq!(results[0], b"data0");
    assert_eq!(results[1], b"data1");
    assert_eq!(results[2], b"data2");

    // Each digest should trigger its own download
    assert_eq!(*download_count.lock().unwrap(), 3);

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_download_with_dedupe_error_propagation() {
    let temp_dir = std::env::temp_dir().join("pulld-test-err");
    let _ = std::fs::remove_dir_all(&temp_dir);
    let cache = Arc::new(CacheStorage::new(temp_dir.clone()).unwrap());

    let digest = "sha256:error";
    let download_count = Arc::new(std::sync::Mutex::new(0));

    // Spawn multiple concurrent requests that will fail
    let mut handles = Vec::new();
    for _ in 0..3 {
        let cache_clone = cache.clone();
        let digest = digest.to_string();
        let count = download_count.clone();

        let handle = tokio::spawn(async move {
            cache_clone
                .download_with_dedupe(&digest, move || {
                    let count = count.clone();
                    async move {
                        *count.lock().unwrap() += 1;
                        sleep(Duration::from_millis(10)).await;
                        Err(DockerProxyError::Cache("Test error".to_string()))
                    }
                })
                .await
        });
        handles.push(handle);
    }

    // All should get the same error
    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    for result in &results {
        assert!(result.is_err());
        if let Err(DockerProxyError::Cache(msg)) = result {
            assert_eq!(msg, "Test error");
        } else {
            panic!("Unexpected error type");
        }
    }

    // Error should only be generated once (deduplication)
    assert_eq!(*download_count.lock().unwrap(), 1);

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_blob_path() {
    let temp_dir = std::env::temp_dir().join("pulld-test-path");
    let cache = CacheStorage::new(temp_dir.clone()).unwrap();

    // Test with sha256: prefix
    let path1 = cache.blob_path("sha256:abc123");
    assert!(path1.to_string_lossy().ends_with("abc123"));

    // Test without prefix
    let path2 = cache.blob_path("def456");
    assert!(path2.to_string_lossy().ends_with("def456"));

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_blob_exists() {
    let temp_dir = std::env::temp_dir().join("pulld-test-exists");
    let _ = std::fs::remove_dir_all(&temp_dir);
    let cache = CacheStorage::new(temp_dir.clone()).unwrap();

    let digest = "sha256:test789";
    assert!(!cache.blob_exists(digest).await);

    // Write a blob
    cache.write_blob(digest, b"test data").await.unwrap();
    assert!(cache.blob_exists(digest).await);

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_chart_path() {
    let temp_dir = std::env::temp_dir().join("pulld-test-chart-path");
    let _ = std::fs::remove_dir_all(&temp_dir);
    let cache = CacheStorage::new(temp_dir.clone()).unwrap();

    let path = cache.chart_path("cilium", "cilium-1.17.7.tgz");
    assert!(path.to_string_lossy().contains("charts"));
    assert!(path.to_string_lossy().contains("cilium"));
    assert!(path.to_string_lossy().contains("cilium-1.17.7.tgz"));

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_chart_exists() {
    let temp_dir = std::env::temp_dir().join("pulld-test-chart-exists");
    let _ = std::fs::remove_dir_all(&temp_dir);
    let cache = CacheStorage::new(temp_dir.clone()).unwrap();

    let repo = "coredns";
    let chart = "coredns-1.43.0.tgz";
    assert!(!cache.chart_exists(repo, chart).await);

    // Write a chart
    cache
        .write_chart(repo, chart, b"test chart data")
        .await
        .unwrap();
    assert!(cache.chart_exists(repo, chart).await);

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_write_and_read_chart() {
    let temp_dir = std::env::temp_dir().join("pulld-test-chart-rw");
    let _ = std::fs::remove_dir_all(&temp_dir);
    let cache = CacheStorage::new(temp_dir.clone()).unwrap();

    let repo = "jetstack";
    let chart = "cert-manager-v1.15.3.tgz";
    let chart_data = b"test helm chart tarball data";

    // Write chart
    cache.write_chart(repo, chart, chart_data).await.unwrap();

    // Read chart back
    let read_data = cache.read_chart(repo, chart).await.unwrap();
    assert_eq!(read_data, chart_data);

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_write_chart_creates_directories() {
    let temp_dir = std::env::temp_dir().join("pulld-test-chart-dirs");
    let _ = std::fs::remove_dir_all(&temp_dir);
    let cache = CacheStorage::new(temp_dir.clone()).unwrap();

    let repo = "topolvm";
    let chart = "topolvm-15.6.1.tgz";
    let chart_data = b"test chart";

    // Write chart - should create repo directory if it doesn't exist
    cache.write_chart(repo, chart, chart_data).await.unwrap();

    // Verify directory was created
    let chart_path = cache.chart_path(repo, chart);
    let repo_dir = chart_path.parent().unwrap();
    assert!(repo_dir.exists());
    assert!(chart_path.exists());

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_read_chart_not_found() {
    let temp_dir = std::env::temp_dir().join("pulld-test-chart-not-found");
    let _ = std::fs::remove_dir_all(&temp_dir);
    let cache = CacheStorage::new(temp_dir.clone()).unwrap();

    let repo = "nonexistent";
    let chart = "chart-1.0.0.tgz";

    // Try to read non-existent chart
    let result = cache.read_chart(repo, chart).await;
    assert!(result.is_err());

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}
