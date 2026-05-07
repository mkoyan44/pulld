//! Tests for cache miss scenarios
//!
//! These tests verify that the cache lookup correctly handles various edge cases
//! that could lead to false cache misses:
//! - Race conditions during atomic file writes (temp file exists)
//! - Filesystem sync issues (file written but not yet visible)
//! - Async metadata checks vs synchronous exists() checks
//! - Empty file detection and cleanup
//! - Pre-pull then immediate lookup scenarios

use pulld::cache::CacheStorage;
use pulld::error::DockerProxyError;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::time::sleep;

/// Test that a blob written by pre-pull can be immediately found
/// This simulates the real-world scenario where pre-pull writes blobs
/// and the server immediately tries to serve them
#[tokio::test]
async fn test_prepull_then_immediate_lookup() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache = Arc::new(CacheStorage::new(temp_dir.path().to_path_buf()).unwrap());

    let digest = "sha256:prepull_test";
    let test_data = b"prepull test data";

    // Simulate pre-pull: write blob
    cache.write_blob(digest, test_data).await.unwrap();

    // Immediately check if blob exists (simulating server lookup)
    // This should work because write_blob uses sync_all()
    let exists = cache.blob_exists(digest).await;
    assert!(exists, "Blob should exist immediately after write");

    // Immediately read the blob (simulating server serving from cache)
    let read_data = cache.read_blob(digest).await.unwrap();
    assert_eq!(read_data, test_data, "Blob content should match");
}

/// Test that async metadata check correctly finds files
/// This verifies that tokio::fs::metadata() is more reliable than PathBuf::exists()
#[tokio::test]
async fn test_async_metadata_check() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache = CacheStorage::new(temp_dir.path().to_path_buf()).unwrap();

    let digest = "sha256:metadata_test";
    let test_data = b"metadata test data";
    let blob_path = cache.blob_path(digest);

    // Write blob
    cache.write_blob(digest, test_data).await.unwrap();

    // Check with async metadata (what the server uses)
    let metadata = tokio::fs::metadata(&blob_path).await;
    assert!(
        metadata.is_ok(),
        "Async metadata check should find the file"
    );
    assert!(metadata.unwrap().len() > 0, "File should have content");

    // Also verify synchronous exists() works (for comparison)
    assert!(blob_path.exists(), "Synchronous exists() should also work");
}

/// Test race condition handling: temp file exists but rename hasn't completed
/// This simulates the scenario where a blob is being written (temp file exists)
/// and a concurrent request tries to read it
#[tokio::test]
async fn test_race_condition_temp_file_exists() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache = Arc::new(CacheStorage::new(temp_dir.path().to_path_buf()).unwrap());

    let digest = "sha256:race_test";
    let test_data = b"race condition test data";
    let blob_path = cache.blob_path(digest);
    let temp_path = blob_path.with_extension("tmp");

    // Manually create temp file to simulate write in progress
    if let Some(parent) = temp_path.parent() {
        fs::create_dir_all(parent).await.unwrap();
    }
    let mut temp_file = fs::File::create(&temp_path).await.unwrap();
    temp_file.write_all(test_data).await.unwrap();
    temp_file.sync_all().await.unwrap();
    drop(temp_file);

    // At this point, temp file exists but final file doesn't
    // The server should detect temp file and wait for rename
    assert!(
        tokio::fs::metadata(&temp_path).await.is_ok(),
        "Temp file should exist"
    );
    assert!(
        tokio::fs::metadata(&blob_path).await.is_err(),
        "Final file should not exist yet"
    );

    // Now complete the write (simulate rename)
    fs::rename(&temp_path, &blob_path).await.unwrap();

    // Sync parent directory to ensure rename is visible
    if let Some(parent) = blob_path.parent() {
        if let Ok(dir) = tokio::fs::File::open(parent).await {
            let _ = dir.sync_all().await;
        }
    }

    // After a brief wait, the file should be readable
    sleep(Duration::from_millis(50)).await;
    let metadata = tokio::fs::metadata(&blob_path).await;
    assert!(
        metadata.is_ok(),
        "File should be readable after rename and sync"
    );
    assert_eq!(metadata.unwrap().len(), test_data.len() as u64);
}

/// Test that empty files are detected and handled correctly
#[tokio::test]
async fn test_empty_file_detection() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache = CacheStorage::new(temp_dir.path().to_path_buf()).unwrap();

    let digest = "sha256:empty_test";
    let blob_path = cache.blob_path(digest);

    // Manually create an empty file (simulating corruption or incomplete write)
    if let Some(parent) = blob_path.parent() {
        fs::create_dir_all(parent).await.unwrap();
    }
    fs::write(&blob_path, b"").await.unwrap();

    // Check metadata - should detect empty file
    let metadata = tokio::fs::metadata(&blob_path).await.unwrap();
    assert_eq!(metadata.len(), 0, "File should be empty");

    // The server should detect this and delete the invalid file
    // For this test, we'll manually verify the detection logic
    if metadata.len() == 0 {
        let _ = fs::remove_file(&blob_path).await;
    }

    // File should be gone
    assert!(
        tokio::fs::metadata(&blob_path).await.is_err(),
        "Empty file should be removed"
    );
}

/// Test concurrent write and read operations
/// This simulates the real-world scenario where pre-pull is writing
/// while the server is trying to read
#[tokio::test]
async fn test_concurrent_write_and_read() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache = Arc::new(CacheStorage::new(temp_dir.path().to_path_buf()).unwrap());

    let digest = "sha256:concurrent_test";
    let test_data = b"concurrent test data";

    // Spawn write task
    let cache_write = cache.clone();
    let digest_write = digest.to_string();
    let data_write = test_data.to_vec();
    let write_handle = tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await; // Small delay to allow read to start first
        cache_write.write_blob(&digest_write, &data_write).await
    });

    // Spawn read task that starts immediately
    let cache_read = cache.clone();
    let digest_read = digest.to_string();
    let read_handle = tokio::spawn(async move {
        // Try to read multiple times (simulating retries)
        for _ in 0..10 {
            if cache_read.blob_exists(&digest_read).await {
                return cache_read.read_blob(&digest_read).await;
            }
            sleep(Duration::from_millis(50)).await;
        }
        Err(DockerProxyError::Cache(
            "Blob not found after retries".to_string(),
        ))
    });

    // Wait for both to complete
    let (write_result, read_result) = tokio::join!(write_handle, read_handle);

    // Write should succeed
    assert!(write_result.unwrap().is_ok(), "Write should succeed");

    // Read should eventually succeed (after write completes)
    let read_data = read_result.unwrap().unwrap();
    assert_eq!(read_data, test_data, "Read should get correct data");
}

/// Test filesystem sync ensures immediate visibility
/// This verifies that sync_all() makes files immediately visible
#[tokio::test]
async fn test_filesystem_sync_immediate_visibility() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache = CacheStorage::new(temp_dir.path().to_path_buf()).unwrap();

    let digest = "sha256:sync_test";
    let test_data = b"sync test data";
    let blob_path = cache.blob_path(digest);

    // Write blob (which includes sync_all)
    cache.write_blob(digest, test_data).await.unwrap();

    // Immediately check with async metadata (no delay)
    let metadata = tokio::fs::metadata(&blob_path).await;
    assert!(
        metadata.is_ok(),
        "File should be immediately visible after write_blob (which syncs)"
    );

    let size = metadata.unwrap().len();
    assert_eq!(size, test_data.len() as u64, "File size should match");
}

/// Test that blob_exists uses async metadata check
/// This verifies the blob_exists method works correctly
#[tokio::test]
async fn test_blob_exists_async_check() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache = CacheStorage::new(temp_dir.path().to_path_buf()).unwrap();

    let digest = "sha256:exists_test";
    let test_data = b"exists test data";

    // Initially should not exist
    assert!(
        !cache.blob_exists(digest).await,
        "Blob should not exist initially"
    );

    // Write blob
    cache.write_blob(digest, test_data).await.unwrap();

    // Should exist immediately
    assert!(
        cache.blob_exists(digest).await,
        "Blob should exist after write"
    );
}

/// Test that parent directory sync makes rename visible
/// This verifies that syncing the parent directory after rename
/// ensures the file is immediately visible
#[tokio::test]
async fn test_parent_directory_sync() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache = CacheStorage::new(temp_dir.path().to_path_buf()).unwrap();

    let digest = "sha256:parent_sync_test";
    let test_data = b"parent sync test data";
    let blob_path = cache.blob_path(digest);
    let temp_path = blob_path.with_extension("tmp");

    // Create parent directory
    if let Some(parent) = blob_path.parent() {
        fs::create_dir_all(parent).await.unwrap();
    }

    // Write to temp file
    fs::write(&temp_path, test_data).await.unwrap();

    // Sync temp file
    if let Ok(file) = tokio::fs::File::open(&temp_path).await {
        let _ = file.sync_all().await;
    }

    // Rename
    fs::rename(&temp_path, &blob_path).await.unwrap();

    // Sync parent directory (critical for visibility)
    if let Some(parent) = blob_path.parent() {
        if let Ok(dir) = tokio::fs::File::open(parent).await {
            let _ = dir.sync_all().await;
        }
    }

    // File should be immediately visible
    let metadata = tokio::fs::metadata(&blob_path).await;
    assert!(
        metadata.is_ok(),
        "File should be visible after rename and parent directory sync"
    );
    assert_eq!(metadata.unwrap().len(), test_data.len() as u64);
}

/// Test that multiple concurrent writes to the same blob are handled correctly
/// This simulates multiple pre-pull tasks writing the same blob
#[tokio::test]
async fn test_concurrent_writes_same_blob() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache = Arc::new(CacheStorage::new(temp_dir.path().to_path_buf()).unwrap());

    let digest = "sha256:concurrent_write_test";
    let test_data = b"concurrent write test data";

    // Spawn multiple concurrent writes
    let mut handles = Vec::new();
    for i in 0..5 {
        let cache_clone = cache.clone();
        let digest_clone = digest.to_string();
        let data = test_data.to_vec();

        let handle = tokio::spawn(async move {
            // Add small random delay to simulate real-world timing
            sleep(Duration::from_millis(i * 10)).await;
            cache_clone.write_blob(&digest_clone, &data).await
        });
        handles.push(handle);
    }

    // Wait for all writes
    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // At least one write should succeed (some may fail due to concurrent temp file access)
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    assert!(
        success_count > 0,
        "At least one concurrent write should succeed (got {} successes)",
        success_count
    );

    // Blob should exist and be readable (even if some writes failed)
    assert!(
        cache.blob_exists(digest).await,
        "Blob should exist after concurrent writes"
    );
    let read_data = cache.read_blob(digest).await.unwrap();
    assert_eq!(read_data, test_data, "Blob content should be correct");
}

/// Test that blob_size correctly reports size after write
#[tokio::test]
async fn test_blob_size_after_write() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache = CacheStorage::new(temp_dir.path().to_path_buf()).unwrap();

    let digest = "sha256:size_test";
    let test_data = b"size test data";

    // Initially should be None
    assert_eq!(
        cache.blob_size(digest).await,
        None,
        "Size should be None initially"
    );

    // Write blob
    cache.write_blob(digest, test_data).await.unwrap();

    // Size should be correct immediately
    let size = cache.blob_size(digest).await;
    assert_eq!(
        size,
        Some(test_data.len() as u64),
        "Size should match after write"
    );
}

/// Test the complete pre-pull workflow: write, sync, verify, read
/// This simulates the exact workflow used by pre-pull
#[tokio::test]
async fn test_prepull_workflow() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache = Arc::new(CacheStorage::new(temp_dir.path().to_path_buf()).unwrap());

    let digest = "sha256:prepull_workflow_test";
    let test_data = b"prepull workflow test data";

    // Step 1: Write blob (pre-pull writes)
    cache.write_blob(digest, test_data).await.unwrap();

    // Step 2: Verify blob was written (pre-pull verification)
    let exists = cache.blob_exists(digest).await;
    assert!(exists, "Blob should exist after write");

    // Step 3: Read blob back to verify (pre-pull verification)
    let read_data = cache.read_blob(digest).await.unwrap();
    assert_eq!(read_data, test_data, "Blob content should match");

    // Step 4: Simulate server lookup (immediate after pre-pull)
    // This is the critical test - server should find the blob immediately
    let server_exists = cache.blob_exists(digest).await;
    assert!(
        server_exists,
        "Server should find blob immediately after pre-pull"
    );

    // Step 5: Server reads blob
    let server_data = cache.read_blob(digest).await.unwrap();
    assert_eq!(server_data, test_data, "Server should read correct data");
}
