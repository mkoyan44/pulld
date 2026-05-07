use crate::registry::manifest::AppState;
use crate::registry::upstream::UpstreamClient;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use futures::StreamExt;
use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio_util::io::ReaderStream;

/// GET /v2/{name}/blobs/{digest}
///
/// Simple proxy implementation:
/// 1. Check cache - if exists, serve from cache
/// 2. If not cached, stream directly from upstream (standard proxy behavior)
/// 3. Cache successful downloads in background
pub async fn get_blob(
    State(state): State<AppState>,
    Path((_name, digest)): Path<(String, String)>,
    req_headers: HeaderMap,
) -> impl IntoResponse {
    tracing::debug!(
        name = %_name,
        digest = %digest,
        "GET blob request"
    );

    // Parse Range header from client (e.g. "bytes=1234-")
    let client_range_start: Option<u64> = req_headers
        .get("range")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("bytes="))
        .and_then(|s| s.strip_suffix('-'))
        .and_then(|s| s.parse().ok());

    let cache = &state.cache;
    let blob_path = cache.blob_path(&digest);

    // Check cache first - use async metadata check for reliable filesystem state
    // This is more reliable than exists() for detecting recently written files
    tracing::debug!(
        digest = %digest,
        cache_path = %blob_path.display(),
        "Checking cache for blob"
    );

    // Also check for temp file (in case rename hasn't completed yet)
    let temp_path = blob_path.with_extension("tmp");

    match tokio::fs::metadata(&blob_path).await {
        Ok(metadata) => {
            let size = metadata.len();
            if size > 0 {
                tracing::info!(
                    digest = %digest,
                    size = size,
                    cache_path = %blob_path.display(),
                    "Cache HIT"
                );

                if let Ok(mut file) = File::open(&blob_path).await {
                    // Handle Range request for cached blobs
                    if let Some(start) = client_range_start {
                        if start < size {
                            use tokio::io::AsyncSeekExt;
                            if file.seek(std::io::SeekFrom::Start(start)).await.is_ok() {
                                let remaining = size - start;
                                let reader = BufReader::with_capacity(64 * 1024, file);
                                let stream = ReaderStream::new(reader);
                                let body = Body::from_stream(stream);

                                let mut headers = HeaderMap::new();
                                headers.insert(
                                    "Content-Type",
                                    "application/octet-stream".parse().unwrap(),
                                );
                                headers.insert(
                                    "Content-Length",
                                    remaining.to_string().parse().unwrap(),
                                );
                                headers.insert(
                                    "Content-Range",
                                    format!("bytes {}-{}/{}", start, size - 1, size)
                                        .parse()
                                        .unwrap(),
                                );
                                headers.insert("X-Cache", "HIT".parse().unwrap());
                                return (StatusCode::PARTIAL_CONTENT, headers, body)
                                    .into_response();
                            }
                        }
                    }

                    let reader = BufReader::with_capacity(64 * 1024, file);
                    let stream = ReaderStream::new(reader);
                    let body = Body::from_stream(stream);

                    let mut headers = HeaderMap::new();
                    headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
                    headers.insert("Content-Length", size.to_string().parse().unwrap());
                    headers.insert("X-Cache", "HIT".parse().unwrap());
                    return (StatusCode::OK, headers, body).into_response();
                } else {
                    tracing::warn!(
                        digest = %digest,
                        cache_path = %blob_path.display(),
                        "Cache file exists but cannot be opened"
                    );
                }
            } else {
                tracing::warn!(
                    digest = %digest,
                    cache_path = %blob_path.display(),
                    "Cache file exists but is empty (size=0)"
                );
                // Cache file exists but is invalid - delete it
                let _ = tokio::fs::remove_file(&blob_path).await;
            }
        }
        Err(e) => {
            // Check if temp file exists (rename in progress)
            if let Ok(temp_metadata) = tokio::fs::metadata(&temp_path).await {
                let temp_size = temp_metadata.len();
                if temp_size > 0 {
                    tracing::debug!(
                        digest = %digest,
                        temp_size = temp_size,
                        temp_path = %temp_path.display(),
                        "Cache file not found but temp file exists - waiting for rename to complete"
                    );
                    // Wait a bit for rename to complete, then retry
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    // Retry metadata check
                    if let Ok(metadata) = tokio::fs::metadata(&blob_path).await {
                        let size = metadata.len();
                        if size > 0 {
                            tracing::info!(
                                digest = %digest,
                                size = size,
                                cache_path = %blob_path.display(),
                                "Cache HIT (after waiting for rename)"
                            );
                            if let Ok(file) = File::open(&blob_path).await {
                                let reader = BufReader::with_capacity(64 * 1024, file);
                                let stream = ReaderStream::new(reader);
                                let body = Body::from_stream(stream);

                                let mut headers = HeaderMap::new();
                                headers.insert(
                                    "Content-Type",
                                    "application/octet-stream".parse().unwrap(),
                                );
                                headers.insert("Content-Length", size.to_string().parse().unwrap());
                                headers.insert("X-Cache", "HIT".parse().unwrap());
                                return (StatusCode::OK, headers, body).into_response();
                            }
                        }
                    }
                }
            }

            // Diagnostic: Check if parent directory exists and list files (for debugging)
            if let Some(parent) = blob_path.parent() {
                if let Ok(parent_metadata) = tokio::fs::metadata(parent).await {
                    if parent_metadata.is_dir() {
                        // Try to read directory to see what files are actually there
                        if let Ok(mut entries) = tokio::fs::read_dir(parent).await {
                            let mut found_files = Vec::new();
                            while let Ok(Some(entry)) = entries.next_entry().await {
                                if let Ok(name) = entry.file_name().into_string() {
                                    found_files.push(name);
                                }
                            }
                            let expected_name = blob_path
                                .file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or("unknown");
                            let has_file = found_files.iter().any(|f| f.as_str() == expected_name);
                            let has_temp = found_files
                                .iter()
                                .any(|f| f.as_str() == format!("{}.tmp", expected_name));

                            tracing::debug!(
                                digest = %digest,
                                cache_path = %blob_path.display(),
                                expected_filename = expected_name,
                                files_in_dir = ?found_files,
                                file_exists = has_file,
                                temp_exists = has_temp,
                                "Directory listing for cache diagnostic"
                            );
                        }
                    }
                }
            }

            if e.kind() == std::io::ErrorKind::NotFound {
                tracing::warn!(
                    digest = %digest,
                    cache_path = %blob_path.display(),
                    "Cache MISS - file not found"
                );
            } else {
                // Non-NotFound error (permission, etc.) - log at warn level
                tracing::warn!(
                    digest = %digest,
                    cache_path = %blob_path.display(),
                    error = %e,
                    error_kind = ?e.kind(),
                    "Cache check failed with non-NotFound error - will fetch from upstream"
                );
            }
        }
    }

    // Cache miss - fetch from upstream
    tracing::warn!(
        digest = %digest,
        name = %_name,
        cache_path = %blob_path.display(),
        "Cache MISS - fetching from upstream"
    );

    let (registry, repository) = crate::registry::manifest::parse_repository(&_name);
    let upstream_client = state.get_upstream_client(&registry);
    let mirrors = upstream_client.mirrors();

    if mirrors.is_empty() {
        return (StatusCode::BAD_GATEWAY, "No upstream mirrors configured").into_response();
    }

    let upstream_path = format!("/v2/{}/blobs/{}", repository, digest);
    let registry_config = state.get_registry_config(&registry);
    let strategy = registry_config.strategy;
    let hedge_delay_ms = registry_config.hedge_delay_ms;

    // Check for existing partial file for resume
    let temp_path = blob_path.with_extension("tmp");
    let mut resume_position: Option<u64> = None;
    let mut existing_hash = None;

    if let Ok(metadata) = tokio::fs::metadata(&temp_path).await {
        let partial_size = metadata.len();
        if partial_size > 0 {
            // Partial file exists - prepare for resume
            resume_position = Some(partial_size);
            tracing::info!(
                digest = %digest,
                resume_position = partial_size,
                "Found partial file, will resume download"
            );

            // Read existing partial content for hash calculation
            if let Ok(existing_data) = tokio::fs::read(&temp_path).await {
                let mut hasher = Sha256::new();
                hasher.update(&existing_data);
                existing_hash = Some(hasher);
            }
        }
    }

    // Build request with Range header if resuming
    let range_header = resume_position.map(|pos| format!("bytes={}-", pos));

    // Fetch from upstream
    let mut response = match crate::registry::race_mirrors(
        &upstream_client,
        mirrors,
        &upstream_path,
        None,
        strategy,
        hedge_delay_ms,
        None, // No Accept header for blob requests
        range_header.as_deref(),
        Some(&state.mirror_selector),
    )
    .await
    {
        Ok(resp) => resp,
        Err(e) => {
            tracing::error!(digest = %digest, error = %e, "Failed to fetch blob from upstream");
            return (StatusCode::BAD_GATEWAY, format!("Upstream error: {}", e)).into_response();
        }
    };

    // Handle 401 - get token and retry only the mirror that returned 401 (tokens are per-mirror)
    if response.status() == StatusCode::UNAUTHORIZED {
        if let Some(www_auth) = response.headers().get("www-authenticate") {
            if let Ok(auth_header) = www_auth.to_str() {
                if let Some(token) = crate::registry::manifest::fetch_registry_token_with_cache(
                    auth_header,
                    &repository,
                    Some(state.token_cache.clone()),
                )
                .await
                {
                    let realm = crate::registry::manifest::realm_from_www_authenticate(auth_header);
                    let mirror_ix = realm.as_ref().and_then(|r| {
                        crate::registry::manifest::mirror_index_from_realm(r, mirrors)
                    });
                    let mut mirror_failures: Vec<(String, String)> = Vec::new();
                    let retry_response = {
                        // Per-mirror auth retry: each mirror may have its own auth endpoint.
                        // Official Docker Hub mirrors share auth.docker.io, but third-party mirrors
                        // (docker.1ms.run, docker.m.daocloud.io) have their own token endpoints.
                        // When a mirror returns 401, fetch a token from ITS auth endpoint and retry.
                        let mut last_result = None;
                        let indices: Vec<usize> = if let Some(ix) = mirror_ix {
                            tracing::debug!(
                                realm = ?realm,
                                mirror_index = ix,
                                "Retrying 401 on matched mirror first, then others with per-mirror auth"
                            );
                            let mut v = vec![ix];
                            for i in 0..mirrors.len() {
                                if i != ix {
                                    v.push(i);
                                }
                            }
                            v
                        } else {
                            tracing::debug!(
                                realm = ?realm,
                                "401 realm doesn't match any mirror URL — trying all mirrors with per-mirror auth"
                            );
                            (0..mirrors.len()).collect()
                        };
                        for try_ix in indices {
                            let attempt = crate::registry::mirror_racer::request_single_mirror(
                                &upstream_client,
                                mirrors,
                                try_ix,
                                &upstream_path,
                                Some(&token),
                                range_header.as_deref(),
                            )
                            .await;
                            match attempt {
                                Ok(resp)
                                    if resp.status().is_success()
                                        || resp.status() == StatusCode::PARTIAL_CONTENT =>
                                {
                                    tracing::info!(
                                        digest = %digest,
                                        mirror_index = try_ix,
                                        mirror = %mirrors[try_ix],
                                        "Auth retry succeeded on mirror"
                                    );
                                    last_result = Some(Ok(resp));
                                    break;
                                }
                                Ok(resp) if resp.status() == StatusCode::UNAUTHORIZED => {
                                    // This mirror returned 401 — it needs its OWN token.
                                    // Extract www-authenticate, fetch a per-mirror token, and retry once.
                                    let mirror_www_auth = resp
                                        .headers()
                                        .get("www-authenticate")
                                        .and_then(|h| h.to_str().ok())
                                        .map(|s| s.to_string());
                                    if let Some(ref mwa) = mirror_www_auth {
                                        tracing::info!(
                                            digest = %digest,
                                            mirror_index = try_ix,
                                            mirror = %mirrors[try_ix],
                                            www_authenticate = %mwa,
                                            "Mirror returned 401 with own auth — fetching per-mirror token"
                                        );
                                        if let Some(mirror_token) = crate::registry::manifest::fetch_registry_token_with_cache(
                                            mwa,
                                            &repository,
                                            Some(state.token_cache.clone()),
                                        )
                                        .await
                                        {
                                            // Retry this specific mirror with its own token
                                            let mirror_retry = crate::registry::mirror_racer::request_single_mirror(
                                                &upstream_client,
                                                mirrors,
                                                try_ix,
                                                &upstream_path,
                                                Some(&mirror_token),
                                                range_header.as_deref(),
                                            )
                                            .await;
                                            match mirror_retry {
                                                Ok(r) if r.status().is_success() || r.status() == StatusCode::PARTIAL_CONTENT => {
                                                    tracing::info!(
                                                        digest = %digest,
                                                        mirror_index = try_ix,
                                                        mirror = %mirrors[try_ix],
                                                        "Per-mirror auth retry succeeded"
                                                    );
                                                    last_result = Some(Ok(r));
                                                    break;
                                                }
                                                other => {
                                                    let reason = match &other {
                                                        Ok(r) => format!("status {}", r.status()),
                                                        Err(e) => e.to_string(),
                                                    };
                                                    tracing::warn!(
                                                        digest = %digest,
                                                        mirror_index = try_ix,
                                                        mirror = %mirrors[try_ix],
                                                        reason = %reason,
                                                        "Per-mirror auth retry also failed, trying next mirror"
                                                    );
                                                    mirror_failures.push((mirrors[try_ix].to_string(), reason.clone()));
                                                    last_result = Some(other);
                                                }
                                            }
                                        } else {
                                            let reason = "per-mirror token fetch failed".to_string();
                                            tracing::warn!(
                                                digest = %digest,
                                                mirror_index = try_ix,
                                                mirror = %mirrors[try_ix],
                                                "Per-mirror token fetch failed, trying next mirror"
                                            );
                                            mirror_failures.push((mirrors[try_ix].to_string(), reason));
                                            last_result = Some(Ok(resp));
                                        }
                                    } else {
                                        let reason =
                                            "401 without www-authenticate header".to_string();
                                        tracing::warn!(
                                            digest = %digest,
                                            mirror_index = try_ix,
                                            mirror = %mirrors[try_ix],
                                            "Mirror returned 401 without www-authenticate, trying next"
                                        );
                                        mirror_failures.push((mirrors[try_ix].to_string(), reason));
                                        last_result = Some(Ok(resp));
                                    }
                                }
                                other => {
                                    let reason = match &other {
                                        Ok(resp) => format!("status {}", resp.status()),
                                        Err(e) => format!("{}", e),
                                    };
                                    tracing::warn!(
                                        digest = %digest,
                                        mirror_index = try_ix,
                                        mirror = %mirrors[try_ix],
                                        reason = %reason,
                                        "Auth retry failed on mirror, trying next"
                                    );
                                    mirror_failures
                                        .push((mirrors[try_ix].to_string(), reason.clone()));
                                    last_result = Some(other);
                                }
                            }
                        }
                        last_result.unwrap_or_else(|| {
                            Err(crate::error::DockerProxyError::Registry(
                                "No mirrors available for auth retry".to_string(),
                            ))
                        })
                    };
                    response = match retry_response {
                        Ok(resp) => resp,
                        Err(e) => {
                            let failure_summary = if mirror_failures.is_empty() {
                                "No mirrors attempted".to_string()
                            } else {
                                mirror_failures
                                    .iter()
                                    .map(|(mirror, reason)| format!("{}: {}", mirror, reason))
                                    .collect::<Vec<_>>()
                                    .join("; ")
                            };
                            tracing::error!(
                                digest = %digest,
                                error = %e,
                                mirror_failures = %failure_summary,
                                "Failed to fetch blob with auth (all {} mirrors exhausted)",
                                mirrors.len()
                            );
                            return (
                                StatusCode::BAD_GATEWAY,
                                format!(
                                    "Auth retry failed after trying {} mirror(s): {}. Last error: {}",
                                    mirrors.len(),
                                    failure_summary,
                                    e
                                )
                            )
                                .into_response();
                        }
                    };
                } else {
                    let realm = crate::registry::manifest::realm_from_www_authenticate(auth_header);
                    tracing::error!(
                        digest = %digest,
                        auth_header = %auth_header,
                        realm = ?realm,
                        "Token fetch failed (auth server unreachable?). \
                         Cannot retry 401 without a token. \
                         Returning diagnostic 502 to client."
                    );
                    // Return a diagnostic error instead of the raw 401, so the caller
                    // knows the blob server tried to handle auth but failed at token fetch.
                    return (
                        StatusCode::BAD_GATEWAY,
                        format!(
                            "Blob auth failed: token fetch from {:?} returned None (auth server unreachable from VM?). \
                             www-authenticate: {}",
                            realm, auth_header
                        ),
                    )
                        .into_response();
                }
            }
        } else {
            // 401 with no www-authenticate header — unusual
            return (
                StatusCode::BAD_GATEWAY,
                format!("Blob 401 without www-authenticate header for {}", digest),
            )
                .into_response();
        }
    }

    // Handle 206 Partial Content (Range request response) or other success statuses
    let is_partial_content = response.status() == StatusCode::PARTIAL_CONTENT;
    if !response.status().is_success() && !is_partial_content {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        tracing::error!(
            digest = %digest,
            status = %status,
            mirror_count = mirrors.len(),
            "Upstream returned error (failover was attempted across all {} mirror(s))",
            mirrors.len()
        );
        if status == StatusCode::BAD_GATEWAY {
            tracing::error!(
                digest = %digest,
                "502 Bad Gateway: all upstream mirrors failed for this layer. Check blob server config has multiple docker.io mirrors for failover."
            );
        }
        return (status, body).into_response();
    }

    if is_partial_content {
        tracing::debug!(digest = %digest, "Received 206 Partial Content - resuming download");
    }

    // Validate that the server is truly resuming from our requested position.
    // Some servers/CDNs return 206 but send the full blob body, which would
    // corrupt the file when appended to the existing partial data.
    let resume_position = if let Some(pos) = resume_position {
        if !is_partial_content {
            // Server returned 200 instead of 206 — sending full blob.
            tracing::warn!(
                digest = %digest,
                "Upstream returned 200 instead of 206 for Range request - restarting fresh download"
            );
            let temp_path = blob_path.with_extension("tmp");
            let _ = tokio::fs::remove_file(&temp_path).await;
            existing_hash = None;
            None
        } else {
            // Server returned 206 — verify Content-Range header confirms our
            // resume position (e.g. "bytes 56868402-57286169/57286170").
            let content_range = response
                .headers()
                .get("content-range")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let range_start = content_range
                .strip_prefix("bytes ")
                .and_then(|s| s.split('-').next())
                .and_then(|s| s.parse::<u64>().ok());

            if range_start == Some(pos) {
                tracing::debug!(
                    digest = %digest,
                    content_range = %content_range,
                    "Content-Range confirmed - resuming from {}",
                    pos
                );
                Some(pos)
            } else {
                // Server returned 206 but Content-Range doesn't match our
                // resume position — likely sending the full blob.
                tracing::warn!(
                    digest = %digest,
                    content_range = %content_range,
                    expected_start = pos,
                    "206 response Content-Range mismatch - restarting fresh download"
                );
                let temp_path = blob_path.with_extension("tmp");
                let _ = tokio::fs::remove_file(&temp_path).await;
                existing_hash = None;
                None
            }
        }
    } else {
        None
    };

    // Get content length from upstream
    let content_length = response
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    tracing::debug!(
        digest = %digest,
        content_length = content_length,
        "Streaming blob from upstream"
    );

    // Stream from upstream to client, caching in background
    let cache_clone = cache.clone();
    let digest_clone = digest.to_string();
    let expected_size = content_length;

    // Create a channel to tee the stream
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(32);

    let upstream_stream = response.bytes_stream();

    // Spawn background task to receive from upstream and send to client + cache
    let resume_pos = resume_position;
    let existing_hash_clone = existing_hash.clone();
    tokio::spawn(async move {
        let blob_path = cache_clone.blob_path(&digest_clone);

        // Create or open temp file for caching
        let temp_path = blob_path.with_extension("tmp");
        if let Some(parent) = blob_path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }

        // Open file in append mode if resuming, create mode otherwise
        let cache_file = if resume_pos.is_some() {
            // Resume: open in append mode
            tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&temp_path)
                .await
                .ok()
        } else {
            // Fresh download: create new file
            tokio::fs::File::create(&temp_path).await.ok()
        };

        // Initialize hasher with existing content if resuming
        let mut hasher = if let Some(existing_hasher) = existing_hash_clone {
            existing_hasher
        } else {
            Sha256::new()
        };

        // Start total_bytes from resume position if resuming
        let mut total_bytes = resume_pos.unwrap_or(0);
        let mut cache_writer = cache_file;

        futures::pin_mut!(upstream_stream);

        while let Some(chunk_result) = upstream_stream.next().await {
            match chunk_result {
                Ok(chunk) => {
                    total_bytes += chunk.len() as u64;
                    hasher.update(&chunk);

                    // Write to cache file if available
                    if let Some(ref mut file) = cache_writer {
                        if file.write_all(&chunk).await.is_err() {
                            cache_writer = None; // Stop caching on error
                        }
                    }

                    // Send to client
                    if tx.send(Ok(chunk)).await.is_err() {
                        break; // Client disconnected
                    }
                }
                Err(e) => {
                    tracing::error!(
                        digest = %digest_clone,
                        error = %e,
                        total_bytes = total_bytes,
                        "Upstream stream error - preserving partial file for resume"
                    );
                    let _ = tx.send(Err(std::io::Error::other(e))).await;
                    // Preserve partial cache file for resume (don't delete)
                    // The file will be used to resume on next attempt
                    return;
                }
            }
        }

        // Verify and finalize cache
        if let Some(file) = cache_writer {
            if file.sync_all().await.is_ok() {
                let calculated_digest = format!("sha256:{:x}", hasher.finalize());

                // Verify size if known
                let size_ok = expected_size.is_none_or(|expected| total_bytes == expected);
                let digest_ok = calculated_digest == digest_clone;

                if size_ok && digest_ok {
                    if tokio::fs::rename(&temp_path, &blob_path).await.is_ok() {
                        tracing::info!(digest = %digest_clone, size = total_bytes, "Blob cached successfully");
                    }
                } else {
                    tracing::warn!(
                        digest = %digest_clone,
                        expected_digest = %digest_clone,
                        calculated_digest = %calculated_digest,
                        expected_size = expected_size,
                        actual_size = total_bytes,
                        "Blob verification failed - not caching"
                    );
                    let _ = tokio::fs::remove_file(&temp_path).await;
                }
            } else {
                let _ = tokio::fs::remove_file(&temp_path).await;
            }
        }
    });

    // Convert receiver to stream for response body
    let body_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let body = Body::from_stream(body_stream);

    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
    if let Some(size) = content_length {
        headers.insert("Content-Length", size.to_string().parse().unwrap());
    }
    headers.insert("X-Cache", "MISS".parse().unwrap());

    (StatusCode::OK, headers, body).into_response()
}

/// HEAD /v2/{name}/blobs/{digest}
pub async fn head_blob(
    State(state): State<AppState>,
    Path((_name, digest)): Path<(String, String)>,
) -> impl IntoResponse {
    tracing::debug!(name = %_name, digest = %digest, "HEAD blob request");

    let cache = &state.cache;
    let blob_path = cache.blob_path(&digest);

    // Check cache - use async metadata check for reliable filesystem state
    if let Ok(metadata) = tokio::fs::metadata(&blob_path).await {
        let size = metadata.len();
        if size > 0 {
            let mut headers = HeaderMap::new();
            headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
            headers.insert("Content-Length", size.to_string().parse().unwrap());
            headers.insert("Docker-Content-Digest", digest.parse().unwrap());
            return (StatusCode::OK, headers).into_response();
        }
    }

    // Not in cache - check upstream
    let (registry, repository) = crate::registry::manifest::parse_repository(&_name);
    let upstream_client = state.get_upstream_client(&registry);
    let mirrors = upstream_client.mirrors();

    if mirrors.is_empty() {
        return StatusCode::NOT_FOUND.into_response();
    }

    let upstream_path = format!("/v2/{}/blobs/{}", repository, digest);

    // Try HEAD request to upstream
    for (idx, mirror) in mirrors.iter().enumerate() {
        let url = format!("{}{}", mirror, upstream_path);
        match upstream_client.client(idx).head(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                // Try to get Content-Length from HEAD response
                let content_length = resp
                    .headers()
                    .get("content-length")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok());

                // If Content-Length is missing, fetch it via GET with Range header
                // Fallback chain for blob size detection:
                // 1. Try HEAD request for Content-Length header (already attempted above)
                // 2. Fallback: GET with Range: bytes=0-0 to get Content-Range header
                // 3. Fallback: Parse Content-Range header (format: "bytes 0-0/12345")
                // 4. Fallback: Full GET request but only read headers (via fetch_blob_size_from_get)
                // 5. Final fallback: Return error if all methods fail
                // This chain handles different registry implementations that may not provide
                // Content-Length in HEAD responses or may require different methods to obtain size.
                let content_length = match content_length {
                    Some(size) => size,
                    None => {
                        tracing::debug!(
                            digest = %digest,
                            "Content-Length missing from HEAD response, fetching via GET with Range header"
                        );
                        // Try GET with Range: bytes=0-0 to get Content-Length without downloading
                        match upstream_client
                            .client(idx)
                            .get(&url)
                            .header("Range", "bytes=0-0")
                            .send()
                            .await
                        {
                            Ok(range_resp) if range_resp.status().is_success() => {
                                // Try to get Content-Length from Range response
                                if let Some(size_str) = range_resp
                                    .headers()
                                    .get("content-range")
                                    .and_then(|v| v.to_str().ok())
                                {
                                    // Parse Content-Range header: "bytes 0-0/12345" -> 12345
                                    if let Some(slash_idx) = size_str.rfind('/') {
                                        if let Ok(size) = size_str[slash_idx + 1..].parse::<u64>() {
                                            tracing::debug!(
                                                digest = %digest,
                                                size = size,
                                                "Got Content-Length from Content-Range header"
                                            );
                                            size
                                        } else {
                                            tracing::warn!(
                                                digest = %digest,
                                                "Failed to parse Content-Range header, trying full GET"
                                            );
                                            // Fallback: try full GET but only read headers
                                            match fetch_blob_size_from_get(
                                                upstream_client.as_ref(),
                                                idx,
                                                &url,
                                            )
                                            .await
                                            {
                                                Some(size) => size,
                                                None => {
                                                    tracing::error!(
                                                        digest = %digest,
                                                        "Failed to get blob size - Content-Length required"
                                                    );
                                                    return (
                                                        StatusCode::BAD_GATEWAY,
                                                        "Upstream registry did not provide Content-Length header",
                                                    )
                                                        .into_response();
                                                }
                                            }
                                        }
                                    } else {
                                        tracing::warn!(
                                            digest = %digest,
                                            "Invalid Content-Range header format, trying full GET"
                                        );
                                        match fetch_blob_size_from_get(
                                            upstream_client.as_ref(),
                                            idx,
                                            &url,
                                        )
                                        .await
                                        {
                                            Some(size) => size,
                                            None => {
                                                tracing::error!(
                                                    digest = %digest,
                                                    "Failed to get blob size - Content-Length required"
                                                );
                                                return (
                                                    StatusCode::BAD_GATEWAY,
                                                    "Upstream registry did not provide Content-Length header",
                                                )
                                                    .into_response();
                                            }
                                        }
                                    }
                                } else {
                                    // No Content-Range header, try full GET
                                    match fetch_blob_size_from_get(
                                        upstream_client.as_ref(),
                                        idx,
                                        &url,
                                    )
                                    .await
                                    {
                                        Some(size) => size,
                                        None => {
                                            tracing::error!(
                                                digest = %digest,
                                                "Failed to get blob size - Content-Length required"
                                            );
                                            return (
                                                StatusCode::BAD_GATEWAY,
                                                "Upstream registry did not provide Content-Length header",
                                            )
                                                .into_response();
                                        }
                                    }
                                }
                            }
                            _ => {
                                // Range request failed, try full GET
                                match fetch_blob_size_from_get(upstream_client.as_ref(), idx, &url)
                                    .await
                                {
                                    Some(size) => size,
                                    None => {
                                        tracing::error!(
                                            digest = %digest,
                                            "Failed to get blob size - Content-Length required"
                                        );
                                        return (
                                            StatusCode::BAD_GATEWAY,
                                            "Upstream registry did not provide Content-Length header",
                                        )
                                            .into_response();
                                    }
                                }
                            }
                        }
                    }
                };

                let mut headers = HeaderMap::new();
                headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
                headers.insert(
                    "Content-Length",
                    content_length.to_string().parse().unwrap(),
                );
                headers.insert("Docker-Content-Digest", digest.parse().unwrap());
                return (StatusCode::OK, headers).into_response();
            }
            _ => continue,
        }
    }

    StatusCode::NOT_FOUND.into_response()
}

/// Helper function to fetch blob size via GET request (only reads headers)
async fn fetch_blob_size_from_get(
    upstream_client: &UpstreamClient,
    mirror_idx: usize,
    url: &str,
) -> Option<u64> {
    tracing::debug!(url = %url, "Fetching blob size via GET request");
    match upstream_client.client(mirror_idx).get(url).send().await {
        Ok(resp) if resp.status().is_success() => resp
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok()),
        _ => None,
    }
}
