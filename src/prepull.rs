use crate::cache::CacheStorage;
use crate::config::{
    Config, DEFAULT_MANIFEST_ACCEPT_HEADER, DEFAULT_REGISTRY_NAME, DEFAULT_REGISTRY_URL,
};
use crate::error::{DockerProxyError, Result};
use crate::registry::upstream::UpstreamClient;
use futures::future;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Run pre-pull based on configuration
pub async fn run_pre_pull(cache_dir: PathBuf, config: &Config) -> Result<()> {
    if !config.pre_pull.enabled {
        info!("Pre-pull is disabled in configuration");
        return Ok(());
    }

    if config.pre_pull.images.is_empty() {
        info!("Pre-pull enabled but no images configured");
        return Ok(());
    }

    let prepull_start = std::time::Instant::now();
    info!(
        "Starting pre-pull of {} images (concurrency: {})",
        config.pre_pull.images.len(),
        config.pre_pull.image_concurrency
    );
    debug!("[prepull] Cache directory: {}", cache_dir.display());

    // Create cache storage (prepull doesn't enforce size limits - uses unlimited cache)
    let cache = Arc::new(CacheStorage::new(cache_dir)?);

    // Create upstream TLS config
    let upstream_tls = config
        .upstream
        .tls
        .as_ref()
        .unwrap_or(&crate::config::UpstreamTlsConfig {
            ca_bundle_path: None,
            use_system_ca: true,
            insecure_skip_verify: false,
        });

    // Create upstream clients for each configured registry
    let mut registry_clients: std::collections::HashMap<String, Arc<UpstreamClient>> =
        std::collections::HashMap::new();

    for (registry_name, registry_config) in &config.upstream.registries {
        let client = Arc::new(UpstreamClient::new(
            registry_config.mirrors.clone(),
            upstream_tls,
            Some(registry_config),
        )?);
        registry_clients.insert(registry_name.clone(), client);
    }

    // Create default client for docker.io if not configured
    let default_mirrors = vec![DEFAULT_REGISTRY_URL.to_string()];
    let default_client = Arc::new(UpstreamClient::new(default_mirrors, upstream_tls, None)?);

    // Ensure docker.io client exists
    if !registry_clients.contains_key(DEFAULT_REGISTRY_NAME) {
        registry_clients.insert(DEFAULT_REGISTRY_NAME.to_string(), default_client.clone());
    }

    let registry_clients = Arc::new(registry_clients);

    // Pull images in parallel with concurrency limit
    let semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.pre_pull.image_concurrency,
    ));
    let mut handles = Vec::new();

    for image_spec in &config.pre_pull.images {
        let image = image_spec.clone();
        let cache_clone = cache.clone();
        let registry_clients_clone = registry_clients.clone();
        let default_client_clone = default_client.clone();
        let semaphore_clone = semaphore.clone();
        let layer_concurrency = config.pre_pull.layer_concurrency;
        let strategy = config.pre_pull.mirror_strategy;
        let upstream_registries = config.upstream.registries.clone();

        let handle = tokio::spawn(async move {
            let _permit = semaphore_clone.acquire().await.unwrap();
            debug!("[prepull] Starting pull: {}", image);

            // Determine which registry this image belongs to
            let (name, _) = parse_image_reference(&image);
            let (registry, _) = parse_repository(&name);

            // Get appropriate upstream client for this registry
            let upstream_client = registry_clients_clone
                .get(&registry)
                .cloned()
                .unwrap_or(default_client_clone);

            debug!("[prepull] Using upstream client for registry: {}", registry);

            let hedge_delay_ms = upstream_registries
                .get(&registry)
                .map(|r| r.hedge_delay_ms)
                .unwrap_or(100);

            match pull_image(
                &image,
                cache_clone,
                upstream_client,
                layer_concurrency,
                strategy,
                hedge_delay_ms,
            )
            .await
            {
                Ok(size) => {
                    info!("[prepull] Completed: {} ({} bytes)", image, size);
                    Ok(())
                }
                Err(e) => {
                    error!("[prepull] Failed: {} - {}", image, e);
                    Err(e)
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all pulls to complete
    let results = future::join_all(handles).await;
    let mut success_count = 0;
    let mut fail_count = 0;

    for result in results {
        match result {
            Ok(Ok(_)) => success_count += 1,
            Ok(Err(_)) => fail_count += 1,
            Err(e) => {
                error!("[prepull] Task panicked: {}", e);
                fail_count += 1;
            }
        }
    }

    let prepull_duration = prepull_start.elapsed();
    info!(
        "Pre-pull completed: {} succeeded, {} failed in {}ms ({:.2}s)",
        success_count,
        fail_count,
        prepull_duration.as_millis(),
        prepull_duration.as_secs_f64()
    );
    tracing::info!(
        "[TIMING] Image pre-pull completed in {}ms",
        prepull_duration.as_millis()
    );

    // Pre-pull Helm charts in parallel with image pre-pull (if charts are configured)
    // Start chart pre-pull asynchronously so it can run in parallel with image pulls
    let chart_prepull_handle = if !config.pre_pull.charts.is_empty() {
        let cache_clone = cache.clone();
        let config_clone = config.clone();
        Some(tokio::spawn(async move {
            let chart_prepull_start = std::time::Instant::now();
            info!(
                "Starting pre-pull of {} Helm charts (concurrency: {})",
                config_clone.pre_pull.charts.len(),
                config_clone.pre_pull.image_concurrency // Reuse image concurrency for charts
            );

            match prepull_charts(cache_clone, &config_clone).await {
                Ok(chart_success) => {
                    let chart_prepull_duration = chart_prepull_start.elapsed();
                    info!(
                        "Helm chart pre-pull completed: {} succeeded in {}ms ({:.2}s)",
                        chart_success,
                        chart_prepull_duration.as_millis(),
                        chart_prepull_duration.as_secs_f64()
                    );
                    tracing::info!(
                        "[TIMING] Helm chart pre-pull completed in {}ms",
                        chart_prepull_duration.as_millis()
                    );
                    Ok(())
                }
                Err(e) => {
                    let chart_prepull_duration = chart_prepull_start.elapsed();
                    warn!(
                        "Helm chart pre-pull completed with errors in {}ms ({:.2}s): {}",
                        chart_prepull_duration.as_millis(),
                        chart_prepull_duration.as_secs_f64(),
                        e
                    );
                    tracing::warn!(
                        "[TIMING] Helm chart pre-pull failed after {}ms: {}",
                        chart_prepull_duration.as_millis(),
                        e
                    );
                    Err(e)
                }
            }
        }))
    } else {
        None
    };

    // Wait for chart pre-pull to complete (if started)
    if let Some(handle) = chart_prepull_handle {
        match handle.await {
            Ok(Ok(_)) => {
                info!("[prepull] Chart pre-pull completed successfully");
            }
            Ok(Err(e)) => {
                warn!("[prepull] Chart pre-pull completed with errors: {}", e);
            }
            Err(e) => {
                error!("[prepull] Chart pre-pull task panicked: {}", e);
            }
        }
    }

    Ok(())
}

/// Pull a single image (manifest + all layers)
pub async fn pull_image(
    image_ref: &str,
    cache: Arc<CacheStorage>,
    upstream: Arc<UpstreamClient>,
    layer_concurrency: usize,
    strategy: crate::config::MirrorStrategy,
    hedge_delay_ms: u64,
) -> Result<u64> {
    // Parse image reference (e.g., "nginx:latest" or "docker.io/library/nginx:latest")
    let (name, reference) = parse_image_reference(image_ref);
    let (registry, repository) = parse_repository(&name);

    debug!(
        "[prepull] Parsed image: registry={}, repository={}, reference={}",
        registry, repository, reference
    );

    // Fetch manifest
    let manifest_data = fetch_manifest(
        &registry,
        &repository,
        &reference,
        &cache,
        &upstream,
        strategy,
        hedge_delay_ms,
    )
    .await?;
    let manifest: Value = serde_json::from_slice(&manifest_data)
        .map_err(|e| DockerProxyError::Registry(format!("Failed to parse manifest: {}", e)))?;

    // Check if this is a manifest list - if so, fetch ALL platform-specific manifests
    let media_type = manifest
        .get("mediaType")
        .and_then(|m| m.as_str())
        .unwrap_or("");

    let mut all_layer_digests = HashSet::new();

    if media_type.contains("manifest.list") || media_type.contains("index") {
        // Detect host platform — must match manifest.rs runtime handler exactly
        let target_arch = match std::env::consts::ARCH {
            "x86_64" => "amd64",
            "aarch64" => "arm64",
            other => other,
        };
        let target_os = "linux";

        debug!(
            "[prepull] Manifest list detected for {}, fetching {}/{} platform-specific manifest",
            image_ref, target_os, target_arch
        );

        // Find host-platform manifest from the manifest list
        if let Some(manifests) = manifest.get("manifests").and_then(|m| m.as_array()) {
            debug!(
                "[prepull] Found {} platform-specific manifests in list, looking for {}/{}",
                manifests.len(),
                target_os,
                target_arch
            );

            let mut selected_digest = None;
            let mut selected_platform = None;

            // First pass: look for host platform specifically
            for manifest_ref in manifests {
                let platform = manifest_ref.get("platform");
                let is_target_platform = platform
                    .map(|p| {
                        p.get("os").and_then(|os| os.as_str()) == Some(target_os)
                            && p.get("architecture").and_then(|arch| arch.as_str())
                                == Some(target_arch)
                    })
                    .unwrap_or(false);

                if let Some(digest) = manifest_ref.get("digest").and_then(|d| d.as_str()) {
                    if is_target_platform {
                        selected_digest = Some(digest.to_string());
                        selected_platform = Some(format!("{}/{}", target_os, target_arch));
                        break;
                    } else if selected_digest.is_none() {
                        // Fallback: use first available platform if host platform not found
                        let platform_info = platform
                            .map(|p| {
                                let os =
                                    p.get("os").and_then(|os| os.as_str()).unwrap_or("unknown");
                                let arch = p
                                    .get("architecture")
                                    .and_then(|arch| arch.as_str())
                                    .unwrap_or("unknown");
                                format!("{}/{}", os, arch)
                            })
                            .unwrap_or_else(|| "unknown".to_string());
                        selected_digest = Some(digest.to_string());
                        selected_platform = Some(platform_info);
                    }
                }
            }

            if let Some(digest) = selected_digest {
                let platform_info = selected_platform
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());
                debug!(
                    "[prepull] Fetching platform-specific manifest for {}: {}",
                    platform_info, digest
                );

                // Fetch the platform-specific manifest using the digest
                let platform_manifest_data = fetch_manifest(
                    &registry,
                    &repository,
                    &digest,
                    &cache,
                    &upstream,
                    strategy,
                    hedge_delay_ms,
                )
                .await?;

                let platform_manifest: Value = serde_json::from_slice(&platform_manifest_data)
                    .map_err(|e| {
                        DockerProxyError::Registry(format!(
                            "Failed to parse platform manifest {}: {}",
                            digest, e
                        ))
                    })?;

                // Extract layer digests from this platform-specific manifest
                let platform_layers = extract_layer_digests(&platform_manifest)?;
                debug!(
                    "[prepull] Platform {} has {} layers",
                    platform_info,
                    platform_layers.len()
                );

                // Add all layer digests to the set
                for layer_digest in platform_layers {
                    all_layer_digests.insert(layer_digest);
                }

                debug!(
                    "[prepull] Collected {} layer digests from {} platform",
                    all_layer_digests.len(),
                    platform_info
                );
            } else {
                return Err(DockerProxyError::Registry(
                    "No platform manifest found in manifest list".to_string(),
                ));
            }
        } else {
            return Err(DockerProxyError::Registry(
                "Manifest list has no manifests array".to_string(),
            ));
        }
    } else {
        // Not a manifest list - extract layer digests from the single manifest
        let layer_digests = extract_layer_digests(&manifest)?;
        for digest in layer_digests {
            all_layer_digests.insert(digest);
        }
    }

    // Convert HashSet to Vec for processing
    let layer_digests: Vec<String> = all_layer_digests.into_iter().collect();
    debug!(
        "[prepull] Found {} layers for {}",
        layer_digests.len(),
        image_ref
    );

    // Pull all layers in parallel with concurrency limit
    let semaphore = Arc::new(tokio::sync::Semaphore::new(layer_concurrency));
    let mut handles = Vec::new();

    for digest in layer_digests {
        let cache_clone = cache.clone();
        let upstream_clone = upstream.clone();
        let repository_clone = repository.clone();
        let semaphore_clone = semaphore.clone();

        let strategy_clone = strategy;
        let hedge_delay_ms_clone = hedge_delay_ms;
        let handle = tokio::spawn(async move {
            let _permit = semaphore_clone.acquire().await.unwrap();
            pull_blob(
                &digest,
                &repository_clone,
                cache_clone,
                upstream_clone,
                strategy_clone,
                hedge_delay_ms_clone,
            )
            .await
        });
        handles.push(handle);
    }

    // Wait for all layers to complete
    let results = future::join_all(handles).await;
    let mut total_size = 0u64;
    let mut layer_count = 0;
    let mut failed_count = 0;

    for result in results {
        match result {
            Ok(Ok(size)) => {
                total_size += size;
                layer_count += 1;
            }
            Ok(Err(e)) => {
                warn!("[prepull] Failed to pull layer: {}", e);
                failed_count += 1;
            }
            Err(e) => {
                warn!("[prepull] Layer pull task panicked: {}", e);
                failed_count += 1;
            }
        }
    }

    // Fail if any layers failed
    if failed_count > 0 {
        let total_layers = layer_count + failed_count;
        return Err(DockerProxyError::Registry(format!(
            "Failed to pull {}/{} layers for image {}",
            failed_count, total_layers, image_ref
        )));
    }

    debug!(
        "[prepull] Pulled {} layers, total size: {} bytes",
        layer_count, total_size
    );
    Ok(total_size)
}

/// Parse image reference into name and tag/digest
fn parse_image_reference(image_ref: &str) -> (String, String) {
    // Handle digest references (e.g., "nginx@sha256:...")
    if let Some(at_idx) = image_ref.find('@') {
        let name = image_ref[..at_idx].to_string();
        let digest = image_ref[at_idx + 1..].to_string();
        return (name, digest);
    }

    // Handle tag references (e.g., "nginx:latest" or "nginx")
    if let Some(colon_idx) = image_ref.rfind(':') {
        // Check if it's a port number (e.g., "localhost:5000/image")
        // Simple heuristic: if there's a / before the :, it's likely a tag
        let before_colon = &image_ref[..colon_idx];
        if before_colon.contains('/') || !before_colon.contains('.') {
            let name = image_ref[..colon_idx].to_string();
            let tag = image_ref[colon_idx + 1..].to_string();
            return (name, tag);
        }
    }

    // No tag specified, default to "latest"
    (image_ref.to_string(), "latest".to_string())
}

/// Parse repository name into registry and repository parts.
/// Handles pulld.internal:5050 prefix (Linux pre-pull): routes to correct upstream registry.
fn parse_repository(name: &str) -> (String, String) {
    // Handle pulld.internal prefix for Linux (pre-pull images rewritten to use proxy URL)
    if name.starts_with("pulld.internal:5050/") {
        let path = name.strip_prefix("pulld.internal:5050/").unwrap_or(name);

        // Route based on image path:
        // - rancher/* images → docker.io (Docker Hub)
        if path.starts_with("rancher/") {
            return ("docker.io".to_string(), path.to_string());
        }

        // Default: K8s control plane images use registry.k8s.io
        return ("registry.k8s.io".to_string(), path.to_string());
    }

    // Format: registry/repository or just repository (defaults to docker.io)
    if let Some(slash_idx) = name.find('/') {
        let registry = name[..slash_idx].to_string();
        let repo = name[slash_idx + 1..].to_string();
        (registry, repo)
    } else {
        (
            DEFAULT_REGISTRY_NAME.to_string(),
            format!("library/{}", name),
        )
    }
}

/// Fetch manifest from upstream or cache
async fn fetch_manifest(
    registry: &str,
    repository: &str,
    reference: &str,
    cache: &Arc<CacheStorage>,
    upstream: &Arc<UpstreamClient>,
    strategy: crate::config::MirrorStrategy,
    hedge_delay_ms: u64,
) -> Result<Vec<u8>> {
    // Check cache first - try both digest-based and tag-based lookup for backward compatibility
    if reference.starts_with("sha256:") {
        // Direct digest lookup
        if let Ok(manifest_data) = cache
            .read_manifest_by_digest(registry, repository, reference)
            .await
        {
            debug!(
                "[prepull] Manifest cache hit (by digest): {}/{}:{}",
                registry, repository, reference
            );
            return Ok(manifest_data);
        }
    } else {
        // Tag lookup: check tag-to-digest mapping first, then lookup by digest
        if let Ok(Some(mapped_digest)) = cache
            .read_tag_digest_mapping(registry, repository, reference)
            .await
        {
            if let Ok(manifest_data) = cache
                .read_manifest_by_digest(registry, repository, &mapped_digest)
                .await
            {
                debug!(
                    "[prepull] Manifest cache hit (by tag mapping): {}/{}:{} -> {}",
                    registry, repository, reference, mapped_digest
                );
                return Ok(manifest_data);
            }
        }
        // DEPRECATED: Legacy tag-based manifest cache lookup for backward compatibility
        if let Ok(manifest_data) = cache.read_manifest(registry, repository, reference).await {
            debug!(
                "[prepull] Manifest cache hit (legacy tag-based): {}/{}:{}",
                registry, repository, reference
            );
            return Ok(manifest_data);
        }
    }

    // Cache miss - fetch from upstream
    let mirrors = upstream.mirrors();
    if mirrors.is_empty() {
        return Err(DockerProxyError::Registry(
            "No upstream mirrors configured".to_string(),
        ));
    }

    let upstream_path = format!("/v2/{}/manifests/{}", repository, reference);

    // Race all mirrors for fastest response
    let response = match crate::registry::race_mirrors(
        upstream,
        mirrors,
        &upstream_path,
        None, // No auth token for pre-pull
        strategy,
        hedge_delay_ms,
        Some(DEFAULT_MANIFEST_ACCEPT_HEADER), // Default manifest Accept header
        None,                                 // No Range header for manifest requests
        None,                                 // No mirror selector in prepull context
    )
    .await
    {
        Ok(resp) => resp,
        Err(e) => {
            return Err(DockerProxyError::Registry(format!(
                "Failed to fetch manifest from upstream: {}",
                e
            )));
        }
    };

    // Handle authentication if needed
    let response = if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        // Extract WWW-Authenticate header before consuming response
        let www_auth = response
            .headers()
            .get("www-authenticate")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());

        if let Some(auth_header) = www_auth {
            if let Some(token) =
                crate::registry::manifest::fetch_registry_token(&auth_header, repository).await
            {
                // Retry racing all mirrors with token
                debug!(
                    "[prepull] Authenticating with token for {}/{}:{}",
                    registry, repository, reference
                );
                match crate::registry::race_mirrors(
                    upstream,
                    mirrors,
                    &upstream_path,
                    Some(&token),
                    strategy,
                    hedge_delay_ms,
                    Some(DEFAULT_MANIFEST_ACCEPT_HEADER), // Default manifest Accept header
                    None,                                 // No Range header for manifest requests
                    None,                                 // No mirror selector in prepull context
                )
                .await
                {
                    Ok(resp) => resp,
                    Err(e) => {
                        return Err(DockerProxyError::Registry(format!(
                            "Failed to retry manifest request with token: {}",
                            e
                        )));
                    }
                }
            } else {
                return Err(DockerProxyError::Registry(
                    "Failed to obtain authentication token".to_string(),
                ));
            }
        } else {
            return Err(DockerProxyError::Registry(
                "No WWW-Authenticate header in 401 response".to_string(),
            ));
        }
    } else {
        response
    };

    if !response.status().is_success() {
        return Err(DockerProxyError::Registry(format!(
            "Failed to fetch manifest: HTTP {}",
            response.status()
        )));
    }

    // Extract Docker-Content-Digest header before consuming response body (matches handler behavior)
    // Try both lowercase and canonical case (HTTP headers are case-insensitive but some servers use canonical case)
    let docker_content_digest = response
        .headers()
        .get("docker-content-digest")
        .or_else(|| response.headers().get("Docker-Content-Digest"))
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());

    let manifest_data = response
        .bytes()
        .await
        .map_err(DockerProxyError::Http)?
        .to_vec();

    // Use Docker-Content-Digest header if available, otherwise calculate (matches handler behavior)
    let digest = docker_content_digest
        .unwrap_or_else(|| format!("sha256:{:x}", Sha256::digest(&manifest_data)));

    // Cache the manifest by digest (content-addressable storage - matches handler lookup)
    if let Err(e) = cache
        .write_manifest_by_digest(registry, repository, &digest, &manifest_data)
        .await
    {
        warn!("[prepull] Failed to cache manifest by digest: {}", e);
    } else {
        debug!(
            "[prepull] Cached manifest by digest: {}/{}:{}",
            registry, repository, digest
        );
    }

    // If reference is a tag (not a digest), also write tag-to-digest mapping
    if !reference.starts_with("sha256:") {
        if let Err(e) = cache
            .write_tag_digest_mapping(registry, repository, reference, &digest)
            .await
        {
            warn!("[prepull] Failed to write tag-to-digest mapping: {}", e);
        } else {
            debug!(
                "[prepull] Cached tag-to-digest mapping: {}/{}:{} -> {}",
                registry, repository, reference, digest
            );
        }

        // Also write using legacy method for backward compatibility
        // This ensures old cached manifests can still be found during transition
        if let Err(e) = cache
            .write_manifest(registry, repository, reference, &manifest_data)
            .await
        {
            warn!("[prepull] Failed to cache manifest (legacy method): {}", e);
        }
    }

    Ok(manifest_data)
}

/// Extract layer digests from manifest JSON
fn extract_layer_digests(manifest: &Value) -> Result<Vec<String>> {
    let mut digests = Vec::new();

    // Check if this is a manifest list (multi-arch)
    let media_type = manifest
        .get("mediaType")
        .and_then(|m| m.as_str())
        .unwrap_or("");

    if media_type.contains("manifest.list") || media_type.contains("index") {
        // This is a manifest list - we need to find the appropriate platform manifest
        // For now, try to get the first manifest (or one for linux/amd64)
        if let Some(manifests) = manifest.get("manifests").and_then(|m| m.as_array()) {
            for manifest_ref in manifests {
                // Prefer linux/amd64, but take any if not found
                let platform = manifest_ref.get("platform");
                let is_linux_amd64 = platform
                    .map(|p| {
                        p.get("os").and_then(|os| os.as_str()) == Some("linux")
                            && p.get("architecture").and_then(|arch| arch.as_str()) == Some("amd64")
                    })
                    .unwrap_or(false);

                if is_linux_amd64 || digests.is_empty() {
                    if let Some(digest) = manifest_ref.get("digest").and_then(|d| d.as_str()) {
                        // Return the digest of the platform-specific manifest
                        // The caller will need to fetch this manifest separately
                        // For now, we'll return it as a single digest to fetch
                        digests.push(digest.to_string());
                        if is_linux_amd64 {
                            break; // Found preferred platform
                        }
                    }
                }
            }

            if !digests.is_empty() {
                // Return the manifest digest - caller should fetch the actual manifest
                warn!("[prepull] Manifest list detected, will fetch platform-specific manifest");
                return Ok(digests);
            }
        }
    }

    // Try manifest v2 format (layers array)
    if let Some(layers) = manifest.get("layers").and_then(|l| l.as_array()) {
        for layer in layers {
            if let Some(digest) = layer.get("digest").and_then(|d| d.as_str()) {
                digests.push(digest.to_string());
            }
        }
        // Extract config blob (ALWAYS needed, even if layers exist)
        if let Some(config) = manifest.get("config") {
            if let Some(digest) = config.get("digest").and_then(|d| d.as_str()) {
                digests.push(digest.to_string());
            }
        }
        if !digests.is_empty() {
            return Ok(digests);
        }
    }

    // Try OCI format (same structure)
    if let Some(layers) = manifest.get("layers").and_then(|l| l.as_array()) {
        for layer in layers {
            if let Some(digest) = layer.get("digest").and_then(|d| d.as_str()) {
                digests.push(digest.to_string());
            }
        }
        // Extract config blob (ALWAYS needed, even if layers exist)
        if let Some(config) = manifest.get("config") {
            if let Some(digest) = config.get("digest").and_then(|d| d.as_str()) {
                digests.push(digest.to_string());
            }
        }
        if !digests.is_empty() {
            return Ok(digests);
        }
    }

    // Try config blob (needed for some formats)
    if let Some(config) = manifest.get("config") {
        if let Some(digest) = config.get("digest").and_then(|d| d.as_str()) {
            digests.push(digest.to_string());
        }
    }

    if digests.is_empty() {
        // Log manifest structure for debugging
        warn!(
            "[prepull] Manifest structure: mediaType={}, keys={:?}",
            media_type,
            manifest.as_object().map(|o| o.keys().collect::<Vec<_>>())
        );
        return Err(DockerProxyError::Registry(
            "No layers found in manifest".to_string(),
        ));
    }

    Ok(digests)
}

/// Pull a single blob (layer) from upstream or cache
async fn pull_blob(
    digest: &str,
    repository: &str,
    cache: Arc<CacheStorage>,
    upstream: Arc<UpstreamClient>,
    strategy: crate::config::MirrorStrategy,
    hedge_delay_ms: u64,
) -> Result<u64> {
    use std::time::Duration;

    // Check cache first
    if let Ok(blob_data) = cache.read_blob(digest).await {
        return Ok(blob_data.len() as u64);
    }

    // Cache miss - fetch from upstream with retry logic
    let mut last_error = None;
    for attempt in 1..=3 {
        match pull_blob_attempt(
            digest,
            repository,
            cache.clone(),
            upstream.clone(),
            strategy,
            hedge_delay_ms,
        )
        .await
        {
            Ok(size) => return Ok(size),
            Err(e) => {
                last_error = Some(e);
                if attempt < 3 {
                    warn!(
                        "[prepull] Retry {}/3 for blob {} (repository: {}): {}",
                        attempt + 1,
                        digest,
                        repository,
                        last_error.as_ref().unwrap()
                    );
                    tokio::time::sleep(Duration::from_secs(attempt as u64 * 2)).await;
                } else {
                    warn!(
                        "[prepull] Failed to pull blob {} for {} after 3 attempts: {}",
                        digest,
                        repository,
                        last_error.as_ref().unwrap()
                    );
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        DockerProxyError::Registry(format!("Failed to pull blob {}: Unknown error", digest))
    }))
}

/// Single attempt to pull a blob from upstream
async fn pull_blob_attempt(
    digest: &str,
    repository: &str,
    cache: Arc<CacheStorage>,
    upstream: Arc<UpstreamClient>,
    strategy: crate::config::MirrorStrategy,
    hedge_delay_ms: u64,
) -> Result<u64> {
    // Cache miss - fetch from upstream
    let mirrors = upstream.mirrors();
    if mirrors.is_empty() {
        return Err(DockerProxyError::Registry(
            "No upstream mirrors configured".to_string(),
        ));
    }

    let upstream_path = format!("/v2/{}/blobs/{}", repository, digest);

    // Race all mirrors for fastest response (blob request - no Accept header needed)
    let response = match crate::registry::race_mirrors(
        &upstream,
        mirrors,
        &upstream_path,
        None,
        strategy,
        hedge_delay_ms,
        None, // Blob requests don't need Accept header
        None, // No Range header (prepull doesn't support resume yet)
        None, // No mirror selector in prepull context
    )
    .await
    {
        Ok(resp) => resp,
        Err(e) => {
            return Err(DockerProxyError::Registry(format!(
                "Failed to fetch blob from upstream: {}",
                e
            )));
        }
    };

    // Handle authentication if needed
    let response = if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        // Extract WWW-Authenticate header before consuming response
        let www_auth = response
            .headers()
            .get("www-authenticate")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());

        if let Some(auth_header) = www_auth {
            debug!(
                "[prepull] Fetching token for blob {} (repository: {})",
                digest, repository
            );
            if let Some(token) =
                crate::registry::manifest::fetch_registry_token(&auth_header, repository).await
            {
                debug!("[prepull] Token obtained, retrying blob request with primary mirror only");
                // Retry with primary mirror only (ghcr.io) - proxy mirrors may not accept tokens
                // Use only the first mirror (primary) for authenticated requests
                let primary_mirror = mirrors.first().ok_or_else(|| {
                    DockerProxyError::Registry("No mirrors configured".to_string())
                })?;
                let primary_url = format!("{}{}", primary_mirror, upstream_path);
                let client = upstream.client(0).clone();
                let mut request = client.get(&primary_url);
                request = request.header("Authorization", format!("Bearer {}", token));
                match request.send().await {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            debug!("[prepull] Blob request with token succeeded for {}", digest);
                            resp
                        } else {
                            return Err(DockerProxyError::Registry(format!(
                                "Failed to fetch blob with token: HTTP {}",
                                resp.status()
                            )));
                        }
                    }
                    Err(e) => {
                        return Err(DockerProxyError::Registry(format!(
                            "Failed to retry blob request with token: {}",
                            e
                        )));
                    }
                }
            } else {
                warn!(
                    "[prepull] Failed to obtain authentication token for blob {}",
                    digest
                );
                return Err(DockerProxyError::Registry(
                    "Failed to obtain authentication token for blob".to_string(),
                ));
            }
        } else {
            return Err(DockerProxyError::Registry(
                "No WWW-Authenticate header in 401 response for blob".to_string(),
            ));
        }
    } else {
        response
    };

    if !response.status().is_success() {
        return Err(DockerProxyError::Registry(format!(
            "Failed to fetch blob: HTTP {}",
            response.status()
        )));
    }

    let blob_data = response
        .bytes()
        .await
        .map_err(DockerProxyError::Http)?
        .to_vec();

    let size = blob_data.len() as u64;

    // Cache the blob
    debug!("[prepull] Caching blob {} ({} bytes)", digest, size);
    if let Err(e) = cache.write_blob(digest, &blob_data).await {
        warn!("[prepull] Failed to cache blob {}: {}", digest, e);
    } else {
        // Verify the blob was written and is readable
        if let Ok(verified_data) = cache.read_blob(digest).await {
            if verified_data.len() == blob_data.len() {
                tracing::debug!(
                    "[prepull] Verified cached blob {}: {} bytes",
                    digest,
                    verified_data.len()
                );
            } else {
                warn!(
                    "[prepull] Blob {} size mismatch: expected {}, got {}",
                    digest,
                    blob_data.len(),
                    verified_data.len()
                );
            }
        } else {
            warn!(
                "[prepull] Failed to verify cached blob {}: not readable",
                digest
            );
        }
    }

    Ok(size)
}

/// Pre-pull Helm charts based on configuration
async fn prepull_charts(cache: Arc<CacheStorage>, config: &Config) -> Result<usize> {
    use reqwest::Client;
    use serde_yaml::Value as YamlValue;
    use std::time::Duration;

    let mut success_count = 0;
    let chart_specs = config.pre_pull.charts.clone();
    let helm_repos = config.helm.repositories.clone();

    // Create semaphore for concurrency control (reuse image_concurrency)
    let semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.pre_pull.image_concurrency,
    ));

    let mut handles = Vec::new();

    for chart_spec in chart_specs {
        let cache_clone = cache.clone();
        let helm_repos_clone = helm_repos.clone();
        let semaphore_clone = semaphore.clone();

        let handle = tokio::spawn(async move {
            let _permit = semaphore_clone.acquire().await.unwrap();
            debug!("[prepull] Starting chart pull: {}", chart_spec);

            // Parse chart spec: format is "repo/chart-name:version" or "repo/chart-name-version.tgz"
            let parts: Vec<&str> = chart_spec.split('/').collect();
            if parts.len() != 2 {
                warn!(
                    "[prepull] Invalid chart spec format: {} (expected: repo/chart-name:version or repo/chart-filename.tgz)",
                    chart_spec
                );
                return Err(DockerProxyError::Config(format!(
                    "Invalid chart spec format: {}",
                    chart_spec
                )));
            }

            let repo_name = parts[0];
            let chart_specifier = parts[1];

            // Get repository URL from config
            let repo_url = match helm_repos_clone.get(repo_name) {
                Some(url) => url.clone(),
                None => {
                    warn!(
                        "[prepull] Helm repository '{}' not found in configuration",
                        repo_name
                    );
                    return Err(DockerProxyError::Config(format!(
                        "Helm repository '{}' not found in configuration",
                        repo_name
                    )));
                }
            };

            // Try to determine chart name and version from specifier
            // Format can be: "chart-name:version" or "chart-name-version.tgz"
            let (chart_name, chart_version, chart_filename) = if chart_specifier.contains(':') {
                // Format: "chart-name:version"
                let name_version: Vec<&str> = chart_specifier.splitn(2, ':').collect();
                let name = name_version[0];
                let version = name_version[1];
                let filename = format!("{}-{}.tgz", name, version);
                (name.to_string(), Some(version.to_string()), filename)
            } else if chart_specifier.ends_with(".tgz") {
                // Format: "chart-name-version.tgz" - use as-is but extract name/version if possible
                let name = chart_specifier
                    .strip_suffix(".tgz")
                    .unwrap_or(chart_specifier);
                // Try to extract version from filename (assumes format: name-version.tgz)
                let parts: Vec<&str> = name.splitn(2, '-').collect();
                if parts.len() == 2 {
                    (
                        parts[0].to_string(),
                        Some(parts[1].to_string()),
                        chart_specifier.to_string(),
                    )
                } else {
                    (name.to_string(), None, chart_specifier.to_string())
                }
            } else {
                warn!(
                    "[prepull] Invalid chart specifier format: {} (expected format: chart-name:version or chart-name-version.tgz)",
                    chart_specifier
                );
                return Err(DockerProxyError::Config(format!(
                    "Invalid chart specifier format: {}",
                    chart_specifier
                )));
            };

            // Check cache first using the extracted filename
            if cache_clone.chart_exists(repo_name, &chart_filename).await {
                debug!(
                    "[prepull] Chart already cached: {}/{}",
                    repo_name, chart_filename
                );
                return Ok(());
            }

            // Fetch index.yaml to get the actual chart URL
            let index_url = if repo_url.ends_with('/') {
                format!("{}index.yaml", repo_url)
            } else {
                format!("{}/index.yaml", repo_url)
            };

            let client = Client::builder()
                .timeout(Duration::from_secs(300))
                .build()
                .map_err(|e| {
                    DockerProxyError::Config(format!("Failed to create HTTP client: {}", e))
                })?;

            // Fetch and parse index.yaml to find the correct chart URL
            let chart_url = match client.get(&index_url).send().await {
                Ok(response) if response.status().is_success() => {
                    match response.text().await {
                        Ok(index_yaml) => {
                            // Parse index.yaml to find chart URL
                            match serde_yaml::from_str::<YamlValue>(&index_yaml) {
                                Ok(index) => {
                                    // Navigate to entries[chart_name] and find version
                                    if let Some(entries) =
                                        index.get("entries").and_then(|e| e.as_mapping())
                                    {
                                        if let Some(chart_entries) =
                                            entries.get(&chart_name).and_then(|e| e.as_sequence())
                                        {
                                            // Find entry matching version (if specified)
                                            // Normalize version by stripping "v" prefix for comparison
                                            let normalize_version = |v: &str| -> String {
                                                v.strip_prefix('v').unwrap_or(v).to_string()
                                            };

                                            let target_entry = if let Some(ref version) =
                                                chart_version
                                            {
                                                let normalized_target = normalize_version(version);
                                                chart_entries.iter().find(|entry| {
                                                    entry
                                                        .get("version")
                                                        .and_then(|v| v.as_str())
                                                        .map(|v| {
                                                            normalize_version(v)
                                                                == normalized_target
                                                        })
                                                        .unwrap_or(false)
                                                })
                                            } else {
                                                // Use latest version (first entry, as they're typically sorted)
                                                chart_entries.first()
                                            };

                                            if let Some(entry) = target_entry {
                                                if let Some(urls) =
                                                    entry.get("urls").and_then(|u| u.as_sequence())
                                                {
                                                    if let Some(url) =
                                                        urls.first().and_then(|u| u.as_str())
                                                    {
                                                        // URL might be absolute or relative
                                                        if url.starts_with("http://")
                                                            || url.starts_with("https://")
                                                        {
                                                            url.to_string()
                                                        } else {
                                                            // Relative URL - prepend repo URL
                                                            if repo_url.ends_with('/') {
                                                                format!("{}{}", repo_url, url)
                                                            } else {
                                                                format!("{}/{}", repo_url, url)
                                                            }
                                                        }
                                                    } else {
                                                        return Err(DockerProxyError::Config(
                                                            format!(
                                                            "Chart {}/{} has no URLs in index.yaml",
                                                            repo_name, chart_specifier
                                                        ),
                                                        ));
                                                    }
                                                } else {
                                                    return Err(DockerProxyError::Config(format!(
                                                        "Chart {}/{} has no URLs field in index.yaml",
                                                        repo_name, chart_specifier
                                                    )));
                                                }
                                            } else {
                                                return Err(DockerProxyError::Config(format!(
                                                    "Chart {}/{} version not found in index.yaml",
                                                    repo_name, chart_specifier
                                                )));
                                            }
                                        } else {
                                            return Err(DockerProxyError::Config(format!(
                                                "Chart {} not found in repository {} index.yaml",
                                                chart_name, repo_name
                                            )));
                                        }
                                    } else {
                                        return Err(DockerProxyError::Config(format!(
                                            "Repository {} index.yaml has no 'entries' field",
                                            repo_name
                                        )));
                                    }
                                }
                                Err(e) => {
                                    return Err(DockerProxyError::Config(format!(
                                        "Failed to parse index.yaml for {}: {}",
                                        repo_name, e
                                    )));
                                }
                            }
                        }
                        Err(e) => {
                            return Err(DockerProxyError::Config(format!(
                                "Failed to read index.yaml response for {}: {}",
                                repo_name, e
                            )));
                        }
                    }
                }
                Ok(response) => {
                    return Err(DockerProxyError::Config(format!(
                        "Failed to fetch index.yaml for {}: HTTP {}",
                        repo_name,
                        response.status()
                    )));
                }
                Err(e) => {
                    return Err(DockerProxyError::Config(format!(
                        "Failed to fetch index.yaml for {}: {}",
                        repo_name, e
                    )));
                }
            };

            // Extract filename from chart URL for caching
            let chart_filename_from_url = if let Some(last_slash) = chart_url.rfind('/') {
                &chart_url[last_slash + 1..]
            } else {
                &chart_filename // Fallback to parsed filename
            };

            // Check cache again with the correct filename
            if cache_clone
                .chart_exists(repo_name, chart_filename_from_url)
                .await
            {
                debug!(
                    "[prepull] Chart already cached: {}/{}",
                    repo_name, chart_filename_from_url
                );
                return Ok(());
            }

            // Fetch chart with retries using the URL from index.yaml
            let mut last_error = None;
            for attempt in 1..=3 {
                match client.get(&chart_url).send().await {
                    Ok(response) => {
                        if response.status().is_success() {
                            match response.bytes().await {
                                Ok(bytes) => {
                                    let chart_data = bytes.to_vec();

                                    // Cache the chart using the filename from URL
                                    match cache_clone
                                        .write_chart(
                                            repo_name,
                                            chart_filename_from_url,
                                            &chart_data,
                                        )
                                        .await
                                    {
                                        Ok(_) => {
                                            info!(
                                                "[prepull] Completed chart: {}/{} ({} bytes)",
                                                repo_name,
                                                chart_filename_from_url,
                                                chart_data.len()
                                            );
                                            return Ok(());
                                        }
                                        Err(e) => {
                                            warn!(
                                                "[prepull] Failed to cache chart {}/{}: {}",
                                                repo_name, chart_filename_from_url, e
                                            );
                                            // Continue - chart was downloaded, just not cached
                                            return Ok(());
                                        }
                                    }
                                }
                                Err(e) => {
                                    last_error =
                                        Some(format!("Failed to read chart response: {}", e));
                                    if attempt < 3 {
                                        debug!(
                                            "[prepull] Retry {}/3 for {}/{}: {}",
                                            attempt + 1,
                                            repo_name,
                                            chart_filename_from_url,
                                            last_error.as_ref().unwrap()
                                        );
                                        tokio::time::sleep(Duration::from_secs(attempt as u64 * 2))
                                            .await;
                                        continue;
                                    }
                                }
                            }
                        } else {
                            last_error =
                                Some(format!("Upstream returned HTTP {}", response.status()));
                            if attempt < 3 && response.status().as_u16() >= 500 {
                                // Retry on server errors
                                debug!(
                                    "[prepull] Retry {}/3 for {}/{}: {}",
                                    attempt + 1,
                                    repo_name,
                                    chart_filename_from_url,
                                    last_error.as_ref().unwrap()
                                );
                                tokio::time::sleep(Duration::from_secs(attempt as u64 * 2)).await;
                                continue;
                            } else {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        last_error = Some(format!("Failed to fetch chart: {}", e));
                        if attempt < 3 {
                            debug!(
                                "[prepull] Retry {}/3 for {}/{}: {}",
                                attempt + 1,
                                repo_name,
                                chart_filename_from_url,
                                last_error.as_ref().unwrap()
                            );
                            tokio::time::sleep(Duration::from_secs(attempt as u64 * 2)).await;
                            continue;
                        }
                    }
                }
            }

            // All retries failed
            error!(
                "[prepull] Failed to pull chart {}/{} after 3 attempts: {}",
                repo_name,
                chart_filename_from_url,
                last_error.as_ref().unwrap_or(&"Unknown error".to_string())
            );
            Err(DockerProxyError::Registry(format!(
                "Failed to pull chart {}/{}: {}",
                repo_name,
                chart_filename_from_url,
                last_error.as_ref().unwrap_or(&"Unknown error".to_string())
            )))
        });

        handles.push(handle);
    }

    // Wait for all chart pulls to complete
    let results = future::join_all(handles).await;
    for result in results {
        match result {
            Ok(Ok(_)) => success_count += 1,
            Ok(Err(e)) => {
                warn!("[prepull] Chart pull failed: {}", e);
            }
            Err(e) => {
                error!("[prepull] Chart pull task panicked: {}", e);
            }
        }
    }

    Ok(success_count)
}
