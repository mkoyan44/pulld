use crate::cache::CacheStorage;
use crate::config::{
    Config, DEFAULT_INITIAL_RTT_MS, DEFAULT_MIRROR_SCORE, DEFAULT_REGISTRY_NAME,
    DEFAULT_REGISTRY_URL,
};
use crate::error::{DockerProxyError, Result};
use crate::helm::{get_chart, get_index};
use crate::registry::{get_blob, get_manifest, head_blob, head_manifest, UpstreamClient};
use crate::tls::{create_server_tls_config, create_server_tls_config_from_pem};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode, Uri},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::error;

// Wrapper functions to parse multi-segment paths for manifests and blobs
// Axum's :name only matches single segments, so we need to parse manually
async fn get_v2_wrapper(
    State(state): State<crate::registry::manifest::AppState>,
    uri: Uri,
    headers: HeaderMap,
) -> axum::response::Response {
    const V2_PREFIX: &str = "/v2/";
    const MANIFESTS_SUFFIX: &str = "/manifests/";
    const BLOBS_SUFFIX: &str = "/blobs/";

    let mut path = uri.path().to_string();

    // Fix malformed paths where /v2/ appears twice (e.g., /v2/v2/quay.io/...)
    // This can happen if containerd incorrectly adds /v2/ to the image reference
    // Example: containerd receives pulld.internal:5050/v2/quay.io/cilium/cilium:v1.17.7
    // and makes request to /v2/v2/quay.io/cilium/cilium/manifests/v1.17.7
    if path.starts_with("/v2/v2/") {
        tracing::warn!(
            original_path = %path,
            "Detected malformed path with double /v2/ prefix - stripping extra prefix"
        );
        path = path.replacen("/v2/v2/", "/v2/", 1);
        tracing::debug!(
            corrected_path = %path,
            "Corrected path after stripping double /v2/ prefix"
        );
    }

    tracing::info!(method = "GET", path = %path, "Registry request received");

    tracing::debug!(
        path = %path,
        "Received v2 request path"
    );

    // Handle manifests: /v2/library/alpine/manifests/latest
    if let Some(manifests_idx) = path.rfind(MANIFESTS_SUFFIX) {
        let name = path[V2_PREFIX.len()..manifests_idx].to_string();
        let reference = path[manifests_idx + MANIFESTS_SUFFIX.len()..].to_string();

        tracing::info!(
            original_path = %path,
            parsed_name = %name,
            parsed_reference = %reference,
            "Registry manifest request parsed"
        );

        tracing::debug!(
            name = %name,
            reference = %reference,
            "Parsed manifest request from path - will be passed to get_manifest"
        );

        // Handle manifest request - get_manifest can handle both tags and digests
        get_manifest(State(state), Path((name, reference)), headers)
            .await
            .into_response()
    }
    // Handle blobs: /v2/library/alpine/blobs/sha256:...
    else if let Some(blobs_idx) = path.rfind(BLOBS_SUFFIX) {
        let name = path[V2_PREFIX.len()..blobs_idx].to_string();
        let digest = path[blobs_idx + BLOBS_SUFFIX.len()..].to_string();

        tracing::info!(
            method = "GET",
            name = %name,
            digest = %digest,
            "Registry blob request parsed"
        );

        tracing::debug!(
            name = %name,
            digest = %digest,
            "Parsed blob request from path"
        );
        get_blob(State(state), Path((name, digest)), headers)
            .await
            .into_response()
    } else {
        (StatusCode::BAD_REQUEST, "Invalid v2 path").into_response()
    }
}

async fn head_v2_wrapper(
    State(state): State<crate::registry::manifest::AppState>,
    uri: Uri,
) -> axum::response::Response {
    const V2_PREFIX: &str = "/v2/";
    const MANIFESTS_SUFFIX: &str = "/manifests/";
    const BLOBS_SUFFIX: &str = "/blobs/";

    let mut path = uri.path().to_string();

    // Fix malformed paths where /v2/ appears twice (same fix as GET handler)
    if path.starts_with("/v2/v2/") {
        tracing::warn!(
            original_path = %path,
            "Detected malformed HEAD path with double /v2/ prefix - stripping extra prefix"
        );
        path = path.replacen("/v2/v2/", "/v2/", 1);
    }

    if let Some(manifests_idx) = path.rfind(MANIFESTS_SUFFIX) {
        let name = path[V2_PREFIX.len()..manifests_idx].to_string();
        let reference = path[manifests_idx + MANIFESTS_SUFFIX.len()..].to_string();

        tracing::info!(
            method = "HEAD",
            name = %name,
            reference = %reference,
            "Registry manifest request parsed"
        );

        // Handle HEAD manifest request - head_manifest can handle both tags and digests
        head_manifest(State(state), Path((name, reference)))
            .await
            .into_response()
    } else if let Some(blobs_idx) = path.rfind(BLOBS_SUFFIX) {
        let name = path[V2_PREFIX.len()..blobs_idx].to_string();
        let digest = path[blobs_idx + BLOBS_SUFFIX.len()..].to_string();
        tracing::info!(
            method = "HEAD",
            name = %name,
            digest = %digest,
            "Registry blob request parsed"
        );
        head_blob(State(state), Path((name, digest)))
            .await
            .into_response()
    } else {
        (StatusCode::BAD_REQUEST, "Invalid v2 path").into_response()
    }
}

fn build_router(app_state: crate::registry::manifest::AppState) -> Router {
    Router::new()
        .route("/v2/", get(api_version))
        .route("/v2/*path", get(get_v2_wrapper).head(head_v2_wrapper))
        .route("/helm/:repo/index.yaml", get(get_index))
        .route("/helm/:repo/charts/:chart", get(get_chart))
        .route("/mutate/pods", post(mutate_pods))
        .route("/api/v1/pre-pull", post(pre_pull))
        .route("/api/v1/cache/stats", get(cache_stats))
        .route("/api/v1/mirror/stats", get(mirror_stats))
        .route("/health", get(health))
        .with_state(app_state)
}

pub async fn start_server(
    cache_dir: PathBuf,
    config: Config,
    server_cert_pem: Option<String>,
    server_key_pem: Option<String>,
) -> Result<tokio::task::JoinHandle<()>> {
    tracing::debug!(
        "[pulld] Initializing cache storage at: {}",
        cache_dir.display()
    );
    let cache_storage = Arc::new(CacheStorage::with_max_size(
        cache_dir.clone(),
        Some(config.cache.max_size_gb),
    )?);
    tracing::debug!(
        "[pulld] Cache storage initialized: max_size={}GB, directory={}",
        config.cache.max_size_gb,
        cache_dir.display()
    );

    // Create upstream clients for each configured registry
    let upstream_tls = config
        .upstream
        .tls
        .as_ref()
        .unwrap_or(&crate::config::UpstreamTlsConfig {
            ca_bundle_path: None,
            use_system_ca: true,
            insecure_skip_verify: false,
        });

    let mut registry_clients: std::collections::HashMap<String, Arc<UpstreamClient>> =
        std::collections::HashMap::new();

    // Create clients for each configured registry
    for (registry_name, registry_config) in &config.upstream.registries {
        let client = Arc::new(UpstreamClient::new(
            registry_config.mirrors.clone(),
            upstream_tls,
            Some(registry_config),
        )?);
        registry_clients.insert(registry_name.clone(), client);
        tracing::debug!(
            "Configured registry client: {} with {} mirror(s)",
            registry_name,
            registry_config.mirrors.len()
        );
    }

    tracing::debug!(
        "Pulld initialized with {} registry client(s): {:?}",
        registry_clients.len(),
        registry_clients.keys().collect::<Vec<_>>()
    );

    // Default client for docker.io if not configured
    let default_mirrors = vec![DEFAULT_REGISTRY_URL.to_string()];
    let default_client = Arc::new(UpstreamClient::new(default_mirrors, upstream_tls, None)?);

    // Ensure docker.io is in the registry clients map (use configured or default)
    if !registry_clients.contains_key(DEFAULT_REGISTRY_NAME) {
        registry_clients.insert(DEFAULT_REGISTRY_NAME.to_string(), default_client.clone());
    }

    // Build AppState (shared between servers)
    use crate::config::MirrorStrategy;
    use crate::registry::manifest::AppState;
    use crate::registry::mirror_racer::MirrorSelector;

    let mirror_selector = Arc::new(MirrorSelector::new(MirrorStrategy::Adaptive));

    // Determine if we should start both HTTP and HTTPS servers
    let should_start_https = config
        .server
        .tls
        .as_ref()
        .map(|tls| {
            let has_cert_pem = server_cert_pem.is_some();
            let has_cert_path = !tls.cert_path.is_empty();
            let should_start = tls.enabled && (has_cert_pem || has_cert_path);

            tracing::debug!(
                "HTTPS startup check: tls.enabled={}, has_cert_pem={}, has_cert_path={}, should_start_https={}",
                tls.enabled, has_cert_pem, has_cert_path, should_start
            );

            should_start
        })
        .unwrap_or_else(|| {
            tracing::warn!("TLS config is None - HTTPS will NOT start");
            false
        });

    let should_start_http = if should_start_https {
        // If HTTPS is enabled, only start HTTP if http_port is explicitly set
        let http_port_set = config.server.http_port.is_some();
        tracing::debug!(
            "HTTP startup check: HTTPS enabled, http_port set={}, should_start_http={}",
            http_port_set,
            http_port_set
        );
        http_port_set
    } else {
        // If HTTPS is not enabled, use the main port for HTTP
        tracing::debug!(
            "HTTP startup check: HTTPS disabled, should_start_http=true (using main port)"
        );
        true
    };

    // Determine proxy scheme and port for URL rewriting in Helm index.yaml
    // When HTTPS is enabled: port 5050 = HTTPS, port 5051 = HTTP
    // When HTTPS is NOT enabled: port 5050 = HTTP
    // We use the same scheme as what's running on the main port (5050)
    let proxy_scheme = if should_start_https {
        "https".to_string() // Port 5050 runs HTTPS
    } else {
        "http".to_string() // Port 5050 runs HTTP
    };
    let proxy_port = config.server.port; // Always use main port (5050)

    let app_state = AppState {
        cache: cache_storage,
        registry_clients: Arc::new(std::sync::RwLock::new(registry_clients)),
        registry_configs: Arc::new(config.upstream.registries.clone()),
        default_registry_client: default_client,
        token_cache: Arc::new(tokio::sync::RwLock::new(
            crate::registry::manifest::TokenCache::default(),
        )),
        helm_repos: Arc::new(config.helm.repositories.clone()),
        pre_pull_config: Some(config.pre_pull.clone()),
        upstream_tls: Arc::new(upstream_tls.clone()),
        mirror_selector: mirror_selector.clone(),
        proxy_host: "pulld.internal".to_string(),
        proxy_port,
        proxy_scheme,
    };

    let bind_address = config.server.bind_address.clone();
    let app_state_clone = app_state.clone();

    // Start HTTPS server if configured
    let https_handle = if should_start_https {
        let addr = format!("{}:{}", bind_address, config.server.port);
        let config_clone = config.clone();
        let server_cert_pem_clone = server_cert_pem.clone();
        let server_key_pem_clone = server_key_pem.clone();
        let app_state_https = app_state.clone();
        tracing::debug!("Attempting to start HTTPS server on {} (TLS enabled)", addr);
        Some(tokio::spawn(async move {
            let addr_for_bind = addr.clone();
            // For TLS, we need std::net::TcpListener for axum_server::from_tcp_rustls
            let std_listener = match tokio::task::spawn_blocking(move || {
                std::net::TcpListener::bind(&addr_for_bind).map_err(|e| {
                    DockerProxyError::Config(format!("Failed to bind to {}: {}", addr_for_bind, e))
                })
            })
            .await
            {
                Ok(Ok(listener)) => {
                    tracing::debug!("Successfully bound HTTPS listener to {}", addr);
                    listener
                }
                Ok(Err(e)) => {
                    error!("Failed to bind HTTPS listener to {}: {}", addr, e);
                    error!("HTTPS server will NOT start - port may be in use or insufficient permissions");
                    return;
                }
                Err(e) => {
                    error!("Failed to create HTTPS listener task: {}", e);
                    return;
                }
            };

            // Use PEM strings from vault if provided, otherwise fall back to file paths
            let tls_config = config_clone.server.tls.as_ref().unwrap();
            let tls = if let (Some(cert_pem), Some(key_pem)) =
                (server_cert_pem_clone, server_key_pem_clone)
            {
                tracing::debug!("Creating TLS config from PEM strings (vault certificates)");
                match create_server_tls_config_from_pem(&cert_pem, &key_pem).await {
                    Ok(tls) => {
                        tracing::debug!("TLS config created successfully from PEM");
                        tls
                    }
                    Err(e) => {
                        error!("Failed to create TLS config from PEM: {}", e);
                        error!(
                            "HTTPS server will NOT start - check certificate format and validity"
                        );
                        return;
                    }
                }
            } else {
                tracing::debug!("Creating TLS config from file paths");
                if tls_config.cert_path.is_empty() || tls_config.key_path.is_empty() {
                    error!(
                        "TLS enabled but no certificates provided (PEM or file paths are empty)"
                    );
                    error!("HTTPS server will NOT start");
                    return;
                }
                match create_server_tls_config(tls_config).await {
                    Ok(tls) => {
                        tracing::debug!("TLS config created successfully from files");
                        tls
                    }
                    Err(e) => {
                        error!("Failed to create TLS config from files: {}", e);
                        error!("HTTPS server will NOT start - check certificate files");
                        return;
                    }
                }
            };

            let app = build_router(app_state_https);
            tracing::debug!("Starting HTTPS server on {} (TLS enabled)", addr);
            tracing::info!("HTTPS server is now listening and ready to accept connections");
            if let Err(e) = axum_server::from_tcp_rustls(std_listener, tls)
                .serve(app.into_make_service())
                .await
            {
                error!("HTTPS server error after startup: {}", e);
            } else {
                tracing::info!("HTTPS server stopped");
            }
        }))
    } else {
        None
    };

    // Start HTTP server if configured
    let http_handle = if should_start_http {
        let http_port = config.server.http_port.unwrap_or(config.server.port);
        let addr = format!("{}:{}", bind_address, http_port);
        let app_state_http = app_state_clone;
        tracing::debug!("Attempting to start HTTP server on {} (plain HTTP)", addr);
        Some(tokio::spawn(async move {
            let listener = match tokio::net::TcpListener::bind(&addr).await {
                Ok(listener) => {
                    tracing::debug!("Successfully bound HTTP listener to {}", addr);
                    listener
                }
                Err(e) => {
                    error!("Failed to bind HTTP listener to {}: {}", addr, e);
                    error!("HTTP server will NOT start - port may be in use or insufficient permissions");
                    return;
                }
            };

            let app = build_router(app_state_http);
            tracing::debug!("Starting HTTP server on {} (plain HTTP)", addr);
            tracing::info!("HTTP server is now listening and ready to accept connections");
            if let Err(e) = axum::serve(listener, app).await {
                error!("HTTP server error after startup: {}", e);
            } else {
                tracing::info!("HTTP server stopped");
            }
        }))
    } else {
        None
    };

    // Return a handle that waits for both servers
    let handle = tokio::spawn(async move {
        match (https_handle, http_handle) {
            (Some(https), Some(http)) => {
                // Wait for both servers (first one to finish/error wins)
                tokio::select! {
                    result = https => {
                        if let Err(e) = result {
                            error!("HTTPS server task failed: {:?}", e);
                        }
                    }
                    result = http => {
                        if let Err(e) = result {
                            error!("HTTP server task failed: {:?}", e);
                        }
                    }
                }
            }
            (Some(https), None) => {
                if let Err(e) = https.await {
                    error!("HTTPS server task failed: {:?}", e);
                }
            }
            (None, Some(http)) => {
                if let Err(e) = http.await {
                    error!("HTTP server task failed: {:?}", e);
                }
            }
            (None, None) => {
                error!("No servers configured (neither HTTP nor HTTPS)");
            }
        }
    });

    Ok(handle)
}

async fn api_version() -> impl IntoResponse {
    tracing::info!(
        method = "GET",
        path = "/v2/",
        "Registry API version request"
    );
    (StatusCode::OK, "{}")
}

async fn mutate_pods(Json(review): Json<crate::admission::AdmissionReview>) -> impl IntoResponse {
    let endpoint = std::env::var("PULLD_ADMISSION_REGISTRY_ENDPOINT")
        .unwrap_or_else(|_| "pulld.4lock.net".to_string());
    Json(crate::admission::mutate_pod_review(review, &endpoint))
}

async fn health() -> impl IntoResponse {
    tracing::debug!("GET /health - Health check request");
    (StatusCode::OK, "ok")
}

#[derive(Deserialize)]
struct PrePullRequest {
    images: Vec<String>,
}

#[derive(Serialize)]
struct PrePullResponse {
    status: String,
    results: Vec<PrePullResult>,
}

#[derive(Serialize)]
struct PrePullResult {
    image: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<u64>,
}

/// POST /api/v1/pre-pull
async fn pre_pull(
    State(state): State<crate::registry::manifest::AppState>,
    Json(req): Json<PrePullRequest>,
) -> impl IntoResponse {
    use crate::prepull::pull_image;
    use std::sync::Arc;

    let cache = state.cache.clone();
    // Use default registry client (docker.io) for pre-pull
    let upstream = state.default_registry_client.clone();

    // Get pre-pull config or use defaults
    let pre_pull_config = state.pre_pull_config.as_ref();
    let image_concurrency = pre_pull_config.map(|c| c.image_concurrency).unwrap_or(8);
    let layer_concurrency = pre_pull_config.map(|c| c.layer_concurrency).unwrap_or(6);
    let strategy = pre_pull_config
        .map(|c| c.mirror_strategy)
        .unwrap_or_default();
    let hedge_delay_ms = pre_pull_config
        .and_then(|_c| {
            state
                .registry_configs
                .get(DEFAULT_REGISTRY_NAME)
                .map(|r| r.hedge_delay_ms)
        })
        .unwrap_or(100);

    // Pull images in parallel using config concurrency
    let semaphore = Arc::new(tokio::sync::Semaphore::new(image_concurrency));
    let mut handles = Vec::new();

    for image in &req.images {
        let image = image.clone();
        let cache_clone = cache.clone();
        let upstream_clone = upstream.clone();
        let semaphore_clone = semaphore.clone();
        let strategy_clone = strategy;
        let hedge_delay_ms_clone = hedge_delay_ms;

        let handle = tokio::spawn(async move {
            let _permit = semaphore_clone.acquire().await.unwrap();
            match pull_image(
                &image,
                cache_clone,
                upstream_clone,
                layer_concurrency,
                strategy_clone,
                hedge_delay_ms_clone,
            )
            .await
            {
                Ok(size) => PrePullResult {
                    image,
                    status: "success".to_string(),
                    size: Some(size),
                },
                Err(e) => PrePullResult {
                    image,
                    status: format!("error: {}", e),
                    size: None,
                },
            }
        });
        handles.push(handle);
    }

    // Wait for all pulls to complete
    let mut results = Vec::new();
    for handle in handles {
        if let Ok(result) = handle.await {
            results.push(result);
        } else {
            // Task panicked
            results.push(PrePullResult {
                image: "unknown".to_string(),
                status: "error: task panicked".to_string(),
                size: None,
            });
        }
    }

    Json(PrePullResponse {
        status: "ok".to_string(),
        results,
    })
}

#[derive(Serialize)]
struct CacheStats {
    total_blobs: usize,
    total_size_bytes: u64,
}

/// GET /api/v1/cache/stats
async fn cache_stats(
    State(state): State<crate::registry::manifest::AppState>,
) -> impl IntoResponse {
    let cache = &state.cache;
    let blobs_dir = cache.base_dir().join("blobs").join("sha256");

    // Count blobs and calculate total size
    let mut total_blobs = 0usize;
    let mut total_size_bytes = 0u64;

    if let Ok(entries) = std::fs::read_dir(&blobs_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                total_blobs += 1;
                if let Ok(metadata) = std::fs::metadata(&path) {
                    total_size_bytes += metadata.len();
                }
            }
        }
    }

    Json(CacheStats {
        total_blobs,
        total_size_bytes,
    })
}

#[derive(Serialize)]
struct MirrorStatsResponse {
    mirrors: Vec<MirrorStat>,
}

#[derive(Serialize)]
struct MirrorStat {
    mirror_url: String,
    rtt_ms: f64,
    throughput_ewma: f64,
    success_count: u32,
    error_count: u32,
    success_rate: f64,
    score: f64,
}

/// GET /api/v1/mirror/stats
async fn mirror_stats(
    State(state): State<crate::registry::manifest::AppState>,
) -> impl IntoResponse {
    let selector = &state.mirror_selector;
    let mut mirrors = Vec::new();

    // Get all configured mirrors from registry configs
    let mut all_mirrors = std::collections::HashSet::new();
    for registry_config in state.registry_configs.values() {
        for mirror in &registry_config.mirrors {
            all_mirrors.insert(mirror.clone());
        }
    }

    // Also include default registry mirrors
    for mirror in state.default_registry_client.mirrors() {
        all_mirrors.insert(mirror.clone());
    }

    // Get stats for each mirror
    for mirror_url in all_mirrors {
        if let Some(stats) = selector.get_stats(&mirror_url).await {
            mirrors.push(MirrorStat {
                mirror_url,
                rtt_ms: stats.rtt_ms(),
                throughput_ewma: stats.throughput_ewma,
                success_count: stats.success_count,
                error_count: stats.error_count,
                success_rate: stats.success_rate(),
                score: stats.score(),
            });
        } else {
            // Mirror with no stats yet - return default values
            mirrors.push(MirrorStat {
                mirror_url,
                rtt_ms: DEFAULT_INITIAL_RTT_MS,
                throughput_ewma: 0.0,
                success_count: 0,
                error_count: 0,
                success_rate: 0.5, // Neutral if no data
                score: DEFAULT_MIRROR_SCORE,
            });
        }
    }

    Json(MirrorStatsResponse { mirrors })
}
