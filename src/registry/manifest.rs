use crate::cache::CacheStorage;
use crate::config::{
    RegistryConfig, DEFAULT_MANIFEST_ACCEPT_HEADER, DEFAULT_REGISTRY_NAME, DEFAULT_REGISTRY_URL,
    DEFAULT_TOKEN_EXPIRY_SECS, TOKEN_EXPIRY_SAFETY_MARGIN_SECS,
};
use crate::registry::mirror_racer::MirrorSelector;
use crate::registry::upstream::UpstreamClient;
use axum::extract::FromRef;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use reqwest::Client;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::RwLock as TokioRwLock;

const DEFAULT_PULL_EVENT_CAPACITY: usize = 512;

#[derive(Clone, Debug, Serialize)]
pub struct PullEvent {
    pub sequence: u64,
    pub timestamp: String,
    pub event: String,
    pub image: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
    pub message: String,
}

impl PullEvent {
    pub fn new(
        event: impl Into<String>,
        image: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            sequence: 0,
            timestamp: String::new(),
            event: event.into(),
            image: image.into(),
            method: None,
            status: None,
            cache: None,
            digest: None,
            elapsed_ms: None,
            message: message.into(),
        }
    }

    pub fn method(mut self, method: &str) -> Self {
        self.method = Some(method.to_string());
        self
    }

    pub fn status(mut self, status: impl Into<String>) -> Self {
        self.status = Some(status.into());
        self
    }

    pub fn cache(mut self, cache: impl Into<String>) -> Self {
        self.cache = Some(cache.into());
        self
    }

    pub fn digest(mut self, digest: impl Into<String>) -> Self {
        self.digest = Some(digest.into());
        self
    }

    pub fn elapsed_ms(mut self, elapsed_ms: u64) -> Self {
        self.elapsed_ms = Some(elapsed_ms);
        self
    }
}

#[derive(Debug)]
pub struct PullEventLog {
    capacity: usize,
    next_sequence: AtomicU64,
    events: TokioRwLock<VecDeque<PullEvent>>,
}

impl PullEventLog {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            next_sequence: AtomicU64::new(1),
            events: TokioRwLock::new(VecDeque::with_capacity(capacity.max(1))),
        }
    }

    pub async fn record(&self, mut event: PullEvent) {
        event.sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        event.timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

        let mut events = self.events.write().await;
        if events.len() >= self.capacity {
            events.pop_front();
        }
        events.push_back(event);
    }

    pub async fn after(&self, sequence: u64) -> Vec<PullEvent> {
        self.events
            .read()
            .await
            .iter()
            .filter(|event| event.sequence > sequence)
            .cloned()
            .collect()
    }

    pub async fn len(&self) -> usize {
        self.events.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.events.read().await.is_empty()
    }

    pub fn latest_sequence(&self) -> u64 {
        self.next_sequence.load(Ordering::Relaxed).saturating_sub(1)
    }
}

impl Default for PullEventLog {
    fn default() -> Self {
        Self::new(DEFAULT_PULL_EVENT_CAPACITY)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct PullMetricKey {
    event: String,
    method: String,
    status: String,
    cache: String,
}

#[derive(Clone, Debug, Default)]
pub struct PullMetrics {
    events_total: Arc<Mutex<BTreeMap<PullMetricKey, u64>>>,
}

impl PullMetrics {
    pub fn record(&self, event: &PullEvent) {
        let key = PullMetricKey {
            event: event.event.clone(),
            method: event.method.clone().unwrap_or_else(|| "-".to_string()),
            status: event
                .status
                .as_deref()
                .map(|status| status.replace(' ', "_"))
                .unwrap_or_else(|| "-".to_string()),
            cache: event.cache.clone().unwrap_or_else(|| "-".to_string()),
        };

        let mut events_total = self
            .events_total
            .lock()
            .expect("pull metrics lock poisoned");
        *events_total.entry(key).or_insert(0) += 1;
    }

    pub async fn render_prometheus(&self, event_log: &PullEventLog) -> String {
        use std::fmt::Write;

        let events_total = self
            .events_total
            .lock()
            .expect("pull metrics lock poisoned")
            .clone();
        let mut output = String::new();

        output.push_str("# HELP pulld_pull_events_total Pull events recorded by pulld.\n");
        output.push_str("# TYPE pulld_pull_events_total counter\n");
        for (key, total) in events_total {
            let _ = writeln!(
                output,
                "pulld_pull_events_total{{event=\"{}\",method=\"{}\",status=\"{}\",cache=\"{}\"}} {}",
                prometheus_label_value(&key.event),
                prometheus_label_value(&key.method),
                prometheus_label_value(&key.status),
                prometheus_label_value(&key.cache),
                total
            );
        }

        output.push_str("# HELP pulld_pull_event_buffer_size Pull events retained in the in-memory event buffer.\n");
        output.push_str("# TYPE pulld_pull_event_buffer_size gauge\n");
        let _ = writeln!(
            output,
            "pulld_pull_event_buffer_size {}",
            event_log.len().await
        );

        output.push_str("# HELP pulld_pull_event_latest_sequence Latest pull event sequence number recorded by pulld.\n");
        output.push_str("# TYPE pulld_pull_event_latest_sequence gauge\n");
        let _ = writeln!(
            output,
            "pulld_pull_event_latest_sequence {}",
            event_log.latest_sequence()
        );

        output
    }
}

fn prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', " ")
}

#[cfg(test)]
mod pull_event_tests {
    use super::{PullEvent, PullEventLog, PullMetrics};

    fn event(name: &str) -> PullEvent {
        PullEvent {
            sequence: 0,
            timestamp: String::new(),
            event: name.to_string(),
            image: "docker.io/library/alpine:3.20".to_string(),
            method: Some("GET".to_string()),
            status: None,
            cache: None,
            digest: None,
            elapsed_ms: None,
            message: "test event".to_string(),
        }
    }

    #[tokio::test]
    async fn pull_event_log_returns_events_after_sequence() {
        let log = PullEventLog::new(8);
        log.record(event("first")).await;
        log.record(event("second")).await;

        let events = log.after(1).await;

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 2);
        assert_eq!(events[0].event, "second");
    }

    #[tokio::test]
    async fn pull_event_log_keeps_bounded_history() {
        let log = PullEventLog::new(2);
        log.record(event("first")).await;
        log.record(event("second")).await;
        log.record(event("third")).await;

        let events = log.after(0).await;

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event, "second");
        assert_eq!(events[1].event, "third");
    }

    #[tokio::test]
    async fn pull_metrics_render_prometheus_counters() {
        let log = PullEventLog::new(8);
        let metrics = PullMetrics::default();
        let event = PullEvent::new(
            "pulld_image_blob_response_opened",
            "docker.io/library/alpine@sha256:test",
            "blob response opened",
        )
        .method("GET")
        .status("200 OK")
        .cache("MISS");

        metrics.record(&event);
        log.record(event).await;

        let output = metrics.render_prometheus(&log).await;

        assert!(output.contains("# TYPE pulld_pull_events_total counter"));
        assert!(output.contains(
            "pulld_pull_events_total{event=\"pulld_image_blob_response_opened\",method=\"GET\",status=\"200_OK\",cache=\"MISS\"} 1"
        ));
        assert!(output.contains("pulld_pull_event_buffer_size 1"));
        assert!(output.contains("pulld_pull_event_latest_sequence 1"));
    }
}

#[derive(Clone)]
pub struct AppState {
    pub cache: Arc<CacheStorage>,
    pub registry_clients:
        Arc<std::sync::RwLock<std::collections::HashMap<String, Arc<UpstreamClient>>>>,
    pub registry_configs: Arc<std::collections::HashMap<String, RegistryConfig>>,
    pub default_registry_client: Arc<UpstreamClient>,
    pub token_cache: Arc<TokioRwLock<TokenCache>>,
    pub helm_repos: Arc<std::collections::HashMap<String, String>>,
    pub pre_pull_config: Option<crate::config::PrePullConfig>,
    pub upstream_tls: Arc<crate::config::UpstreamTlsConfig>,
    pub mirror_selector: Arc<MirrorSelector>,
    pub proxy_host: String,
    pub proxy_port: u16,
    pub proxy_scheme: String,
    pub pull_events: Arc<PullEventLog>,
    pub pull_metrics: Arc<PullMetrics>,
}

impl AppState {
    pub async fn record_pull_event(&self, event: PullEvent) {
        self.pull_metrics.record(&event);
        self.pull_events.record(event).await;
    }

    /// Get upstream client for a specific registry
    ///
    /// Fallback chain for registry client resolution:
    /// 1. Check if registry is already configured and cached
    /// 2. Auto-detect registry domains (contains a dot, like quay.io, ghcr.io)
    ///    - Creates client on-the-fly with https://{registry} as mirror URL
    ///    - Caches the client for future use
    /// 3. Fallback to default_registry_client (docker.io) if auto-detection fails
    ///
    /// This allows the proxy to work with unconfigured registries by automatically
    /// detecting registry domains and creating appropriate clients.
    pub fn get_upstream_client(&self, registry: &str) -> Arc<UpstreamClient> {
        // Check if already configured
        {
            let clients = self.registry_clients.read().unwrap();
            if let Some(client) = clients.get(registry) {
                return client.clone();
            }
        }

        // Auto-detect registry domains (contains a dot, like quay.io, ghcr.io)
        // Create a client on the fly for unconfigured registries
        if registry.contains('.') && registry != DEFAULT_REGISTRY_NAME {
            let mirror_url = format!("https://{}", registry);
            tracing::info!(
                registry = %registry,
                mirror_url = %mirror_url,
                "Auto-detecting registry domain - creating client with mirror URL"
            );

            let registry_config = self.get_registry_config(registry);

            // Create client on the fly
            match UpstreamClient::new(
                vec![mirror_url.clone()],
                &self.upstream_tls,
                Some(&registry_config),
            ) {
                Ok(client) => {
                    let client = Arc::new(client);
                    // Cache it for future use
                    {
                        let mut clients = self.registry_clients.write().unwrap();
                        clients.insert(registry.to_string(), client.clone());
                    }
                    tracing::info!(
                        registry = %registry,
                        mirror_url = %mirror_url,
                        "Auto-created upstream client for unconfigured registry and cached it"
                    );
                    return client;
                }
                Err(e) => {
                    tracing::warn!(
                        registry = %registry,
                        error = %e,
                        "Failed to create auto-detected registry client, falling back to default"
                    );
                }
            }
        }

        // Fallback to default client
        self.default_registry_client.clone()
    }

    /// Get registry config for a specific registry
    ///
    /// Fallback chain for registry configuration:
    /// 1. Return configured registry config if it exists
    /// 2. Auto-detect registry domains (contains a dot, like quay.io, ghcr.io)
    ///    - Returns config with https://{registry} as the mirror URL
    ///    - Uses default strategy, timeout, and other settings
    /// 3. Fallback to default docker.io config (DEFAULT_REGISTRY_URL)
    ///
    /// This ensures every registry has a valid config, even if not explicitly configured.
    pub fn get_registry_config(&self, registry: &str) -> RegistryConfig {
        if let Some(config) = self.registry_configs.get(registry) {
            return config.clone();
        }

        // Auto-detect registry domains (contains a dot, like quay.io, ghcr.io)
        // Return config with the registry's URL as the mirror
        if registry.contains('.') && registry != DEFAULT_REGISTRY_NAME {
            let mirror_url = format!("https://{}", registry);
            tracing::debug!(
                registry = %registry,
                mirror = %mirror_url,
                "Auto-detecting registry domain"
            );
            return RegistryConfig {
                mirrors: vec![mirror_url],
                strategy: crate::config::MirrorStrategy::default(),
                max_parallel: 4,
                chunk_size: 16_777_216, // 16MB
                hedge_delay_ms: 100,
                timeout_secs: 30,
                auth: None,
                ca_cert_path: None,
                insecure: false,
            };
        }

        // Default config for unconfigured registries (fallback to docker.io)
        RegistryConfig {
            mirrors: vec![DEFAULT_REGISTRY_URL.to_string()],
            strategy: crate::config::MirrorStrategy::default(),
            max_parallel: 4,
            chunk_size: 16_777_216, // 16MB
            hedge_delay_ms: 100,
            timeout_secs: 30,
            auth: None,
            ca_cert_path: None,
            insecure: false,
        }
    }
}

#[derive(Default)]
pub struct TokenCache {
    tokens: HashMap<String, CachedToken>,
}

struct CachedToken {
    token: String,
    expires_at: Instant,
}

impl TokenCache {
    pub fn new() -> Self {
        Self {
            tokens: HashMap::new(),
        }
    }

    fn get(&self, key: &str) -> Option<String> {
        self.tokens.get(key).and_then(|cached| {
            if cached.expires_at > Instant::now() {
                Some(cached.token.clone())
            } else {
                None
            }
        })
    }

    fn insert(&mut self, key: String, token: String, expires_in_seconds: Option<u64>) {
        // Default to configured value if expiry not provided, subtract safety margin
        let expires_in = expires_in_seconds.unwrap_or(DEFAULT_TOKEN_EXPIRY_SECS);
        let expires_at = Instant::now()
            + Duration::from_secs(expires_in.saturating_sub(TOKEN_EXPIRY_SAFETY_MARGIN_SECS));

        self.tokens.insert(key, CachedToken { token, expires_at });

        // Clean up expired tokens periodically (simple cleanup - remove expired entries)
        self.tokens
            .retain(|_, cached| cached.expires_at > Instant::now());
    }
}

impl FromRef<AppState> for Arc<CacheStorage> {
    fn from_ref(state: &AppState) -> Self {
        state.cache.clone()
    }
}

/// Fetch authentication token for anonymous pulls from any Docker registry
/// Follows OCI Distribution API authentication flow:
/// 1. Parse WWW-Authenticate header to extract realm, service, and scope
/// 2. Check token cache first
/// 3. Request token from the realm URL if not cached
/// 4. Cache token with expiry for future requests
/// 5. Return token for use in Authorization header
pub async fn fetch_registry_token(www_auth: &str, repository: &str) -> Option<String> {
    fetch_registry_token_with_cache(www_auth, repository, None).await
}

/// Fetch token with optional cache
pub async fn fetch_registry_token_with_cache(
    www_auth: &str,
    repository: &str,
    token_cache: Option<Arc<TokioRwLock<TokenCache>>>,
) -> Option<String> {
    // Parse WWW-Authenticate header: Bearer realm="...",service="...",scope="..."
    // Example: Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/nginx:pull"
    let mut realm = None;
    let mut service = None;
    let mut scope = None;

    // Strip "Bearer " prefix if present
    let auth_str = www_auth.strip_prefix("Bearer ").unwrap_or(www_auth);

    for part in auth_str.split(',') {
        let part = part.trim();
        if part.starts_with("realm=") {
            realm = part
                .strip_prefix("realm=")
                .and_then(|s| s.strip_prefix('"'))
                .and_then(|s| s.strip_suffix('"'))
                .map(|s| s.to_string());
        } else if part.starts_with("service=") {
            service = part
                .strip_prefix("service=")
                .and_then(|s| s.strip_prefix('"'))
                .and_then(|s| s.strip_suffix('"'))
                .map(|s| s.to_string());
        } else if part.starts_with("scope=") {
            scope = part
                .strip_prefix("scope=")
                .and_then(|s| s.strip_prefix('"'))
                .and_then(|s| s.strip_suffix('"'))
                .map(|s| s.to_string());
        }
    }

    // Build token URL following OCI Distribution API spec
    let token_url = if let Some(realm_url) = realm {
        let mut url = realm_url.clone();

        // Add service parameter if present in header
        if let Some(service_val) = &service {
            url.push_str(&format!("?service={}", service_val));
        } else {
            // If no service in header, start query string
            url.push('?');
        }

        // Add scope parameter
        // If scope is in header, use it; otherwise construct from repository
        if let Some(scope_val) = &scope {
            if url.ends_with('?') {
                url.push_str(&format!("scope={}", scope_val));
            } else {
                url.push_str(&format!("&scope={}", scope_val));
            }
        } else {
            // Construct scope from repository: repository:{repo}:pull
            let scope_val = format!("repository:{}:pull", repository);
            if url.ends_with('?') {
                url.push_str(&format!("scope={}", scope_val));
            } else {
                url.push_str(&format!("&scope={}", scope_val));
            }
        }

        url
    } else {
        tracing::warn!("No realm found in WWW-Authenticate header");
        return None;
    };

    // Check cache first
    if let Some(ref cache) = token_cache {
        let cache_key = token_url.clone();
        let cache_read = cache.read().await;
        if let Some(cached_token) = cache_read.get(&cache_key) {
            tracing::debug!("Using cached token for {}", cache_key);
            return Some(cached_token);
        }
    }

    // Request token
    let client = Client::new();
    match client.get(&token_url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                if let Ok(json) = response.json::<serde_json::Value>().await {
                    // Token field fallback: Try "token" first, then "access_token"
                    // Different registries use different field names in their token responses:
                    // - Docker Hub and most registries use "token"
                    // - Some registries (e.g., GitHub Container Registry) use "access_token"
                    let token = json
                        .get("token")
                        .and_then(|t| t.as_str())
                        .or_else(|| json.get("access_token").and_then(|t| t.as_str()));

                    if let Some(token_str) = token {
                        let token_value = token_str.to_string();

                        // Extract expires_in from response (if available)
                        let expires_in = json.get("expires_in").and_then(|v| v.as_u64());

                        // Cache the token
                        if let Some(ref cache) = token_cache {
                            let cache_key = token_url.clone();
                            let mut cache_write = cache.write().await;
                            cache_write.insert(cache_key, token_value.clone(), expires_in);
                        }

                        return Some(token_value);
                    }
                }
            }
        }
        Err(e) => {
            tracing::warn!("Failed to fetch registry token from {}: {}", token_url, e);
        }
    }

    None
}

/// Extract realm URL from WWW-Authenticate header (e.g. for matching 401 to a mirror).
pub fn realm_from_www_authenticate(www_auth: &str) -> Option<String> {
    let auth_str = www_auth.strip_prefix("Bearer ").unwrap_or(www_auth);
    for part in auth_str.split(',') {
        let part = part.trim();
        if part.starts_with("realm=") {
            return part
                .strip_prefix("realm=")
                .and_then(|s| s.strip_prefix('"'))
                .and_then(|s| s.strip_suffix('"'))
                .map(|s| s.to_string());
        }
    }
    None
}

/// Find the mirror index that corresponds to the auth realm from a 401 response.
/// Tokens are per-mirror: we must retry only this mirror with the token.
pub fn mirror_index_from_realm(realm_url: &str, mirrors: &[String]) -> Option<usize> {
    let realm_lower = realm_url.to_lowercase();
    let realm_host = realm_lower
        .strip_prefix("https://")
        .or_else(|| realm_lower.strip_prefix("http://"))
        .and_then(|s| s.split('/').next())
        .unwrap_or(&realm_lower);

    for (idx, mirror) in mirrors.iter().enumerate() {
        let mirror_trimmed = mirror.trim_end_matches('/');
        let mirror_host = mirror_trimmed
            .strip_prefix("https://")
            .or_else(|| mirror_trimmed.strip_prefix("http://"))
            .and_then(|s| s.split('/').next())
            .unwrap_or(mirror_trimmed);

        if realm_host == mirror_host {
            return Some(idx);
        }
        // Docker Hub: realm is auth.docker.io, mirror is registry-1.docker.io
        if realm_host == "auth.docker.io" && mirror_host.contains("registry-1.docker.io") {
            return Some(idx);
        }
    }
    None
}

/// Parse repository name into registry and repository parts
pub fn parse_repository(name: &str) -> (String, String) {
    // Format: registry/repository or just repository (defaults to docker.io)
    // Special case: "library/xxx" is a Docker Hub official image, not a registry named "library"
    // Special case: User repositories (e.g., "zyclonite/zerotier") are Docker Hub user repos, not a registry named "zyclonite"
    let result = if let Some(slash_idx) = name.find('/') {
        let first_part = &name[..slash_idx];
        // If first part is "library", it's a Docker Hub official image
        if first_part == "library" {
            (DEFAULT_REGISTRY_NAME.to_string(), name.to_string())
        } else {
            // Check if this looks like a Docker Hub user repository (has only one slash)
            // Known Docker Hub format: user/repo (e.g., "zyclonite/zerotier")
            // Known registry format: registry/user/repo (e.g., "quay.io/user/repo" or "ghcr.io/user/repo")
            // If the name has only one slash and doesn't contain a dot in the first part, treat as Docker Hub user repo
            if !first_part.contains('.') && name.matches('/').count() == 1 {
                // Docker Hub user repository (e.g., "zyclonite/zerotier" -> docker.io, "zyclonite/zerotier")
                (DEFAULT_REGISTRY_NAME.to_string(), name.to_string())
            } else {
                // Registry/repository format (e.g., "quay.io/cilium/cilium" -> "quay.io", "cilium/cilium")
                let registry = first_part.to_string();
                let repo = name[slash_idx + 1..].to_string();
                (registry, repo)
            }
        }
    } else {
        // No slash - Docker Hub official image (e.g., "nginx" -> docker.io, "library/nginx")
        (
            DEFAULT_REGISTRY_NAME.to_string(),
            format!("library/{}", name),
        )
    };
    tracing::debug!(
        name = %name,
        registry = %result.0,
        repository = %result.1,
        "Parsed repository name"
    );
    result
}

/// GET /v2/{name}/manifests/{reference}
pub async fn get_manifest(
    State(state): State<AppState>,
    Path((name, reference)): Path<(String, String)>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // CRITICAL: Always use schema 2 (v2) when fetching from upstream, regardless of client's Accept header.
    // containerd doesn't support schema 1 manifests, so we must ensure we only fetch schema 2 from upstream.
    // The client's Accept header may include schema 1 (e.g., containerd sends both v1 and v2),
    // but we ignore that and always request schema 2 from upstream.
    let upstream_accept_header = DEFAULT_MANIFEST_ACCEPT_HEADER;

    // Extract client's Accept header for logging (but don't use it for upstream requests)
    let client_accept_header = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("not provided");

    tracing::debug!(
        name = %name,
        reference = %reference,
        client_accept_header = %client_accept_header,
        upstream_accept_header = %upstream_accept_header,
        "GET manifest request received - always using schema 2 for upstream"
    );

    let cache = &state.cache;
    let (registry, repository) = parse_repository(&name);

    tracing::debug!(
        input_name = %name,
        parsed_registry = %registry,
        parsed_repository = %repository,
        reference = %reference,
        "Repository parsed from name - checking cache and upstream client"
    );

    tracing::debug!(
        registry = %registry,
        repository = %repository,
        reference = %reference,
        "Checking cache for manifest"
    );

    // Detect VM architecture from host platform.
    // VMs always match the host CPU architecture (Hyper-V on x86_64, Apple Virt on arm64).
    fn detect_platform_architecture() -> (String, String) {
        let arch = match std::env::consts::ARCH {
            "x86_64" => "amd64",
            "aarch64" => "arm64",
            other => other,
        };
        (arch.to_string(), "linux".to_string())
    }

    // Helper function to resolve platform-specific manifest from manifest list
    async fn resolve_platform_manifest(
        cache: &Arc<CacheStorage>,
        registry: &str,
        repository: &str,
        manifest_list_data: &[u8],
        _accept_header: &str,
    ) -> Result<Option<(Vec<u8>, String)>, crate::error::DockerProxyError> {
        use crate::error::DockerProxyError;
        // Parse manifest list
        let manifest_list: serde_json::Value =
            serde_json::from_slice(manifest_list_data).map_err(|e| {
                DockerProxyError::Registry(format!("Failed to parse manifest list: {}", e))
            })?;

        // Check if this is actually a manifest list
        let media_type = manifest_list
            .get("mediaType")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if !media_type.contains("manifest.list") && !media_type.contains("image.index") {
            // Not a manifest list, return as-is
            return Ok(None);
        }

        // Extract manifests array
        let manifests = manifest_list
            .get("manifests")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                DockerProxyError::Registry("Manifest list missing manifests array".to_string())
            })?;

        // Detect target platform
        let (target_arch, target_os) = detect_platform_architecture();

        // Try to find matching platform manifest
        let mut best_match: Option<&serde_json::Value> = None;
        for manifest in manifests {
            if let Some(platform) = manifest.get("platform") {
                let arch = platform
                    .get("architecture")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let os = platform.get("os").and_then(|v| v.as_str()).unwrap_or("");

                if arch == target_arch && os == target_os {
                    best_match = Some(manifest);
                    break;
                }
            }
        }

        // Fallback to first manifest if no exact match found
        let selected_manifest = best_match.or_else(|| manifests.first()).ok_or_else(|| {
            DockerProxyError::Registry("Manifest list has no manifests".to_string())
        })?;

        // Get digest of selected manifest
        let digest = selected_manifest
            .get("digest")
            .and_then(|v| v.as_str())
            .ok_or_else(|| DockerProxyError::Registry("Manifest missing digest".to_string()))?;

        // Try to read platform-specific manifest from cache
        match cache
            .read_manifest_by_digest(registry, repository, digest)
            .await
        {
            Ok(platform_manifest) => {
                let media_type = selected_manifest
                    .get("mediaType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("application/vnd.docker.distribution.manifest.v2+json");
                Ok(Some((platform_manifest, media_type.to_string())))
            }
            Err(_) => {
                // Platform-specific manifest not in cache - will need to fetch
                tracing::debug!(
                    registry = %registry,
                    repository = %repository,
                    digest = %digest,
                    "Platform-specific manifest not in cache, will fetch from upstream"
                );
                Ok(None)
            }
        }
    }

    // Cache lookup logic: digest-based with tag-to-digest mapping
    let cached_manifest_result = if reference.starts_with("sha256:") {
        // Direct digest lookup
        cache
            .read_manifest_by_digest(&registry, &repository, &reference)
            .await
    } else {
        // Tag lookup: check tag-to-digest mapping first
        match cache
            .read_tag_digest_mapping(&registry, &repository, &reference)
            .await
        {
            Ok(Some(mapped_digest)) => {
                tracing::debug!(
                    registry = %registry,
                    repository = %repository,
                    tag = %reference,
                    digest = %mapped_digest,
                    "Found tag-to-digest mapping, looking up by digest"
                );
                cache
                    .read_manifest_by_digest(&registry, &repository, &mapped_digest)
                    .await
            }
            Ok(None) => {
                // No mapping found - cache miss
                tracing::debug!(
                    registry = %registry,
                    repository = %repository,
                    tag = %reference,
                    "No tag-to-digest mapping found"
                );
                Err(crate::error::DockerProxyError::Cache(
                    "Cache miss".to_string(),
                ))
            }
            Err(e) => {
                tracing::warn!(
                    registry = %registry,
                    repository = %repository,
                    tag = %reference,
                    error = %e,
                    "Error reading tag-to-digest mapping"
                );
                Err(e)
            }
        }
    };

    match cached_manifest_result {
        Ok(manifest_data) => {
            // Validate cached manifest is schema 2
            let mut is_schema_1 = false;
            let mut is_manifest_list = false;
            let mut media_type = String::new();

            if let Ok(manifest_json) = serde_json::from_slice::<serde_json::Value>(&manifest_data) {
                // Check schemaVersion
                if let Some(schema_version) =
                    manifest_json.get("schemaVersion").and_then(|v| v.as_u64())
                {
                    if schema_version == 1 {
                        is_schema_1 = true;
                    }
                }
                // Check mediaType
                if let Some(mt) = manifest_json.get("mediaType").and_then(|v| v.as_str()) {
                    media_type = mt.to_string();
                    // Only reject Docker schema 1, not OCI Image Index or OCI Image Manifest v1
                    // Docker schema 1: application/vnd.docker.distribution.manifest.v1+json (NOT supported)
                    // OCI Image Index: application/vnd.oci.image.index.v1+json (SUPPORTED)
                    // OCI Image Manifest: application/vnd.oci.image.manifest.v1+json (SUPPORTED)
                    if mt == "application/vnd.docker.distribution.manifest.v1+json"
                        || mt.contains("schema1")
                    {
                        is_schema_1 = true;
                    }
                    if mt.contains("manifest.list") || mt.contains("image.index") {
                        is_manifest_list = true;
                    }
                }
            }

            // If schema 1, invalidate and fetch fresh
            if is_schema_1 {
                tracing::warn!(
                    registry = %registry,
                    repository = %repository,
                    reference = %reference,
                    "Cache HIT but manifest is schema 1 - invalidating and fetching schema 2"
                );
                // Fall through to fetch from upstream
            } else if is_manifest_list && !reference.starts_with("sha256:") {
                // Manifest list cached - try to resolve to platform-specific manifest
                tracing::debug!(
                    registry = %registry,
                    repository = %repository,
                    reference = %reference,
                    "Cached manifest is a manifest list, resolving to platform-specific manifest"
                );
                match resolve_platform_manifest(
                    cache,
                    &registry,
                    &repository,
                    &manifest_data,
                    client_accept_header,
                )
                .await
                {
                    Ok(Some((platform_manifest, platform_media_type))) => {
                        // Found platform-specific manifest in cache
                        let digest = format!("sha256:{:x}", Sha256::digest(&platform_manifest));
                        let mut response_headers = HeaderMap::new();
                        response_headers
                            .insert("Content-Type", platform_media_type.parse().unwrap());
                        response_headers.insert("Docker-Content-Digest", digest.parse().unwrap());
                        tracing::info!(
                            registry = %registry,
                            repository = %repository,
                            reference = %reference,
                            digest = %digest,
                            "Cache HIT: returning platform-specific manifest from manifest list"
                        );
                        return (StatusCode::OK, response_headers, platform_manifest)
                            .into_response();
                    }
                    Ok(None) => {
                        // Platform-specific manifest not in cache - will fetch
                        tracing::debug!(
                            registry = %registry,
                            repository = %repository,
                            reference = %reference,
                            "Platform-specific manifest not in cache, fetching from upstream"
                        );
                        // Fall through to fetch from upstream
                    }
                    Err(e) => {
                        tracing::warn!(
                            registry = %registry,
                            repository = %repository,
                            reference = %reference,
                            error = %e,
                            "Error resolving platform manifest from list, fetching from upstream"
                        );
                        // Fall through to fetch from upstream
                    }
                }
            } else {
                // Cached manifest is valid schema 2 single-platform manifest - return it
                let digest = format!("sha256:{:x}", Sha256::digest(&manifest_data));
                let content_type = if media_type.is_empty() {
                    "application/vnd.docker.distribution.manifest.v2+json".to_string()
                } else {
                    media_type
                };

                let mut response_headers = HeaderMap::new();
                response_headers.insert("Content-Type", content_type.parse().unwrap());
                response_headers.insert("Docker-Content-Digest", digest.parse().unwrap());
                tracing::info!(
                    registry = %registry,
                    repository = %repository,
                    reference = %reference,
                    digest = %digest,
                    size = manifest_data.len(),
                    "Cache HIT: returning cached schema 2 manifest"
                );
                return (StatusCode::OK, response_headers, manifest_data).into_response();
            }
        }
        Err(_) => {
            // Cache miss - continue to fetch from upstream
            tracing::debug!(
                registry = %registry,
                repository = %repository,
                reference = %reference,
                "Cache MISS: fetching from upstream"
            );
        }
    }

    // Cache miss - fetch from upstream
    tracing::warn!(
        registry = %registry,
        repository = %repository,
        reference = %reference,
        "Cache MISS: fetching from upstream"
    );

    tracing::debug!(
        registry = %registry,
        repository = %repository,
        reference = %reference,
        "Repository parsed - checking upstream client"
    );

    tracing::debug!(
        upstream_accept_header = %upstream_accept_header,
        "Using schema 2 Accept header for upstream request (containerd compatibility)"
    );

    // Get the appropriate upstream client for this registry
    // Check if registry is configured or will be auto-detected
    let is_configured = {
        let clients = state.registry_clients.read().unwrap();
        clients.contains_key(&registry)
    };

    tracing::debug!(
        registry = %registry,
        is_configured = is_configured,
        "Getting upstream client for registry (will auto-detect if not configured)"
    );

    let upstream_client = state.get_upstream_client(&registry);
    let mirrors = upstream_client.mirrors();
    if mirrors.is_empty() {
        tracing::error!(
            registry = %registry,
            "No upstream mirrors configured for registry"
        );
        return (StatusCode::BAD_GATEWAY, "No upstream mirrors configured").into_response();
    }

    tracing::debug!(
        registry = %registry,
        mirrors_count = mirrors.len(),
        mirrors = ?mirrors.iter().take(3).collect::<Vec<_>>(),
        is_configured = is_configured,
        "Upstream client obtained - mirrors ready for request"
    );

    // Build upstream URL
    // Docker Hub API format:
    // - Official images: /v2/library/{image}/manifests/{tag}
    // - User images: /v2/{user}/{image}/manifests/{tag}
    // The repository already includes the "library/" prefix if it's an official image
    let upstream_path = format!("/v2/{}/manifests/{}", repository, reference);

    tracing::debug!(
        registry = %registry,
        repository = %repository,
        reference = %reference,
        upstream_path = %upstream_path,
        mirrors = ?mirrors.iter().take(2).collect::<Vec<_>>(),
        "Constructed upstream path for manifest request"
    );

    // Log the full upstream URL that will be requested (for debugging)
    if let Some(first_mirror) = mirrors.first() {
        let full_url = format!("{}{}", first_mirror, upstream_path);
        tracing::debug!(
            registry = %registry,
            mirror = %first_mirror,
            upstream_path = %upstream_path,
            full_upstream_url = %full_url,
            "Full upstream URL that will be requested (first mirror)"
        );
    }

    tracing::debug!(
        upstream_path = %upstream_path,
        upstream_accept_header = %upstream_accept_header,
        "Fetching manifest from upstream"
    );

    // Get registry config for strategy
    let registry_config = state.get_registry_config(&registry);
    let strategy = registry_config.strategy;
    let hedge_delay_ms = registry_config.hedge_delay_ms;

    // Race all mirrors for fastest response
    tracing::debug!(
        upstream_path = %upstream_path,
        strategy = ?strategy,
        mirrors_count = mirrors.len(),
        "Racing mirrors for manifest"
    );

    let mut response = match crate::registry::race_mirrors(
        &upstream_client,
        mirrors,
        &upstream_path,
        None,
        strategy,
        hedge_delay_ms,
        Some(upstream_accept_header), // Always use schema 2 for upstream (containerd compatibility)
        None,                         // No Range header for manifest requests
        Some(&state.mirror_selector),
    )
    .await
    {
        Ok(resp) => {
            let resp_status = resp.status();
            let resp_status_u16 = resp_status.as_u16();
            tracing::debug!(
                upstream_path = %upstream_path,
                status = %resp_status,
                status_u16 = resp_status_u16,
                mirrors = ?mirrors.iter().take(2).collect::<Vec<_>>(),
                "Mirror race response received - checking for 401 auth requirement"
            );
            resp
        }
        Err(e) => {
            tracing::error!(
                upstream_path = %upstream_path,
                mirrors = ?mirrors.iter().take(2).collect::<Vec<_>>(),
                error = %e,
                "Failed to fetch manifest from upstream - all mirrors failed"
            );
            return (
                StatusCode::BAD_GATEWAY,
                format!("Failed to fetch from upstream: {}", e),
            )
                .into_response();
        }
    };

    // Handle 401 authentication required - get token and retry with racing
    let response_status = response.status();
    let response_status_u16 = response_status.as_u16();
    let unauthorized_u16 = reqwest::StatusCode::UNAUTHORIZED.as_u16();

    tracing::debug!(
        upstream_path = %upstream_path,
        status = %response_status,
        status_u16 = response_status_u16,
        unauthorized_u16 = unauthorized_u16,
        is_unauthorized = (response_status_u16 == unauthorized_u16),
        "Checking response status for 401 authentication"
    );

    if response_status_u16 == unauthorized_u16 {
        tracing::debug!(
            upstream_path = %upstream_path,
            status = %response_status,
            "Received 401 Unauthorized, fetching Docker Hub token"
        );
        // Extract WWW-Authenticate header
        if let Some(www_auth) = response.headers().get("www-authenticate") {
            if let Ok(auth_header) = www_auth.to_str() {
                tracing::debug!("WWW-Authenticate header: {}", auth_header);
                if let Some(token) = fetch_registry_token_with_cache(
                    auth_header,
                    &repository,
                    Some(state.token_cache.clone()),
                )
                .await
                {
                    tracing::debug!(
                        repository = %repository,
                        "Successfully fetched token, retrying with all mirrors"
                    );
                    // Retry racing all mirrors with Bearer token
                    response = match crate::registry::race_mirrors(
                        &upstream_client,
                        mirrors,
                        &upstream_path,
                        Some(&token),
                        strategy,
                        hedge_delay_ms,
                        Some(upstream_accept_header), // Always use schema 2 for upstream (containerd compatibility)
                        None,                         // No Range header for manifest requests
                        Some(&state.mirror_selector),
                    )
                    .await
                    {
                        Ok(resp) => {
                            tracing::debug!(
                                status = %resp.status(),
                                "Retry with token completed"
                            );
                            resp
                        }
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                "Failed to retry request with token"
                            );
                            return (
                                StatusCode::BAD_GATEWAY,
                                format!("Failed to fetch from upstream: {}", e),
                            )
                                .into_response();
                        }
                    };
                } else {
                    tracing::warn!("Failed to fetch Docker Hub token");
                }
            } else {
                tracing::warn!("Failed to parse WWW-Authenticate header");
            }
        } else {
            tracing::warn!("No WWW-Authenticate header in 401 response");
        }
    }

    let status = response.status();
    let status_u16 = status.as_u16();
    tracing::debug!(
        registry = %registry,
        repository = %repository,
        reference = %reference,
        status = %status,
        status_u16 = status_u16,
        "Final response status after auth handling"
    );

    let content_type_header = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Extract Docker-Content-Digest header before consuming response body
    let docker_content_digest = response
        .headers()
        .get("docker-content-digest")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    if status.is_success() {
        tracing::debug!(
            registry = %registry,
            repository = %repository,
            reference = %reference,
            status = %status,
            "Upstream request successful, processing manifest"
        );

        match response.bytes().await {
            Ok(manifest_bytes) => {
                let manifest_data = manifest_bytes.to_vec();

                // Validate that we received schema 2 manifest (containerd compatibility)
                // Check if manifest is schema 1 (which containerd doesn't support)
                if let Ok(manifest_json) =
                    serde_json::from_slice::<serde_json::Value>(&manifest_data)
                {
                    if let Some(schema_version) =
                        manifest_json.get("schemaVersion").and_then(|v| v.as_u64())
                    {
                        if schema_version == 1 {
                            tracing::error!(
                                registry = %registry,
                                repository = %repository,
                                reference = %reference,
                                "CRITICAL: Received schema 1 manifest from upstream, but containerd doesn't support it. This should not happen when requesting schema 2."
                            );
                            return (
                                StatusCode::BAD_GATEWAY,
                                "Upstream returned schema 1 manifest (not supported by containerd). Requested schema 2 but got schema 1.".to_string(),
                            )
                                .into_response();
                        }
                    }
                    // Also check mediaType for Docker schema 1 (NOT OCI Image Index/Manifest v1)
                    if let Some(media_type) =
                        manifest_json.get("mediaType").and_then(|v| v.as_str())
                    {
                        // Only reject Docker schema 1, not OCI Image Index or OCI Image Manifest v1
                        // Docker schema 1: application/vnd.docker.distribution.manifest.v1+json (NOT supported)
                        // OCI Image Index: application/vnd.oci.image.index.v1+json (SUPPORTED)
                        // OCI Image Manifest: application/vnd.oci.image.manifest.v1+json (SUPPORTED)
                        if media_type == "application/vnd.docker.distribution.manifest.v1+json"
                            || media_type.contains("schema1")
                        {
                            tracing::error!(
                                registry = %registry,
                                repository = %repository,
                                reference = %reference,
                                media_type = %media_type,
                                "CRITICAL: Received Docker schema 1 manifest from upstream, but containerd doesn't support it."
                            );
                            return (
                                StatusCode::BAD_GATEWAY,
                                format!("Upstream returned Docker schema 1 manifest (mediaType: {}) - not supported by containerd", media_type),
                            )
                                .into_response();
                        }
                    }
                }

                tracing::info!(
                    registry = %registry,
                    repository = %repository,
                    reference = %reference,
                    size_bytes = manifest_data.len(),
                    "Manifest received from upstream (schema 2 validated), caching"
                );

                // Extract digest from Docker-Content-Digest header (preferred) or calculate it
                let digest = docker_content_digest
                    .unwrap_or_else(|| format!("sha256:{:x}", Sha256::digest(&manifest_data)));

                // Check if this is a manifest list
                let is_manifest_list = if let Ok(manifest_json) =
                    serde_json::from_slice::<serde_json::Value>(&manifest_data)
                {
                    manifest_json
                        .get("mediaType")
                        .and_then(|v| v.as_str())
                        .map(|mt| mt.contains("manifest.list") || mt.contains("image.index"))
                        .unwrap_or(false)
                } else {
                    false
                };

                // Cache manifest by digest (primary storage - content-addressable)
                let digest_cache_result = cache
                    .write_manifest_by_digest(&registry, &repository, &digest, &manifest_data)
                    .await;

                match digest_cache_result {
                    Ok(_) => {
                        tracing::info!(
                            registry = %registry,
                            repository = %repository,
                            digest = %digest,
                            "Manifest cached successfully by digest"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            registry = %registry,
                            repository = %repository,
                            digest = %digest,
                            error = %e,
                            "Failed to cache manifest by digest - this is a critical error"
                        );
                        // Don't fail the request if cache write fails, but log it prominently
                    }
                }

                // If reference is a tag, write tag-to-digest mapping
                if !reference.starts_with("sha256:") {
                    let tag_mapping_result = cache
                        .write_tag_digest_mapping(&registry, &repository, &reference, &digest)
                        .await;

                    match tag_mapping_result {
                        Ok(_) => {
                            tracing::info!(
                                registry = %registry,
                                repository = %repository,
                                tag = %reference,
                                digest = %digest,
                                "Tag-to-digest mapping written"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                registry = %registry,
                                repository = %repository,
                                tag = %reference,
                                digest = %digest,
                                error = %e,
                                "Failed to write tag-to-digest mapping (non-critical)"
                            );
                        }
                    }
                }

                // If manifest is a manifest list, extract and cache platform-specific manifests
                if is_manifest_list {
                    if let Ok(manifest_list) =
                        serde_json::from_slice::<serde_json::Value>(&manifest_data)
                    {
                        if let Some(manifests) =
                            manifest_list.get("manifests").and_then(|v| v.as_array())
                        {
                            tracing::info!(
                                registry = %registry,
                                repository = %repository,
                                digest = %digest,
                                platform_count = manifests.len(),
                                "Manifest list detected, extracting platform-specific manifests"
                            );
                        }
                    }
                }

                // Get content type from response, or detect from manifest data
                let content_type = content_type_header.unwrap_or_else(|| {
                    // Try to detect from manifest mediaType field
                    if let Ok(manifest_json) =
                        serde_json::from_slice::<serde_json::Value>(&manifest_data)
                    {
                        manifest_json
                            .get("mediaType")
                            .and_then(|v| v.as_str())
                            .unwrap_or("application/vnd.docker.distribution.manifest.v2+json")
                            .to_string()
                    } else {
                        "application/vnd.docker.distribution.manifest.v2+json".to_string()
                    }
                });

                let mut headers = HeaderMap::new();
                headers.insert("Content-Type", content_type.parse().unwrap());
                headers.insert("Docker-Content-Digest", digest.parse().unwrap());

                tracing::info!(
                    registry = %registry,
                    repository = %repository,
                    reference = %reference,
                    "Returning manifest to client"
                );

                (StatusCode::OK, headers, manifest_data).into_response()
            }
            Err(e) => {
                tracing::error!(
                    registry = %registry,
                    repository = %repository,
                    reference = %reference,
                    error = %e,
                    "Failed to read manifest response bytes"
                );
                (
                    StatusCode::BAD_GATEWAY,
                    format!("Failed to read upstream response: {}", e),
                )
                    .into_response()
            }
        }
    } else {
        // Try to read error response body for debugging (clone response first to avoid consuming)
        let status_u16 = status.as_u16();
        let error_body = match response.text().await {
            Ok(body) => body,
            Err(e) => format!("Unable to read error body: {}", e),
        };

        tracing::error!(
            registry = %registry,
            repository = %repository,
            reference = %reference,
            status = %status,
            status_u16 = status_u16,
            upstream_accept_header = %upstream_accept_header,
            upstream_path = %upstream_path,
            error_body = %error_body,
            "Upstream returned error status"
        );

        // Special handling: if we got 404, it might mean the URL is wrong or the image doesn't exist
        // Log full diagnostic information for 404 errors
        if status_u16 == 404 {
            let full_upstream_url = mirrors
                .first()
                .map(|m| format!("{}{}", m, upstream_path))
                .unwrap_or_else(|| format!("<unknown mirror>{}", upstream_path));

            tracing::error!(
                registry = %registry,
                repository = %repository,
                reference = %reference,
                upstream_path = %upstream_path,
                full_upstream_url = %full_upstream_url,
                mirrors = ?mirrors,
                "Got 404 from upstream registry - this might indicate:\n  - URL construction issue (check upstream_path)\n  - Image doesn't exist at this registry\n  - Registry API format mismatch\n  - Network/proxy issue"
            );
        }

        // Return the upstream status code (404, 401, etc.)
        // The client will handle it appropriately
        // For 404 errors, provide detailed diagnostic information
        let error_message = if status_u16 == 404 {
            let full_upstream_url = mirrors
                .first()
                .map(|m| format!("{}{}", m, upstream_path))
                .unwrap_or_else(|| format!("<unknown mirror>{}", upstream_path));

            if error_body.is_empty() {
                format!(
                    "Upstream registry ({}) returned: {} (empty response body). \
                     Requested URL: {}\n\
                     Possible issues:\n  - URL construction issue (upstream_path: {})\n  - Image doesn't exist\n  - Registry API format mismatch\n  - Network/proxy issue",
                    registry, status, full_upstream_url, upstream_path
                )
            } else {
                format!(
                    "Upstream registry ({}) returned: {} - {}\n\
                     Requested URL: {}\n\
                     Upstream path: {}",
                    registry, status, error_body, full_upstream_url, upstream_path
                )
            }
        } else {
            format!("Upstream registry returned: {} - {}", status, error_body)
        };

        (
            StatusCode::from_u16(status_u16).unwrap_or(StatusCode::BAD_GATEWAY),
            error_message,
        )
            .into_response()
    }
}

/// HEAD /v2/{name}/manifests/{reference}
///
/// HEAD requests should return the same headers as GET but without the body.
/// If manifest is not in cache, fetch from upstream to populate cache and return headers.
pub async fn head_manifest(
    State(state): State<AppState>,
    Path((name, reference)): Path<(String, String)>,
) -> impl IntoResponse {
    let cache = &state.cache;
    let (registry, repository) = parse_repository(&name);

    tracing::info!(
        input_name = %name,
        parsed_registry = %registry,
        parsed_repository = %repository,
        reference = %reference,
        "HEAD manifest request - checking cache first"
    );

    // Cache lookup logic: same as GET request
    let cached_manifest_result = if reference.starts_with("sha256:") {
        // Direct digest lookup
        cache
            .read_manifest_by_digest(&registry, &repository, &reference)
            .await
    } else {
        // Tag lookup: check tag-to-digest mapping first
        match cache
            .read_tag_digest_mapping(&registry, &repository, &reference)
            .await
        {
            Ok(Some(mapped_digest)) => {
                tracing::debug!(
                    registry = %registry,
                    repository = %repository,
                    tag = %reference,
                    digest = %mapped_digest,
                    "Found tag-to-digest mapping for HEAD request, looking up by digest"
                );
                cache
                    .read_manifest_by_digest(&registry, &repository, &mapped_digest)
                    .await
            }
            Ok(None) => {
                // No mapping found - cache miss, will fetch from upstream
                tracing::debug!(
                    registry = %registry,
                    repository = %repository,
                    tag = %reference,
                    "No tag-to-digest mapping found for HEAD request - will fetch from upstream"
                );
                Err(crate::error::DockerProxyError::Cache(
                    "Cache miss".to_string(),
                ))
            }
            Err(e) => {
                tracing::warn!(
                    registry = %registry,
                    repository = %repository,
                    tag = %reference,
                    error = %e,
                    "Error reading tag-to-digest mapping for HEAD request"
                );
                Err(e)
            }
        }
    };

    // If found in cache, return headers immediately
    if let Ok(manifest_data) = cached_manifest_result {
        let content_type = if let Ok(manifest_json) =
            serde_json::from_slice::<serde_json::Value>(&manifest_data)
        {
            manifest_json
                .get("mediaType")
                .and_then(|v| v.as_str())
                .unwrap_or("application/vnd.docker.distribution.manifest.v2+json")
                .to_string()
        } else {
            "application/vnd.docker.distribution.manifest.v2+json".to_string()
        };

        let digest = format!("sha256:{:x}", Sha256::digest(&manifest_data));
        let mut response_headers = HeaderMap::new();
        response_headers.insert("Content-Type", content_type.parse().unwrap());
        response_headers.insert(
            "Content-Length",
            manifest_data.len().to_string().parse().unwrap(),
        );
        response_headers.insert("Docker-Content-Digest", digest.parse().unwrap());

        tracing::info!(
            registry = %registry,
            repository = %repository,
            reference = %reference,
            digest = %digest,
            size = manifest_data.len(),
            "HEAD request: Cache HIT - returning headers from cached manifest"
        );

        return (StatusCode::OK, response_headers).into_response();
    }

    // Cache miss - fetch from upstream (same logic as GET, but return only headers)
    tracing::info!(
        registry = %registry,
        repository = %repository,
        reference = %reference,
        "HEAD request: Cache MISS - fetching from upstream to populate cache"
    );

    // Use the same upstream fetch logic as GET request
    // We'll fetch the full manifest to populate cache, then return only headers
    let upstream_accept_header = DEFAULT_MANIFEST_ACCEPT_HEADER;

    let upstream_client = state.get_upstream_client(&registry);
    let mirrors = upstream_client.mirrors();
    if mirrors.is_empty() {
        tracing::error!(
            registry = %registry,
            "No upstream mirrors configured for registry in HEAD request"
        );
        return (StatusCode::BAD_GATEWAY, "No upstream mirrors configured").into_response();
    }

    let upstream_path = format!("/v2/{}/manifests/{}", repository, reference);

    tracing::info!(
        registry = %registry,
        repository = %repository,
        reference = %reference,
        upstream_path = %upstream_path,
        "HEAD request: Fetching manifest from upstream to populate cache"
    );

    let registry_config = state.get_registry_config(&registry);
    let strategy = registry_config.strategy;
    let hedge_delay_ms = registry_config.hedge_delay_ms;

    // Fetch from upstream (GET to get full manifest for caching)
    let mut response = match crate::registry::race_mirrors(
        &upstream_client,
        mirrors,
        &upstream_path,
        None,
        strategy,
        hedge_delay_ms,
        Some(upstream_accept_header),
        None, // No Range header for manifest requests
        Some(&state.mirror_selector),
    )
    .await
    {
        Ok(resp) => {
            let resp_status = resp.status();
            tracing::info!(
                upstream_path = %upstream_path,
                status = %resp_status,
                "HEAD request: Upstream response received"
            );
            resp
        }
        Err(e) => {
            tracing::error!(
                upstream_path = %upstream_path,
                error = %e,
                "HEAD request: Failed to fetch manifest from upstream"
            );
            return (
                StatusCode::BAD_GATEWAY,
                format!("Failed to fetch from upstream: {}", e),
            )
                .into_response();
        }
    };

    // Handle 401 authentication
    let response_status = response.status();
    let response_status_u16 = response_status.as_u16();
    if response_status_u16 == reqwest::StatusCode::UNAUTHORIZED.as_u16() {
        tracing::info!(
            upstream_path = %upstream_path,
            status = %response_status,
            registry = %registry,
            repository = %repository,
            "HEAD request: Received 401 Unauthorized, attempting to fetch token"
        );
        if let Some(www_auth) = response.headers().get("www-authenticate") {
            if let Ok(auth_header) = www_auth.to_str() {
                tracing::debug!(
                    registry = %registry,
                    repository = %repository,
                    www_authenticate = %auth_header,
                    "HEAD request: WWW-Authenticate header found, fetching token"
                );
                if let Some(token) = fetch_registry_token_with_cache(
                    auth_header,
                    &repository,
                    Some(state.token_cache.clone()),
                )
                .await
                {
                    tracing::info!(
                        registry = %registry,
                        repository = %repository,
                        "HEAD request: Successfully fetched token, retrying with all mirrors"
                    );
                    // Retry with token
                    response = match crate::registry::race_mirrors(
                        &upstream_client,
                        mirrors,
                        &upstream_path,
                        Some(&token),
                        strategy,
                        hedge_delay_ms,
                        Some(upstream_accept_header),
                        None, // No Range header for manifest requests
                        Some(&state.mirror_selector),
                    )
                    .await
                    {
                        Ok(resp) => {
                            tracing::info!(
                                status = %resp.status(),
                                "HEAD request: Retry with token completed"
                            );
                            resp
                        }
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                "HEAD request: Failed to retry with token"
                            );
                            return (
                                StatusCode::BAD_GATEWAY,
                                format!("Failed to fetch from upstream: {}", e),
                            )
                                .into_response();
                        }
                    };
                } else {
                    tracing::warn!(
                        registry = %registry,
                        repository = %repository,
                        www_authenticate = %auth_header,
                        "HEAD request: Failed to fetch registry token"
                    );
                }
            } else {
                tracing::warn!(
                    registry = %registry,
                    repository = %repository,
                    "HEAD request: Failed to parse WWW-Authenticate header"
                );
            }
        } else {
            tracing::warn!(
                registry = %registry,
                repository = %repository,
                "HEAD request: No WWW-Authenticate header in 401 response"
            );
        }
    }

    let status = response.status();
    let status_u16 = status.as_u16();

    if status.is_success() {
        // Extract headers before consuming response body
        let content_type_header = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let docker_content_digest_header = response
            .headers()
            .get("docker-content-digest")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Fetch succeeded - read manifest, cache it, return headers only
        match response.bytes().await {
            Ok(manifest_bytes) => {
                let manifest_data = manifest_bytes.to_vec();
                let digest = docker_content_digest_header
                    .unwrap_or_else(|| format!("sha256:{:x}", Sha256::digest(&manifest_data)));

                // Cache the manifest (same as GET request)
                let _ = cache
                    .write_manifest_by_digest(&registry, &repository, &digest, &manifest_data)
                    .await;

                if !reference.starts_with("sha256:") {
                    let _ = cache
                        .write_tag_digest_mapping(&registry, &repository, &reference, &digest)
                        .await;
                }

                // Use content type from upstream response or detect from manifest
                let content_type = content_type_header.unwrap_or_else(|| {
                    if let Ok(manifest_json) =
                        serde_json::from_slice::<serde_json::Value>(&manifest_data)
                    {
                        manifest_json
                            .get("mediaType")
                            .and_then(|v| v.as_str())
                            .unwrap_or("application/vnd.docker.distribution.manifest.v2+json")
                            .to_string()
                    } else {
                        "application/vnd.docker.distribution.manifest.v2+json".to_string()
                    }
                });

                let mut response_headers = HeaderMap::new();
                response_headers.insert("Content-Type", content_type.parse().unwrap());
                response_headers.insert(
                    "Content-Length",
                    manifest_data.len().to_string().parse().unwrap(),
                );
                response_headers.insert("Docker-Content-Digest", digest.parse().unwrap());

                tracing::info!(
                    registry = %registry,
                    repository = %repository,
                    reference = %reference,
                    digest = %digest,
                    size = manifest_data.len(),
                    "HEAD request: Manifest fetched from upstream, cached, returning headers"
                );

                (StatusCode::OK, response_headers).into_response()
            }
            Err(e) => {
                tracing::error!(
                    registry = %registry,
                    repository = %repository,
                    reference = %reference,
                    error = %e,
                    "HEAD request: Failed to read manifest response bytes"
                );
                (
                    StatusCode::BAD_GATEWAY,
                    format!("Failed to read upstream response: {}", e),
                )
                    .into_response()
            }
        }
    } else {
        // Upstream returned error - return same status
        let error_body = response.text().await.unwrap_or_default();

        tracing::error!(
            registry = %registry,
            repository = %repository,
            reference = %reference,
            status = %status,
            status_u16 = status_u16,
            upstream_path = %upstream_path,
            error_body = %error_body,
            "HEAD request: Upstream returned error status"
        );

        (
            StatusCode::from_u16(status_u16).unwrap_or(StatusCode::BAD_GATEWAY),
            error_body,
        )
            .into_response()
    }
}
