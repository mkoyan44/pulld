use crate::config::MirrorStrategy;
use crate::config::{
    DEFAULT_INITIAL_RTT_MS, DEFAULT_MIRROR_SCORE, ERROR_PENALTY_MS, MAX_ADAPTIVE_MIRRORS,
};
use crate::error::{DockerProxyError, Result};
use crate::registry::upstream::UpstreamClient;
use futures::future;
use reqwest::Response;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Race multiple mirrors with hedged strategy: start primary, after delay start secondary; cancel slower
#[allow(clippy::too_many_arguments)]
pub async fn race_mirrors_hedged(
    upstream_client: &UpstreamClient,
    mirrors: &[String],
    url_path: &str,
    auth_token: Option<&str>,
    hedge_delay_ms: u64,
    accept_header: Option<&str>,
    range_header: Option<&str>,
    mirror_selector: Option<&MirrorSelector>,
) -> Result<Response> {
    if mirrors.is_empty() {
        return Err(DockerProxyError::Registry(
            "No mirrors configured".to_string(),
        ));
    }

    if mirrors.len() == 1 {
        // Single mirror - just use it directly
        let request_start = Instant::now();
        let client = upstream_client.client(0).clone();
        let full_url = format!("{}{}", mirrors[0], url_path);
        let mut request = client.get(&full_url);
        if let Some(token) = auth_token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }
        if let Some(accept) = accept_header {
            request = request.header("Accept", accept);
        }
        if let Some(range) = range_header {
            request = request.header("Range", range);
        }
        let response = request.send().await.map_err(DockerProxyError::Http)?;

        // Track stats if selector provided
        if let Some(selector) = mirror_selector {
            let rtt_ms = request_start.elapsed().as_millis() as f64;
            let status = response.status();
            let bytes = response
                .headers()
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let success = status.is_success();
            selector
                .update_stats(
                    &mirrors[0],
                    success,
                    Some(rtt_ms),
                    Some(bytes),
                    Some(request_start.elapsed()),
                )
                .await;
        }

        return Ok(response);
    }

    let primary_mirror = &mirrors[0];
    let secondary_mirror = &mirrors[1];
    let client_primary = upstream_client.client(0).clone();
    let client_secondary = upstream_client.client(1).clone();
    let url_primary = format!("{}{}", primary_mirror, url_path);
    let url_secondary = format!("{}{}", secondary_mirror, url_path);
    let token = auth_token.map(|t| t.to_string());

    let accept = accept_header.map(|s| s.to_string());
    let range = range_header.map(|s| s.to_string());

    // Start primary request
    let mut primary_handle = {
        let token = token.clone();
        let accept = accept.clone();
        let range = range.clone();
        tokio::spawn(async move {
            let mut request = client_primary.get(&url_primary);
            if let Some(ref token) = token {
                request = request.header("Authorization", format!("Bearer {}", token));
            }
            if let Some(ref accept) = accept {
                request = request.header("Accept", accept);
            }
            if let Some(ref range) = range {
                request = request.header("Range", range.clone());
            }
            request.send().await.map_err(DockerProxyError::Http)
        })
    };

    // Start secondary request after delay
    let mut secondary_handle = {
        let accept = accept.clone();
        let range = range.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(hedge_delay_ms)).await;
            let mut request = client_secondary.get(&url_secondary);
            if let Some(ref token) = token {
                request = request.header("Authorization", format!("Bearer {}", token));
            }
            if let Some(ref accept) = accept {
                request = request.header("Accept", accept);
            }
            if let Some(ref range) = range {
                request = request.header("Range", range.clone());
            }
            request.send().await.map_err(DockerProxyError::Http)
        })
    };

    // Wait for first successful response (2xx) or 401 (for auth handling)
    // Continue waiting if we get error responses (4xx, 5xx) that aren't 401
    tokio::select! {
        result = &mut primary_handle => {
            match result {
                Ok(Ok(response)) => {
                    let status = response.status();
                    // Accept successful responses (2xx) or 401 (for auth handling)
                    if status.is_success() || status.as_u16() == 401 {
                        secondary_handle.abort();
                        tracing::debug!(mirror = %primary_mirror, status = %status, "Primary mirror succeeded");

                        // Track stats if selector provided
                        if let Some(selector) = mirror_selector {
                            let bytes = response
                                .headers()
                                .get("content-length")
                                .and_then(|v| v.to_str().ok())
                                .and_then(|s| s.parse::<u64>().ok())
                                .unwrap_or(0);
                            selector.update_stats(primary_mirror, true, None, Some(bytes), None).await;
                        }

                        Ok(response)
                    } else {
                        // Primary returned error status, wait for secondary
                        tracing::debug!(mirror = %primary_mirror, status = %status, "Primary mirror returned error, waiting for secondary");
                        let secondary_result = secondary_handle.await;
                        match secondary_result {
                            Ok(Ok(secondary_response)) => {
                                let secondary_status = secondary_response.status();
                                if secondary_status.is_success() || secondary_status == reqwest::StatusCode::UNAUTHORIZED {
                                    tracing::debug!(mirror = %secondary_mirror, status = %secondary_status, "Secondary mirror succeeded");

                                    // Track stats if selector provided
                                    if let Some(selector) = mirror_selector {
                                        let bytes = secondary_response
                                            .headers()
                                            .get("content-length")
                                            .and_then(|v| v.to_str().ok())
                                            .and_then(|s| s.parse::<u64>().ok())
                                            .unwrap_or(0);
                                        selector.update_stats(secondary_mirror, true, None, Some(bytes), None).await;
                                        selector.update_stats(primary_mirror, false, None, None, None).await;
                                    }

                                    Ok(secondary_response)
                                } else {
                                    // Both failed, return secondary (last error)
                                    tracing::error!(mirror = %secondary_mirror, status = %secondary_status, "Both mirrors failed");
                                    Ok(secondary_response)
                                }
                            }
                            Ok(Err(e)) => Err(e),
                            Err(_) => {
                                // Secondary panicked, return primary error response
                                tracing::error!(mirror = %primary_mirror, status = %status, "Secondary panicked, returning primary error");
                                Ok(response)
                            }
                        }
                    }
                }
                Ok(Err(e)) => {
                    // Primary failed, wait for secondary
                    tracing::debug!(error = %e, "Primary request failed, waiting for secondary");
                    secondary_handle
                        .await
                        .map_err(|_| DockerProxyError::Registry("Secondary task panicked".to_string()))?
                }
                Err(_) => {
                    // Primary panicked, wait for secondary
                    secondary_handle
                        .await
                        .map_err(|_| DockerProxyError::Registry("Primary task panicked".to_string()))?
                }
            }
        }
        result = &mut secondary_handle => {
            // Secondary completed first
            match result {
                Ok(Ok(secondary_response)) => {
                    let secondary_status = secondary_response.status();
                    // Accept successful responses (2xx) or 401 (for auth handling)
                    if secondary_status.is_success() || secondary_status == reqwest::StatusCode::UNAUTHORIZED {
                        primary_handle.abort();
                        tracing::debug!(mirror = %secondary_mirror, status = %secondary_status, "Secondary mirror succeeded first");

                        // Track stats if selector provided
                        if let Some(selector) = mirror_selector {
                            let bytes = secondary_response
                                .headers()
                                .get("content-length")
                                .and_then(|v| v.to_str().ok())
                                .and_then(|s| s.parse::<u64>().ok())
                                .unwrap_or(0);
                            selector.update_stats(secondary_mirror, true, None, Some(bytes), None).await;
                        }

                        Ok(secondary_response)
                    } else {
                        // Secondary returned error status, wait for primary
                        tracing::debug!(mirror = %secondary_mirror, status = %secondary_status, "Secondary returned error, waiting for primary");
                        let primary_result = primary_handle.await;
                        match primary_result {
                            Ok(Ok(primary_response)) => {
                                let primary_status = primary_response.status();
                                if primary_status.is_success() || primary_status == reqwest::StatusCode::UNAUTHORIZED {
                                    tracing::debug!(mirror = %primary_mirror, status = %primary_status, "Primary mirror succeeded");

                                    // Track stats if selector provided
                                    if let Some(selector) = mirror_selector {
                                        let bytes = primary_response
                                            .headers()
                                            .get("content-length")
                                            .and_then(|v| v.to_str().ok())
                                            .and_then(|s| s.parse::<u64>().ok())
                                            .unwrap_or(0);
                                        selector.update_stats(primary_mirror, true, None, Some(bytes), None).await;
                                        selector.update_stats(secondary_mirror, false, None, None, None).await;
                                    }

                                    Ok(primary_response)
                                } else {
                                    // Both failed, return primary (last error)
                                    tracing::error!(mirror = %primary_mirror, status = %primary_status, "Both mirrors failed");
                                    Ok(primary_response)
                                }
                            }
                            Ok(Err(e)) => Err(e),
                            Err(_) => {
                                // Primary panicked, return secondary error response
                                tracing::error!(mirror = %secondary_mirror, status = %secondary_status, "Primary panicked, returning secondary error");
                                Ok(secondary_response)
                            }
                        }
                    }
                }
                Ok(Err(_e)) => {
                    // Secondary failed, wait for primary
                    primary_handle
                        .await
                        .map_err(|_| DockerProxyError::Registry("Primary task panicked".to_string()))?
                }
                Err(_) => {
                    // Secondary panicked, wait for primary
                    primary_handle.await.map_err(|_| DockerProxyError::Registry("Secondary task panicked".to_string()))?
                }
            }
        }
    }
}

/// Request a single mirror by index. Used for 401 retry: token is valid only for the
/// mirror that returned 401, so we must retry that mirror only.
pub async fn request_single_mirror(
    upstream_client: &UpstreamClient,
    mirrors: &[String],
    mirror_index: usize,
    url_path: &str,
    auth_token: Option<&str>,
    range_header: Option<&str>,
) -> Result<Response> {
    let mirror = mirrors.get(mirror_index).ok_or_else(|| {
        DockerProxyError::Registry(format!(
            "Mirror index {} out of range ({} mirrors)",
            mirror_index,
            mirrors.len()
        ))
    })?;
    let client = upstream_client.client(mirror_index).clone();
    let base = mirror.trim_end_matches('/');
    let full_url = format!("{}{}", base, url_path);
    let mut request = client.get(&full_url);
    if let Some(token) = auth_token {
        request = request.header("Authorization", format!("Bearer {}", token));
    }
    if let Some(range) = range_header {
        request = request.header("Range", range);
    }
    let response = request.send().await.map_err(DockerProxyError::Http)?;
    Ok(response)
}

/// Race multiple mirrors and return the fastest successful response
/// Optionally includes an Authorization header for authenticated requests
/// Supports different strategies: failover, hedged, adaptive
/// If mirror_selector is provided, tracks stats for adaptive mirror selection
#[allow(clippy::too_many_arguments)]
pub async fn race_mirrors(
    upstream_client: &UpstreamClient,
    mirrors: &[String],
    url_path: &str,
    auth_token: Option<&str>,
    strategy: crate::config::MirrorStrategy,
    hedge_delay_ms: u64,
    accept_header: Option<&str>,
    range_header: Option<&str>,
    mirror_selector: Option<&MirrorSelector>,
) -> Result<Response> {
    if mirrors.is_empty() {
        return Err(DockerProxyError::Registry(
            "No mirrors configured".to_string(),
        ));
    }

    // Use hedged strategy if configured
    if strategy == crate::config::MirrorStrategy::Hedged && mirrors.len() >= 2 {
        return race_mirrors_hedged(
            upstream_client,
            mirrors,
            url_path,
            auth_token,
            hedge_delay_ms,
            accept_header,
            range_header,
            mirror_selector,
        )
        .await;
    }

    // For adaptive strategy, select top mirrors based on stats
    let mirrors_to_race = if strategy == crate::config::MirrorStrategy::Adaptive {
        // Use provided MirrorSelector or create a temporary one
        if let Some(selector) = mirror_selector {
            let selected_indices = selector.select_mirrors(mirrors).await;
            selected_indices
                .iter()
                .map(|&idx| mirrors[idx].clone())
                .collect::<Vec<_>>()
        } else {
            // No selector provided - create temporary one for this request
            let selector = MirrorSelector::new(strategy);
            let selected_indices = selector.select_mirrors(mirrors).await;
            selected_indices
                .iter()
                .map(|&idx| mirrors[idx].clone())
                .collect::<Vec<_>>()
        }
    } else {
        // Failover or other strategies - race all mirrors
        mirrors.to_vec()
    };

    tracing::debug!(strategy = ?strategy, "Racing {} mirrors for path: {}", mirrors_to_race.len(), url_path);

    // Start requests to all mirrors simultaneously
    let start_time = Instant::now();
    let futures: Vec<_> = mirrors_to_race
        .iter()
        .enumerate()
        .map(|(idx, mirror_url)| {
            let client = upstream_client.client(idx).clone();
            let full_url = format!("{}{}", mirror_url, url_path);
            let token = auth_token.map(|t| t.to_string());
            let accept = accept_header.map(|s| s.to_string()); // Clone string for 'static lifetime
            let range = range_header.map(|s| s.to_string());
            let mirror_name = mirror_url.clone();

            tokio::spawn(async move {
                let request_start = Instant::now();

                let mut request = client.get(&full_url);

                // Add Authorization header if token provided
                if let Some(ref token) = token {
                    request = request.header("Authorization", format!("Bearer {}", token));
                }

                // Add Accept header if provided (required for Docker Hub manifest requests)
                if let Some(ref accept) = accept {
                    request = request.header("Accept", accept);
                }

                // Add Range header if provided (for resume support)
                if let Some(ref range) = range {
                    request = request.header("Range", range.clone());
                }

                let response = request.send().await.map_err(DockerProxyError::Http)?;

                let rtt_ms = request_start.elapsed().as_millis() as f64;
                let status = response.status();
                let status_u16 = status.as_u16();

                // Log response headers for debugging (especially WWW-Authenticate for 401)
                let www_auth = response
                    .headers()
                    .get("www-authenticate")
                    .and_then(|h| h.to_str().ok())
                    .map(|s| s.to_string());

                tracing::debug!(
                    mirror = %mirror_name,
                    url = %full_url,
                    rtt_ms = rtt_ms,
                    status = %status,
                    status_u16 = status_u16,
                    www_authenticate = ?www_auth,
                    "Mirror response received - status check will happen in race loop"
                );

                Ok::<(usize, Response, f64), DockerProxyError>((idx, response, rtt_ms))
            })
        })
        .collect();

    // Wait for the first successful response (2xx).
    // 401 is stored but does NOT stop the race — a different mirror may serve the
    // blob without authentication (e.g. dockerproxy.com for public Docker Hub images).
    // Only after ALL mirrors have responded do we fall back to the stored 401 so the
    // caller (blob.rs) can attempt the token-exchange flow.
    let mut remaining_futures = futures;
    let mut last_error: Option<DockerProxyError> = None;
    let mut last_response: Option<(usize, Response, f64)> = None;
    // Keep the first 401 we see — prefer it over generic errors for auth handling
    let mut auth_response: Option<(usize, Response, f64)> = None;

    while !remaining_futures.is_empty() {
        let (result, _remaining_idx, futures_remaining) =
            future::select_all(remaining_futures).await;
        remaining_futures = futures_remaining;

        match result {
            Ok(Ok((idx, response, rtt_ms))) => {
                let status = response.status();
                let status_u16 = status.as_u16();

                tracing::debug!(
                    mirror_index = idx,
                    mirror = %mirrors[idx],
                    status = %status,
                    status_u16 = status_u16,
                    rtt_ms = rtt_ms,
                    "Received response from mirror - checking status"
                );

                // 2xx → immediate win, cancel everything else
                if status.is_success() {
                    let total_time = start_time.elapsed();
                    tracing::debug!(
                        mirror_index = idx,
                        mirror = %mirrors[idx],
                        rtt_ms = rtt_ms,
                        total_time_ms = total_time.as_millis(),
                        status = %status,
                        "Mirror returned 2xx - accepting immediately"
                    );

                    // Track stats if selector provided
                    if let Some(selector) = mirror_selector {
                        let bytes = response
                            .headers()
                            .get("content-length")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|s| s.parse::<u64>().ok())
                            .unwrap_or(0);
                        selector
                            .update_stats(
                                &mirrors[idx],
                                true,
                                Some(rtt_ms),
                                Some(bytes),
                                Some(total_time),
                            )
                            .await;
                    }

                    // Cancel remaining futures
                    for future in remaining_futures {
                        future.abort();
                    }

                    return Ok(response);
                } else if status_u16 == reqwest::StatusCode::UNAUTHORIZED.as_u16() {
                    // 401 → store for auth handling but keep racing other mirrors
                    tracing::debug!(
                        mirror_index = idx,
                        mirror = %mirrors[idx],
                        rtt_ms = rtt_ms,
                        "Mirror returned 401 - storing for auth fallback, continuing race"
                    );
                    if auth_response.is_none() {
                        auth_response = Some((idx, response, rtt_ms));
                    }
                } else {
                    // Store error response but continue waiting for other mirrors
                    tracing::warn!(
                        mirror_index = idx,
                        mirror = %mirrors[idx],
                        status = %status,
                        status_u16 = status_u16,
                        "Mirror returned non-success/non-401 status, waiting for other mirrors"
                    );

                    // Track error stats if selector provided
                    if let Some(selector) = mirror_selector {
                        selector
                            .update_stats(&mirrors[idx], false, Some(rtt_ms), None, None)
                            .await;
                    }

                    last_response = Some((idx, response, rtt_ms));
                    last_error = Some(DockerProxyError::Registry(format!(
                        "Mirror {} returned status {}",
                        mirrors[idx], status
                    )));
                }
            }
            Ok(Err(e)) => {
                tracing::debug!(error = %e, "Mirror request failed, waiting for other mirrors");

                // Store error for final error reporting if all mirrors fail
                last_error = Some(DockerProxyError::Registry(format!(
                    "Mirror request failed: {}",
                    e
                )));
            }
            Err(e) => {
                tracing::debug!(error = %e, "Mirror task panicked, waiting for other mirrors");
                last_error = Some(DockerProxyError::Registry(format!(
                    "Mirror race task panicked: {}",
                    e
                )));
            }
        }
    }

    // All mirrors responded — no 2xx found.
    // Return stored 401 (if any) so blob.rs can attempt token-exchange auth.
    if let Some((idx, response, _rtt_ms)) = auth_response {
        tracing::info!(
            mirror_index = idx,
            mirror = %mirrors[idx],
            mirror_count = mirrors.len(),
            "No mirror returned 2xx; returning stored 401 for auth handling"
        );
        return Ok(response);
    }

    // All mirrors have responded, but none were successful
    // Return the last error response if available (for proper error propagation)
    if let Some((idx, response, _rtt_ms)) = last_response {
        let final_status = response.status();
        let final_status_u16 = final_status.as_u16();
        tracing::error!(
            mirror_index = idx,
            mirror = %mirrors[idx],
            mirror_count = mirrors.len(),
            status = %final_status,
            status_u16 = final_status_u16,
            "All {} mirror(s) failed or returned non-success/non-401; returning last error (mirror {} returned {})",
            mirrors.len(),
            idx,
            final_status
        );

        // CRITICAL: If we got 404 from all mirrors, this is suspicious
        // Docker Hub should return 401 for authentication, not 404
        // This might indicate URL construction issue or request not reaching Docker Hub
        if final_status_u16 == 404 {
            tracing::error!(
                "Got 404 from all mirrors - this is unexpected for Docker Hub which should return 401 for auth. \
                 This might indicate:\n  - URL construction issue\n  - Request not reaching Docker Hub\n  - Network/proxy issue"
            );
        }

        // 502 Bad Gateway: upstream mirror(s) failed; failover was attempted across all configured mirrors
        if final_status_u16 == 502 {
            tracing::error!(
                mirror_count = mirrors.len(),
                "502 from upstream: all configured mirror(s) returned 502. \
                 Ensure multiple mirrors are configured for failover (e.g. docker.io: registry-1.docker.io, docker.m.daocloud.io, registry.dockermirror.com)."
            );
        }

        Ok(response)
    } else {
        // No response received at all
        let total_time = start_time.elapsed();
        tracing::error!(
            total_time_ms = total_time.as_millis(),
            error = ?last_error,
            "All mirrors failed with no response"
        );
        Err(last_error.unwrap_or_else(|| {
            DockerProxyError::Registry("All mirrors failed with no response".to_string())
        }))
    }
}

/// Statistics for a single mirror (EWMA-based tracking)
#[derive(Debug, Clone)]
pub struct MirrorStats {
    /// Round-trip time (EWMA in milliseconds)
    pub rtt_ewma: f64,
    /// Throughput (bytes per second, EWMA)
    pub throughput_ewma: f64,
    /// Error count (recent errors)
    pub error_count: u32,
    /// Success count (recent successes)
    pub success_count: u32,
    /// Last error timestamp
    pub last_error: Option<Instant>,
    /// Last success timestamp
    pub last_success: Option<Instant>,
}

impl Default for MirrorStats {
    fn default() -> Self {
        Self {
            rtt_ewma: DEFAULT_INITIAL_RTT_MS,
            throughput_ewma: 0.0,
            error_count: 0,
            success_count: 0,
            last_error: None,
            last_success: None,
        }
    }
}

impl MirrorStats {
    const ALPHA: f64 = 0.3; // EWMA smoothing factor

    /// Update stats with a successful request
    pub fn record_success(&mut self, rtt_ms: f64, bytes: u64, duration: Duration) {
        // Update RTT EWMA
        self.rtt_ewma = Self::ALPHA * rtt_ms + (1.0 - Self::ALPHA) * self.rtt_ewma;

        // Update throughput EWMA (bytes per second)
        let duration_secs = duration.as_secs_f64();
        if duration_secs > 0.0 {
            let throughput = bytes as f64 / duration_secs;
            self.throughput_ewma =
                Self::ALPHA * throughput + (1.0 - Self::ALPHA) * self.throughput_ewma;
        }

        self.success_count += 1;
        self.last_success = Some(Instant::now());

        // Decay error count on success
        if self.error_count > 0 {
            self.error_count = self.error_count.saturating_sub(1);
        }
    }

    /// Update stats with an error
    pub fn record_error(&mut self) {
        self.error_count += 1;
        self.last_error = Some(Instant::now());
    }

    /// Calculate score for adaptive selection (higher is better)
    /// Score = throughput / (RTT + error_penalty)
    pub fn score(&self) -> f64 {
        let error_penalty = self.error_count as f64 * ERROR_PENALTY_MS;
        let rtt_with_penalty = self.rtt_ewma + error_penalty;

        if rtt_with_penalty <= 0.0 {
            return 0.0;
        }

        // Prefer higher throughput and lower RTT
        // Add small constant to avoid division by zero
        self.throughput_ewma / (rtt_with_penalty + 1.0)
    }

    /// Get success rate (0.0 to 1.0)
    pub fn success_rate(&self) -> f64 {
        let total = self.success_count + self.error_count;
        if total == 0 {
            return 0.5; // Neutral if no data
        }
        self.success_count as f64 / total as f64
    }

    /// Get RTT in milliseconds
    pub fn rtt_ms(&self) -> f64 {
        self.rtt_ewma
    }
}

/// Mirror selector that tracks stats and selects mirrors based on strategy
pub struct MirrorSelector {
    stats: Arc<RwLock<HashMap<String, MirrorStats>>>,
    strategy: MirrorStrategy,
}

impl MirrorSelector {
    pub fn new(strategy: MirrorStrategy) -> Self {
        Self {
            stats: Arc::new(RwLock::new(HashMap::new())),
            strategy,
        }
    }

    /// Select mirrors based on strategy
    pub async fn select_mirrors(&self, mirrors: &[String]) -> Vec<usize> {
        match self.strategy {
            MirrorStrategy::Failover => (0..mirrors.len()).collect(),
            MirrorStrategy::Hedged => {
                // Primary + secondary (after delay)
                if mirrors.len() >= 2 {
                    vec![0, 1]
                } else {
                    vec![0]
                }
            }
            MirrorStrategy::Striped => {
                // All mirrors for parallel range requests
                (0..mirrors.len()).collect()
            }
            MirrorStrategy::Adaptive => {
                // Select top mirrors by score
                self.select_adaptive(mirrors).await
            }
        }
    }

    /// Select mirrors adaptively based on scores
    async fn select_adaptive(&self, mirrors: &[String]) -> Vec<usize> {
        let stats = self.stats.read().await;
        let mut scored: Vec<(usize, f64)> = mirrors
            .iter()
            .enumerate()
            .map(|(idx, mirror)| {
                let score = stats
                    .get(mirror)
                    .map(|s| s.score())
                    .unwrap_or(DEFAULT_MIRROR_SCORE);
                (idx, score)
            })
            .collect();

        // Sort by score (descending)
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Return top mirrors (or all if less than max)
        scored
            .into_iter()
            .take(MAX_ADAPTIVE_MIRRORS.min(mirrors.len()))
            .map(|(idx, _)| idx)
            .collect()
    }

    /// Update stats for a mirror
    pub async fn update_stats(
        &self,
        mirror: &str,
        success: bool,
        rtt_ms: Option<f64>,
        bytes: Option<u64>,
        duration: Option<Duration>,
    ) {
        let mut stats = self.stats.write().await;
        let mirror_stats = stats
            .entry(mirror.to_string())
            .or_insert_with(MirrorStats::default);

        if success {
            if let (Some(rtt), Some(bytes), Some(dur)) = (rtt_ms, bytes, duration) {
                mirror_stats.record_success(rtt, bytes, dur);
            } else {
                mirror_stats.success_count += 1;
                mirror_stats.last_success = Some(Instant::now());
            }
        } else {
            mirror_stats.record_error();
        }
    }

    /// Get stats for a mirror
    pub async fn get_stats(&self, mirror: &str) -> Option<MirrorStats> {
        let stats = self.stats.read().await;
        stats.get(mirror).cloned()
    }
}
