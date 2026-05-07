// Helm chart tarball proxy

use crate::registry::manifest::AppState;
use axum::{extract::Path, extract::State, http::StatusCode, response::IntoResponse};
use reqwest::Client;
use serde_yaml::Value as YamlValue;

/// GET /helm/{repo}/charts/{chart}
pub async fn get_chart(
    State(state): State<AppState>,
    Path((repo, chart)): Path<(String, String)>,
) -> impl IntoResponse {
    let cache = &state.cache;

    // Ensure chart filename has .tgz extension for caching
    let chart_filename = if chart.ends_with(".tgz") {
        chart.clone()
    } else {
        format!("{}.tgz", chart)
    };

    // Check cache first
    if cache.chart_exists(&repo, &chart_filename).await {
        tracing::info!(
            repo = %repo,
            chart = %chart_filename,
            "Cache HIT: returning cached Helm chart"
        );
        match cache.read_chart(&repo, &chart_filename).await {
            Ok(chart_data) => {
                let mut headers = axum::http::HeaderMap::new();
                headers.insert("Content-Type", "application/gzip".parse().unwrap());
                headers.insert(
                    "Content-Disposition",
                    format!("attachment; filename=\"{}\"", chart_filename)
                        .parse()
                        .unwrap(),
                );
                return (StatusCode::OK, headers, chart_data).into_response();
            }
            Err(e) => {
                tracing::warn!(
                    repo = %repo,
                    chart = %chart_filename,
                    error = %e,
                    "Cache HIT but failed to read chart, fetching from upstream"
                );
                // Fall through to fetch from upstream
            }
        }
    } else {
        tracing::debug!(
            repo = %repo,
            chart = %chart_filename,
            "Cache MISS: fetching Helm chart from upstream"
        );
    }

    // Get repository URL from config
    let repo_url = match state.helm_repos.get(&repo) {
        Some(url) => url.clone(),
        None => {
            return (
                StatusCode::NOT_FOUND,
                format!("Helm repository '{}' not found in configuration", repo),
            )
                .into_response();
        }
    };

    // Extract chart name and version from filename
    // Format is typically: chart-name-version.tgz or chart-name-vversion.tgz
    // We need to find the original URL from index.yaml to get the correct upstream URL
    let index_url = if repo_url.ends_with('/') {
        format!("{}index.yaml", repo_url)
    } else {
        format!("{}/index.yaml", repo_url)
    };

    // Fetch index.yaml to get the original chart URL
    let client = Client::new();
    let chart_url = match client.get(&index_url).send().await {
        Ok(response) if response.status().is_success() => {
            match response.text().await {
                Ok(index_yaml) => {
                    // Parse index.yaml to find the chart URL
                    match serde_yaml::from_str::<YamlValue>(&index_yaml) {
                        Ok(index) => {
                            // Try to find the chart entry by matching the filename
                            // The filename from the rewritten URL should match exactly with the original URL filename
                            tracing::debug!(
                                repo = %repo,
                                chart = %chart_filename,
                                "Searching index.yaml for chart URL matching filename"
                            );

                            // Try to find matching chart entry by checking URLs
                            if let Some(entries) = index.get("entries").and_then(|e| e.as_mapping())
                            {
                                for (_chart_name, chart_versions) in entries.iter() {
                                    if let Some(versions) = chart_versions.as_sequence() {
                                        for version_entry in versions.iter() {
                                            if let Some(urls) = version_entry
                                                .get("urls")
                                                .and_then(|u| u.as_sequence())
                                            {
                                                for url in urls.iter() {
                                                    if let Some(url_str) = url.as_str() {
                                                        // Extract filename from original URL
                                                        let original_filename =
                                                            if let Some(last_slash) =
                                                                url_str.rfind('/')
                                                            {
                                                                &url_str[last_slash + 1..]
                                                            } else {
                                                                url_str
                                                            };

                                                        tracing::debug!(
                                                            repo = %repo,
                                                            requested_chart = %chart_filename,
                                                            original_filename = %original_filename,
                                                            "Comparing chart filenames"
                                                        );

                                                        // Check if filename matches exactly
                                                        // Since we rewrite URLs using the filename from the original URL,
                                                        // the filename should match exactly
                                                        if original_filename == chart_filename {
                                                            tracing::info!(
                                                                repo = %repo,
                                                                chart = %chart_filename,
                                                                original_url = %url_str,
                                                                "Found matching chart URL in index.yaml"
                                                            );
                                                            // Found matching URL - use it
                                                            if url_str.starts_with("http://")
                                                                || url_str.starts_with("https://")
                                                            {
                                                                tracing::debug!(
                                                                    repo = %repo,
                                                                    chart = %chart_filename,
                                                                    original_url = %url_str,
                                                                    "Found matching chart URL in index.yaml"
                                                                );
                                                                let response =
                                                                    fetch_and_cache_chart(
                                                                        client.clone(),
                                                                        url_str,
                                                                        cache,
                                                                        &repo,
                                                                        &chart_filename,
                                                                    )
                                                                    .await;
                                                                return response.into_response();
                                                            } else {
                                                                // Relative URL - prepend repo URL
                                                                let full_url =
                                                                    if repo_url.ends_with('/') {
                                                                        format!(
                                                                            "{}{}",
                                                                            repo_url, url_str
                                                                        )
                                                                    } else {
                                                                        format!(
                                                                            "{}/{}",
                                                                            repo_url, url_str
                                                                        )
                                                                    };
                                                                tracing::debug!(
                                                                    repo = %repo,
                                                                    chart = %chart_filename,
                                                                    original_url = %full_url,
                                                                    "Found matching chart URL in index.yaml (relative)"
                                                                );
                                                                let response =
                                                                    fetch_and_cache_chart(
                                                                        client.clone(),
                                                                        &full_url,
                                                                        cache,
                                                                        &repo,
                                                                        &chart_filename,
                                                                    )
                                                                    .await;
                                                                return response.into_response();
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            // Fallback: try constructing URL from filename (original behavior)
                            tracing::warn!(
                                repo = %repo,
                                chart = %chart_filename,
                                "Chart not found in index.yaml entries, using fallback URL construction"
                            );
                            if repo_url.ends_with('/') {
                                format!("{}{}", repo_url, chart_filename)
                            } else {
                                format!("{}/{}", repo_url, chart_filename)
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                repo = %repo,
                                chart = %chart_filename,
                                error = %e,
                                "Failed to parse index.yaml, using fallback URL construction"
                            );
                            // Fallback: try constructing URL from filename
                            if repo_url.ends_with('/') {
                                format!("{}{}", repo_url, chart_filename)
                            } else {
                                format!("{}/{}", repo_url, chart_filename)
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        repo = %repo,
                        chart = %chart_filename,
                        error = %e,
                        "Failed to read index.yaml, using fallback URL construction"
                    );
                    // Fallback: try constructing URL from filename
                    if repo_url.ends_with('/') {
                        format!("{}{}", repo_url, chart_filename)
                    } else {
                        format!("{}/{}", repo_url, chart_filename)
                    }
                }
            }
        }
        Ok(response) => {
            tracing::warn!(
                repo = %repo,
                chart = %chart_filename,
                status = %response.status(),
                "Failed to fetch index.yaml, using fallback URL construction"
            );
            // Fallback: try constructing URL from filename
            if repo_url.ends_with('/') {
                format!("{}{}", repo_url, chart_filename)
            } else {
                format!("{}/{}", repo_url, chart_filename)
            }
        }
        Err(e) => {
            tracing::warn!(
                repo = %repo,
                chart = %chart_filename,
                error = %e,
                "Failed to fetch index.yaml, using fallback URL construction"
            );
            // Fallback: try constructing URL from filename
            if repo_url.ends_with('/') {
                format!("{}{}", repo_url, chart_filename)
            } else {
                format!("{}/{}", repo_url, chart_filename)
            }
        }
    };

    // Fetch chart from upstream using the resolved URL
    fetch_and_cache_chart(client, &chart_url, cache, &repo, &chart_filename)
        .await
        .into_response()
}

/// Helper function to fetch a chart from upstream and cache it
async fn fetch_and_cache_chart(
    client: Client,
    chart_url: &str,
    cache: &crate::cache::storage::CacheStorage,
    repo: &str,
    chart_filename: &str,
) -> impl IntoResponse {
    match client.get(chart_url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.bytes().await {
                    Ok(bytes) => {
                        let chart_data = bytes.to_vec();

                        // Cache the chart (best effort - don't fail request if cache write fails)
                        if let Err(e) = cache.write_chart(repo, chart_filename, &chart_data).await {
                            tracing::warn!(
                                repo = %repo,
                                chart = %chart_filename,
                                error = %e,
                                "Failed to cache Helm chart (non-critical, continuing)"
                            );
                        } else {
                            tracing::info!(
                                repo = %repo,
                                chart = %chart_filename,
                                size = chart_data.len(),
                                "Cached Helm chart successfully"
                            );
                        }

                        let mut headers = axum::http::HeaderMap::new();
                        headers.insert("Content-Type", "application/gzip".parse().unwrap());
                        headers.insert(
                            "Content-Disposition",
                            format!("attachment; filename=\"{}\"", chart_filename)
                                .parse()
                                .unwrap(),
                        );
                        (StatusCode::OK, headers, chart_data).into_response()
                    }
                    Err(e) => {
                        tracing::error!("Failed to read Helm chart response: {}", e);
                        (
                            StatusCode::BAD_GATEWAY,
                            format!("Failed to read upstream response: {}", e),
                        )
                            .into_response()
                    }
                }
            } else {
                tracing::error!(
                    repo = %repo,
                    chart = %chart_filename,
                    url = %chart_url,
                    status = %response.status(),
                    "Upstream repository returned error status"
                );
                (
                    StatusCode::from_u16(response.status().as_u16()).unwrap(),
                    format!("Upstream repository error: HTTP {}", response.status()),
                )
                    .into_response()
            }
        }
        Err(e) => {
            tracing::error!(
                repo = %repo,
                chart = %chart_filename,
                url = %chart_url,
                error = %e,
                "Failed to fetch Helm chart from upstream"
            );
            (
                StatusCode::BAD_GATEWAY,
                format!("Failed to fetch from upstream: {}", e),
            )
                .into_response()
        }
    }
}
