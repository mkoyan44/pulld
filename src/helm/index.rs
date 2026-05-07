// Helm repository index.yaml proxy

use crate::registry::manifest::AppState;
use axum::{extract::Path, extract::State, http::StatusCode, response::IntoResponse};
use reqwest::Client;
use serde_yaml::Value as YamlValue;

/// GET /helm/{repo}/index.yaml
pub async fn get_index(
    State(state): State<AppState>,
    Path(repo): Path<String>,
) -> impl IntoResponse {
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

    // Build index.yaml URL
    let index_url = if repo_url.ends_with('/') {
        format!("{}index.yaml", repo_url)
    } else {
        format!("{}/index.yaml", repo_url)
    };

    // Fetch index.yaml from upstream
    let client = Client::new();
    match client.get(&index_url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.bytes().await {
                    Ok(bytes) => {
                        // Parse YAML and rewrite chart URLs to point to pulld
                        match rewrite_index_urls(&bytes, &repo, &state) {
                            Ok(rewritten_bytes) => {
                                let mut headers = axum::http::HeaderMap::new();
                                headers
                                    .insert("Content-Type", "application/x-yaml".parse().unwrap());
                                (StatusCode::OK, headers, rewritten_bytes).into_response()
                            }
                            Err(e) => {
                                tracing::warn!(
                                    repo = %repo,
                                    error = %e,
                                    "Failed to rewrite index.yaml URLs, returning original"
                                );
                                // Return original if rewriting fails (better than failing the request)
                                let mut headers = axum::http::HeaderMap::new();
                                headers
                                    .insert("Content-Type", "application/x-yaml".parse().unwrap());
                                (StatusCode::OK, headers, bytes.to_vec()).into_response()
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to read Helm index response: {}", e);
                        (
                            StatusCode::BAD_GATEWAY,
                            format!("Failed to read upstream response: {}", e),
                        )
                            .into_response()
                    }
                }
            } else {
                (
                    StatusCode::from_u16(response.status().as_u16()).unwrap(),
                    "Upstream repository error",
                )
                    .into_response()
            }
        }
        Err(e) => {
            tracing::error!("Failed to fetch Helm index from {}: {}", index_url, e);
            (
                StatusCode::BAD_GATEWAY,
                format!("Failed to fetch from upstream: {}", e),
            )
                .into_response()
        }
    }
}

/// Rewrite chart URLs in Helm index.yaml to point to pulld
fn rewrite_index_urls(
    index_yaml: &[u8],
    repo_name: &str,
    state: &AppState,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // Parse YAML
    let mut index: YamlValue = serde_yaml::from_slice(index_yaml)?;

    // Build proxy base URL
    let proxy_base_url = format!(
        "{}://{}:{}/helm/{}/charts",
        state.proxy_scheme, state.proxy_host, state.proxy_port, repo_name
    );

    // Rewrite URLs in entries
    if let Some(entries) = index.get_mut("entries").and_then(|e| e.as_mapping_mut()) {
        for (_chart_name, chart_versions) in entries.iter_mut() {
            if let Some(versions) = chart_versions.as_sequence_mut() {
                for version_entry in versions.iter_mut() {
                    if let Some(urls) = version_entry
                        .get_mut("urls")
                        .and_then(|u| u.as_sequence_mut())
                    {
                        for url in urls.iter_mut() {
                            if let Some(url_str) = url.as_str() {
                                // Extract chart filename from original URL
                                // URLs can be absolute (https://...) or relative
                                let chart_filename = if let Some(last_slash) = url_str.rfind('/') {
                                    &url_str[last_slash + 1..]
                                } else {
                                    url_str
                                };

                                // Rewrite to pulld URL
                                let new_url = format!("{}/{}", proxy_base_url, chart_filename);
                                *url = YamlValue::String(new_url);
                            }
                        }
                    }
                }
            }
        }
    }

    // Serialize back to YAML
    let yaml_string = serde_yaml::to_string(&index)?;
    Ok(yaml_string.into_bytes())
}
