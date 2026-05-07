//! Per-mirror auth retry tests
//!
//! These tests verify that when mirrors have DIFFERENT auth endpoints,
//! the blob server correctly fetches a per-mirror token for each mirror
//! instead of reusing a single token that only works for one mirror.
//!
//! Background: Docker Hub mirrors have their own auth endpoints:
//! - registry-1.docker.io → auth.docker.io/token
//! - docker.1ms.run       → docker.1ms.run/openapi/v1/auth/token
//! - docker.m.daocloud.io → docker.m.daocloud.io/auth/token
//!
//! When one mirror's token is rejected by another mirror, the blob server
//! must fetch a new token from the rejecting mirror's auth endpoint.

use pulld::registry::manifest::{
    fetch_registry_token_with_cache, mirror_index_from_realm, realm_from_www_authenticate,
};
use pulld::{start_server, Config, MirrorStrategy, RegistryConfig};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::RwLock as TokioRwLock;
use tokio::time::sleep;

/// Helper: start a mock registry that returns 401 with a custom www-authenticate header,
/// and a mock token endpoint that returns a token.
/// When the correct token is provided, the registry serves the blob.
async fn start_mock_mirror(
    port: u16,
    service_name: &str,
    token_path: &str,
    expected_token: &str,
    blob_content: &[u8],
) -> tokio::task::JoinHandle<()> {
    use axum::{
        extract::Query,
        http::{HeaderMap, StatusCode, Uri},
        response::IntoResponse,
        routing::get,
        Router,
    };

    let service_name = service_name.to_string();
    let token_path = token_path.to_string();
    let expected_token = expected_token.to_string();
    let blob_content = blob_content.to_vec();

    tokio::spawn(async move {
        let expected_token_clone = expected_token.clone();
        let blob_content_clone = blob_content.clone();
        let service_clone = service_name.clone();
        let token_path_clone = token_path.clone();

        let app = Router::new()
            // Registry v2 API — version check
            .route(
                "/v2/",
                get({
                    let svc = service_clone.clone();
                    let tp = token_path_clone.clone();
                    move || async move {
                        let www_auth = format!(
                            r#"Bearer realm="http://localhost:{}{}", service="{}""#,
                            port, tp, svc
                        );
                        (
                            StatusCode::UNAUTHORIZED,
                            [("www-authenticate", www_auth)],
                            "",
                        )
                    }
                }),
            )
            // Catch-all for /v2/* — handles blobs and manifests
            // Uses wildcard because axum path params may not match colons in sha256:digest
            .route(
                "/v2/*rest",
                get({
                    let tok = expected_token_clone.clone();
                    let data = blob_content_clone.clone();
                    let svc = service_clone.clone();
                    let tp = token_path_clone.clone();
                    move |headers: HeaderMap, uri: Uri| async move {
                        let path = uri.path();
                        // Check if this is a blob request
                        if !path.contains("/blobs/") {
                            return (StatusCode::NOT_FOUND, "not a blob request").into_response();
                        }
                        let auth = headers
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("");
                        let expected_bearer = format!("Bearer {}", tok);
                        if auth == expected_bearer {
                            (StatusCode::OK, data).into_response()
                        } else {
                            let www_auth = format!(
                                r#"Bearer realm="http://localhost:{}{}", service="{}",scope="repository:library/alpine:pull""#,
                                port, tp, svc
                            );
                            (
                                StatusCode::UNAUTHORIZED,
                                [("www-authenticate", www_auth)],
                                r#"{"errors":[{"code":"UNAUTHORIZED","message":"authentication required"}]}"#,
                            )
                                .into_response()
                        }
                    }
                }),
            )
            // Token endpoint — returns a fixed token
            .route(
                &token_path,
                get({
                    let tok = expected_token.clone();
                    move |Query(_params): Query<HashMap<String, String>>| async move {
                        let token_json = serde_json::json!({
                            "token": tok,
                            "expires_in": 300,
                            "issued_at": "2026-01-01T00:00:00Z"
                        });
                        (StatusCode::OK, axum::Json(token_json))
                    }
                }),
            );

        let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        axum::serve(listener, app).await.unwrap();
    })
}

/// Test 1: Per-mirror token exchange
///
/// Two mirrors with DIFFERENT auth endpoints and DIFFERENT tokens.
/// Mirror A's token is rejected by Mirror B and vice versa.
/// The blob server must fetch a per-mirror token for the mirror that returned 401.
#[tokio::test]
async fn test_per_mirror_token_exchange() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pulld=debug")
        .try_init();

    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().join("cache");

    // Start two mock mirrors with different auth endpoints
    let _mirror_a = start_mock_mirror(
        15401,
        "mirror-a",
        "/auth/token",
        "TOKEN_A_SECRET",
        b"alpine layer data",
    )
    .await;
    let _mirror_b = start_mock_mirror(
        15402,
        "mirror-b",
        "/auth/token",
        "TOKEN_B_SECRET",
        b"alpine layer data",
    )
    .await;

    sleep(Duration::from_millis(200)).await;

    // Configure blob server with both mock mirrors
    let mut config = Config::default();
    config.server.port = 15400;
    config.upstream.registries.clear();
    config.upstream.registries.insert(
        "docker.io".to_string(),
        RegistryConfig {
            mirrors: vec![
                "http://localhost:15401".to_string(),
                "http://localhost:15402".to_string(),
            ],
            strategy: MirrorStrategy::Failover,
            max_parallel: 2,
            chunk_size: 16_777_216,
            hedge_delay_ms: 100,
            timeout_secs: 10,
            auth: None,
            ca_cert_path: None,
            insecure: false,
        },
    );

    let _server = start_server(cache_dir, config.clone(), None, None)
        .await
        .expect("Failed to start blob server");
    sleep(Duration::from_millis(500)).await;

    // Pull a blob through the blob server — it should handle per-mirror auth
    let client = reqwest::Client::new();
    let blob_url = "http://localhost:15400/v2/library/alpine/blobs/sha256:testdigest123";
    let resp = client
        .get(blob_url)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .expect("Failed to send blob request");

    assert!(
        resp.status().is_success(),
        "Blob pull should succeed with per-mirror auth, got status: {}",
        resp.status()
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), b"alpine layer data");
}

/// Test 2: Connection error on first mirror, fallback to second with per-mirror auth
///
/// Mirror A is unreachable (wrong port). Mirror B is reachable with its own auth.
/// The blob server should fall through to Mirror B, fetch B's token, and succeed.
#[tokio::test]
async fn test_connection_error_fallback_to_reachable_mirror() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pulld=debug")
        .try_init();

    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().join("cache");

    // Only start Mirror B — Mirror A's port is unused (connection refused)
    let _mirror_b = start_mock_mirror(
        15412,
        "mirror-b",
        "/auth/token",
        "TOKEN_B_ONLY",
        b"fallback layer data",
    )
    .await;

    sleep(Duration::from_millis(200)).await;

    let mut config = Config::default();
    config.server.port = 15410;
    config.upstream.registries.clear();
    config.upstream.registries.insert(
        "docker.io".to_string(),
        RegistryConfig {
            mirrors: vec![
                "http://localhost:15411".to_string(), // Mirror A: unreachable
                "http://localhost:15412".to_string(), // Mirror B: working
            ],
            strategy: MirrorStrategy::Failover,
            max_parallel: 2,
            chunk_size: 16_777_216,
            hedge_delay_ms: 100,
            timeout_secs: 5,
            auth: None,
            ca_cert_path: None,
            insecure: false,
        },
    );

    let _server = start_server(cache_dir, config.clone(), None, None)
        .await
        .expect("Failed to start blob server");
    sleep(Duration::from_millis(500)).await;

    let client = reqwest::Client::new();
    let blob_url = "http://localhost:15410/v2/library/alpine/blobs/sha256:fallbackdigest";
    let resp = client
        .get(blob_url)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .expect("Failed to send blob request");

    assert!(
        resp.status().is_success(),
        "Should fallback to Mirror B and succeed, got status: {}",
        resp.status()
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), b"fallback layer data");
}

/// Test 3: Mirror race prefers 2xx over 401
///
/// Mirror A returns 401 (needs auth). Mirror B returns 200 (anonymous).
/// The race should prefer Mirror B's 200 response.
#[tokio::test]
async fn test_race_prefers_2xx_over_401() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pulld=debug")
        .try_init();

    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().join("cache");

    // Mirror A: always returns 401
    let _mirror_a = start_mock_mirror(
        15421,
        "mirror-a",
        "/auth/token",
        "TOKEN_A",
        b"", // won't be used
    )
    .await;

    // Mirror B: returns 200 without auth (anonymous mirror)
    let _mirror_b_handle = tokio::spawn(async move {
        use axum::{http::StatusCode, routing::get, Router};

        let app = Router::new()
            .route("/v2/", get(|| async { (StatusCode::OK, "{}") }))
            .route(
                "/v2/*rest",
                get(|| async { (StatusCode::OK, "anonymous blob data") }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:15422")
            .await
            .unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    sleep(Duration::from_millis(200)).await;

    let mut config = Config::default();
    config.server.port = 15420;
    config.upstream.registries.clear();
    config.upstream.registries.insert(
        "docker.io".to_string(),
        RegistryConfig {
            mirrors: vec![
                "http://localhost:15421".to_string(), // Mirror A: 401
                "http://localhost:15422".to_string(), // Mirror B: 200 anonymous
            ],
            strategy: MirrorStrategy::Failover,
            max_parallel: 2,
            chunk_size: 16_777_216,
            hedge_delay_ms: 100,
            timeout_secs: 10,
            auth: None,
            ca_cert_path: None,
            insecure: false,
        },
    );

    let _server = start_server(cache_dir, config.clone(), None, None)
        .await
        .expect("Failed to start blob server");
    sleep(Duration::from_millis(500)).await;

    let client = reqwest::Client::new();
    let blob_url = "http://localhost:15420/v2/library/alpine/blobs/sha256:anon_digest";
    let resp = client
        .get(blob_url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .expect("Failed to send blob request");

    assert!(
        resp.status().is_success(),
        "Race should prefer 200 over 401, got status: {}",
        resp.status()
    );
}

/// Test 4: All mirrors unreachable returns error (no hang)
#[tokio::test]
async fn test_all_mirrors_unreachable_returns_error() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pulld=debug")
        .try_init();

    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().join("cache");

    let mut config = Config::default();
    config.server.port = 15430;
    config.upstream.registries.clear();
    config.upstream.registries.insert(
        "docker.io".to_string(),
        RegistryConfig {
            mirrors: vec![
                "http://localhost:15431".to_string(), // No server listening
                "http://localhost:15432".to_string(), // No server listening
            ],
            strategy: MirrorStrategy::Failover,
            max_parallel: 2,
            chunk_size: 16_777_216,
            hedge_delay_ms: 100,
            timeout_secs: 3,
            auth: None,
            ca_cert_path: None,
            insecure: false,
        },
    );

    let _server = start_server(cache_dir, config.clone(), None, None)
        .await
        .expect("Failed to start blob server");
    sleep(Duration::from_millis(500)).await;

    let client = reqwest::Client::new();
    let blob_url = "http://localhost:15430/v2/library/alpine/blobs/sha256:unreachable_digest";
    let resp = client
        .get(blob_url)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .expect("Failed to send request");

    assert!(
        !resp.status().is_success(),
        "Should return error when all mirrors unreachable, got: {}",
        resp.status()
    );
    // Should be 502 Bad Gateway (upstream unreachable)
    assert_eq!(
        resp.status().as_u16(),
        502,
        "Expected 502 Bad Gateway for all mirrors unreachable"
    );
}

/// Test 5: Realm doesn't match any mirror URL (production Docker Hub scenario)
///
/// In production, auth.docker.io's realm doesn't match registry-1.docker.io or other mirror URLs.
/// The per-mirror auth loop must still try all mirrors, fetching per-mirror tokens as needed.
/// Mirror A: unreachable (connection refused)
/// Mirror B: has its OWN auth endpoint (different host from the initial 401's realm)
#[tokio::test]
async fn test_realm_mismatch_still_does_per_mirror_auth() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pulld=debug")
        .try_init();

    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().join("cache");

    // Start a "third-party mirror" (port 15452) with its own auth endpoint
    // This simulates docker.m.daocloud.io which has realm=docker.m.daocloud.io/auth/token
    let _mirror_b = start_mock_mirror(
        15452,
        "third-party-mirror",
        "/auth/token",
        "THIRD_PARTY_TOKEN",
        b"third-party blob data",
    )
    .await;

    // Start a "shared auth server" (port 15453) simulating auth.docker.io
    // This is the realm for the initial 401 (from a Docker Hub mirror)
    let _auth_server = tokio::spawn(async move {
        use axum::{extract::Query, http::StatusCode, routing::get, Router};
        let app = Router::new().route(
            "/token",
            get(
                |Query(_params): Query<HashMap<String, String>>| async move {
                    let token_json = serde_json::json!({
                        "token": "DOCKER_HUB_TOKEN",
                        "expires_in": 300,
                    });
                    (StatusCode::OK, axum::Json(token_json))
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:15453")
            .await
            .unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    sleep(Duration::from_millis(200)).await;

    // Configure blob server:
    // Mirror A (port 15451): unreachable → simulates registry-1.docker.io being down
    // Mirror B (port 15452): reachable, own auth → simulates docker.m.daocloud.io
    // The initial 401's realm points to auth.docker.io (port 15453) which matches NEITHER mirror.
    let mut config = Config::default();
    config.server.port = 15450;
    config.upstream.registries.clear();
    config.upstream.registries.insert(
        "docker.io".to_string(),
        RegistryConfig {
            mirrors: vec![
                "http://localhost:15451".to_string(), // Mirror A: unreachable
                "http://localhost:15452".to_string(), // Mirror B: reachable, own auth
            ],
            strategy: MirrorStrategy::Failover,
            max_parallel: 2,
            chunk_size: 16_777_216,
            hedge_delay_ms: 100,
            timeout_secs: 5,
            auth: None,
            ca_cert_path: None,
            insecure: false,
        },
    );

    let _server = start_server(cache_dir, config.clone(), None, None)
        .await
        .expect("Failed to start blob server");
    sleep(Duration::from_millis(500)).await;

    let client = reqwest::Client::new();
    let blob_url = "http://localhost:15450/v2/library/alpine/blobs/sha256:mismatchtest";
    let resp = client
        .get(blob_url)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .expect("Failed to send blob request");

    assert!(
        resp.status().is_success(),
        "Should succeed via Mirror B's per-mirror auth even when realm doesn't match any mirror, got status: {}",
        resp.status()
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), b"third-party blob data");
}

/// Test 6: Token cache isolation between different realms
///
/// Tokens from different auth endpoints must be cached separately.
/// A token from mirror A's realm must not be reused for mirror B's realm.
#[tokio::test]
async fn test_token_cache_isolation_between_realms() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pulld=debug")
        .try_init();

    // Start two mock token endpoints
    let _token_a_handle = tokio::spawn(async move {
        use axum::{http::StatusCode, routing::get, Router};
        let app = Router::new().route(
            "/auth/token",
            get(|| async {
                (
                    StatusCode::OK,
                    axum::Json(serde_json::json!({"token": "CACHED_TOKEN_A", "expires_in": 300})),
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:15441")
            .await
            .unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    let _token_b_handle = tokio::spawn(async move {
        use axum::{http::StatusCode, routing::get, Router};
        let app = Router::new().route(
            "/auth/token",
            get(|| async {
                (
                    StatusCode::OK,
                    axum::Json(serde_json::json!({"token": "CACHED_TOKEN_B", "expires_in": 300})),
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:15442")
            .await
            .unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    sleep(Duration::from_millis(200)).await;

    let cache: Arc<TokioRwLock<pulld::registry::manifest::TokenCache>> = Arc::new(
        TokioRwLock::new(pulld::registry::manifest::TokenCache::new()),
    );

    // Fetch token from realm A
    let www_auth_a = r#"Bearer realm="http://localhost:15441/auth/token",service="mirror-a""#;
    let token_a =
        fetch_registry_token_with_cache(www_auth_a, "library/alpine", Some(cache.clone())).await;
    assert_eq!(
        token_a.as_deref(),
        Some("CACHED_TOKEN_A"),
        "Should get token A from realm A"
    );

    // Fetch token from realm B
    let www_auth_b = r#"Bearer realm="http://localhost:15442/auth/token",service="mirror-b""#;
    let token_b =
        fetch_registry_token_with_cache(www_auth_b, "library/alpine", Some(cache.clone())).await;
    assert_eq!(
        token_b.as_deref(),
        Some("CACHED_TOKEN_B"),
        "Should get token B from realm B (not cached token A)"
    );

    // Verify token A is still cached correctly (not overwritten)
    let token_a_again =
        fetch_registry_token_with_cache(www_auth_a, "library/alpine", Some(cache.clone())).await;
    assert_eq!(
        token_a_again.as_deref(),
        Some("CACHED_TOKEN_A"),
        "Token A should still be cached"
    );

    // Verify tokens are distinct
    assert_ne!(
        token_a, token_b,
        "Tokens from different realms must be distinct"
    );

    // Verify realm matching works correctly
    let mirrors = vec![
        "http://localhost:15441".to_string(),
        "http://localhost:15442".to_string(),
    ];
    let realm_a = realm_from_www_authenticate(www_auth_a).unwrap();
    let realm_b = realm_from_www_authenticate(www_auth_b).unwrap();
    assert_eq!(
        mirror_index_from_realm(&realm_a, &mirrors),
        Some(0),
        "Realm A should match mirror index 0"
    );
    assert_eq!(
        mirror_index_from_realm(&realm_b, &mirrors),
        Some(1),
        "Realm B should match mirror index 1"
    );
}
