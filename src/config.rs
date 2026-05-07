use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::OnceLock;

// Constants for hardcoded values
/// Default Docker registry name
pub const DEFAULT_REGISTRY_NAME: &str = "docker.io";

/// Default Docker registry URL
pub const DEFAULT_REGISTRY_URL: &str = "https://registry-1.docker.io";

/// Default manifest Accept header for Docker registry API
/// CRITICAL: Must include manifest list types FIRST to get multi-arch images
/// Order: manifest list (Docker), image index (OCI), then single-platform manifests
pub const DEFAULT_MANIFEST_ACCEPT_HEADER: &str =
    "application/vnd.docker.distribution.manifest.list.v2+json, \
     application/vnd.oci.image.index.v1+json, \
     application/vnd.docker.distribution.manifest.v2+json, \
     application/vnd.oci.image.manifest.v1+json";

/// Default token expiry in seconds (5 minutes)
pub const DEFAULT_TOKEN_EXPIRY_SECS: u64 = 300;

/// Safety margin to subtract from token expiry (30 seconds)
pub const TOKEN_EXPIRY_SAFETY_MARGIN_SECS: u64 = 30;

/// Default initial RTT for mirror statistics (1 second in milliseconds)
pub const DEFAULT_INITIAL_RTT_MS: f64 = 1000.0;

/// Default score for unknown mirrors
pub const DEFAULT_MIRROR_SCORE: f64 = 1000.0;

/// Maximum number of mirrors to use in adaptive strategy
pub const MAX_ADAPTIVE_MIRRORS: usize = 3;

/// Error penalty in milliseconds per error
pub const ERROR_PENALTY_MS: f64 = 1000.0;

/// Parsed default configuration (parsed once at first access)
static DEFAULT_CONFIG: OnceLock<Config> = OnceLock::new();

/// Strategy for selecting mirrors
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum MirrorStrategy {
    /// Try mirrors in order, fallback on error
    Failover,
    /// Start primary, after delay start secondary; cancel slower
    Hedged,
    /// Split large blob into ranges across mirrors (if Range supported)
    Striped,
    /// Choose based on rolling stats (RTT + throughput - error penalty)
    #[default]
    Adaptive,
}

impl<'de> serde::Deserialize<'de> for MirrorStrategy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.to_lowercase().as_str() {
            "failover" => Ok(MirrorStrategy::Failover),
            "hedged" => Ok(MirrorStrategy::Hedged),
            "striped" => Ok(MirrorStrategy::Striped),
            "adaptive" => Ok(MirrorStrategy::Adaptive),
            _ => Err(serde::de::Error::custom(format!(
                "unknown variant `{}`, expected one of `failover`, `hedged`, `striped`, `adaptive`",
                s
            ))),
        }
    }
}

impl std::str::FromStr for MirrorStrategy {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "failover" => Ok(MirrorStrategy::Failover),
            "hedged" => Ok(MirrorStrategy::Hedged),
            "striped" => Ok(MirrorStrategy::Striped),
            "adaptive" => Ok(MirrorStrategy::Adaptive),
            _ => Err(format!("Unknown mirror strategy: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub cache: CacheConfig,
    pub upstream: UpstreamConfig,
    pub pre_pull: PrePullConfig,
    #[serde(default)]
    pub helm: HelmConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub bind_address: String,
    pub port: u16,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    /// Optional HTTP port for insecure connections (e.g., for localhost testing)
    /// If set, both HTTP and HTTPS servers will run simultaneously
    #[serde(default)]
    pub http_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub enabled: bool,
    pub cert_path: String,
    pub key_path: String,
    #[serde(default)]
    pub client_auth: bool,
    #[serde(default)]
    pub client_ca_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    pub directory: String,
    pub max_size_gb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpstreamConfig {
    #[serde(default)]
    pub tls: Option<UpstreamTlsConfig>,
    #[serde(default)]
    pub registries: HashMap<String, RegistryConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpstreamTlsConfig {
    #[serde(default)]
    pub ca_bundle_path: Option<String>,
    #[serde(default = "default_true")]
    pub use_system_ca: bool,
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryConfig {
    /// REQUIRED: Must have at least 1 mirror
    pub mirrors: Vec<String>,
    #[serde(default = "default_mirror_strategy_enum")]
    pub strategy: MirrorStrategy,
    #[serde(default = "default_max_parallel")]
    pub max_parallel: usize,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,
    #[serde(default = "default_hedge_delay_ms")]
    pub hedge_delay_ms: u64,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default)]
    pub auth: Option<RegistryAuth>,
    #[serde(default)]
    pub ca_cert_path: Option<String>,
    #[serde(default)]
    pub insecure: bool,
}

fn default_mirror_strategy_enum() -> MirrorStrategy {
    MirrorStrategy::Adaptive
}

fn default_max_parallel() -> usize {
    4
}

fn default_chunk_size() -> usize {
    16_777_216 // 16MB
}

fn default_hedge_delay_ms() -> u64 {
    100
}

fn default_timeout_secs() -> u64 {
    30
}

impl RegistryConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.mirrors.is_empty() {
            return Err("RegistryConfig must have at least one mirror".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryAuth {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrePullConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_image_concurrency")]
    pub image_concurrency: usize,
    #[serde(default = "default_layer_concurrency")]
    pub layer_concurrency: usize,
    #[serde(default = "default_chunk_concurrency")]
    pub chunk_concurrency: usize,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,
    #[serde(default = "default_mirror_strategy_enum")]
    pub mirror_strategy: MirrorStrategy,
    #[serde(default)]
    pub images: Vec<String>,
    #[serde(default)]
    pub charts: Vec<String>,
}

fn default_image_concurrency() -> usize {
    16 // Increased from 8 to 16 for faster pre-pull
}

fn default_layer_concurrency() -> usize {
    6
}

fn default_chunk_concurrency() -> usize {
    4
}

impl Config {
    /// Build the default configuration directly in Rust code (no TOML parsing)
    fn build_default() -> Config {
        use std::collections::HashMap;

        let mut registries = HashMap::new();

        // docker.io registry - primary + fallbacks for 502/unreachable (failover across mirrors).
        // All listed mirrors support anonymous pull for public images (no login).
        // Mirror list verified 2026-02-09: removed broken mirrors, added verified alternates.
        registries.insert(
            "docker.io".to_string(),
            RegistryConfig {
                mirrors: vec![
                    "https://registry-1.docker.io".to_string(), // Primary (verified manifest pull)
                    "https://registry.hub.docker.com".to_string(), // Official alternate (verified)
                    "https://docker.1ms.run".to_string(),       // Fast mirror (verified 401)
                    "https://dockerproxy.com".to_string(),      // Open proxy (verified 200)
                    "https://docker.m.daocloud.io".to_string(), // DaoCloud (verified 401)
                                                                // Removed: registry.dockermirror.com (broken - Cloudflare 521 error)
                ],
                strategy: MirrorStrategy::Failover,
                max_parallel: 4,
                chunk_size: 16_777_216, // 16MB
                hedge_delay_ms: 100,
                timeout_secs: 30,
                auth: None,
                ca_cert_path: None,
                insecure: false,
            },
        );

        // ghcr.io registry - all mirrors support anonymous pulls for public images
        registries.insert(
            "ghcr.io".to_string(),
            RegistryConfig {
                mirrors: vec![
                    "https://ghcr.io".to_string(),              // Primary (GitHub official)
                    "https://ghcr.dockerproxy.com".to_string(), // Fallback 1 - International
                    "https://ghcr.m.daocloud.io".to_string(), // Fallback 2 - Asia/International (DaoCloud)
                ],
                strategy: MirrorStrategy::Hedged,
                max_parallel: 3,
                chunk_size: default_chunk_size(),
                hedge_delay_ms: default_hedge_delay_ms(),
                timeout_secs: 600, // 10 minutes for large blob downloads (was 30)
                auth: None,        // No auth needed for public images
                ca_cert_path: None,
                insecure: false,
            },
        );

        // quay.io registry
        registries.insert(
            "quay.io".to_string(),
            RegistryConfig {
                mirrors: vec!["https://quay.io".to_string()],
                strategy: MirrorStrategy::Failover,
                max_parallel: 4, // Enable parallel downloads for striped strategy
                chunk_size: 16_777_216, // 16MB chunks for striped downloads
                hedge_delay_ms: default_hedge_delay_ms(),
                timeout_secs: 600, // 10 minutes for large blob downloads
                auth: None,
                ca_cert_path: None,
                insecure: false,
            },
        );

        Config {
            server: ServerConfig {
                bind_address: "0.0.0.0".to_string(),
                port: 5050,
                tls: None,
                http_port: None,
            },
            cache: CacheConfig {
                directory: "cache/pulld".to_string(),
                max_size_gb: 20,
            },
            upstream: UpstreamConfig {
                tls: Some(UpstreamTlsConfig {
                    ca_bundle_path: None,
                    use_system_ca: true,
                    insecure_skip_verify: false,
                }),
                registries,
            },
            pre_pull: PrePullConfig {
                enabled: true,
                image_concurrency: default_image_concurrency(),
                layer_concurrency: default_layer_concurrency(),
                chunk_concurrency: default_chunk_concurrency(),
                chunk_size: default_chunk_size(),
                mirror_strategy: MirrorStrategy::Adaptive,
                images: Self::get_platform_images(),
                charts: vec![],
            },
            helm: HelmConfig::default(),
        }
    }

    /// Pre-pull image list: base container images needed for device bootstrap
    fn get_platform_images() -> Vec<String> {
        vec![
            "docker.io/library/alpine:latest".to_string(),
            "docker.io/zerotier/zerotier:latest".to_string(),
            "docker.io/coredns/coredns:1.12.1".to_string(),
        ]
    }

    /// Get the default configuration (built in Rust code, cached in OnceLock)
    pub(crate) fn default_parsed() -> &'static Config {
        DEFAULT_CONFIG.get_or_init(Self::build_default)
    }
}

impl Default for Config {
    fn default() -> Self {
        // Use the default configuration (built in Rust code)
        Self::default_parsed().clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HelmConfig {
    #[serde(default)]
    pub repositories: HashMap<String, String>,
}
