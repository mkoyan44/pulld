use serde::{Deserialize, Serialize};

/// Manifest metadata stored alongside cached manifests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestMetadata {
    pub digest: String,
    pub content_type: String,
    pub size: u64,
    pub cached_at: u64, // Unix timestamp
}

/// Tag to digest mapping
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagMapping {
    pub tag: String,
    pub digest: String,
    pub updated_at: u64,
}
