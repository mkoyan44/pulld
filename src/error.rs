use thiserror::Error;

pub type Result<T> = std::result::Result<T, DockerProxyError>;

#[derive(Error, Debug)]
pub enum DockerProxyError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Cache error: {0}")]
    Cache(String),

    #[error("Registry error: {0}")]
    Registry(String),
}

impl DockerProxyError {
    /// Reconstruct a cloneable copy of the error.
    /// String-based variants are cloned directly; non-Clone variants (Io, Http, Serialization)
    /// are converted to a Cache error with their Debug representation.
    pub fn clone_error(&self) -> Self {
        match self {
            Self::Config(msg) => Self::Config(msg.clone()),
            Self::Tls(msg) => Self::Tls(msg.clone()),
            Self::Cache(msg) => Self::Cache(msg.clone()),
            Self::Registry(msg) => Self::Registry(msg.clone()),
            other => Self::Cache(format!("{:?}", other)),
        }
    }
}
