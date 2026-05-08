use crate::error::{DockerProxyError, Result};
use axum::body::Body;
use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, ObjectStore, PutPayload};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{broadcast, Mutex};
use tokio_util::io::ReaderStream;

/// Download guard for singleflight deduplication
/// Maps digest to a broadcast sender that can send the download result to multiple waiters
/// Using Arc<Result> to make it Clone-able for broadcast channel
type DownloadGuard = Arc<Mutex<HashMap<String, broadcast::Sender<Arc<Result<Vec<u8>>>>>>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct S3CacheConfig {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub force_path_style: bool,
    pub prefix: String,
}

#[derive(Clone)]
enum CacheBackend {
    Filesystem,
    ObjectStore {
        store: Arc<dyn ObjectStore>,
        prefix: String,
    },
}

pub struct CachedBlob {
    pub size: u64,
    pub range_start: Option<u64>,
    pub body: Body,
}

const S3_MULTIPART_MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
const S3_MULTIPART_CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// Content-addressable blob storage
pub struct CacheStorage {
    base_dir: PathBuf,
    blobs_dir: PathBuf,
    manifests_dir: PathBuf,
    charts_dir: PathBuf,
    max_size_bytes: u64,
    backend: CacheBackend,
    /// Tracks in-flight downloads to prevent duplicate concurrent downloads
    in_flight: DownloadGuard,
}

impl CacheStorage {
    pub fn new(base_dir: PathBuf) -> Result<Self> {
        Self::with_max_size(base_dir, None)
    }

    pub fn with_max_size(base_dir: PathBuf, max_size_gb: Option<u64>) -> Result<Self> {
        let blobs_dir = base_dir.join("blobs").join("sha256");
        let manifests_dir = base_dir.join("manifests");
        let charts_dir = base_dir.join("charts");

        // Create directories
        std::fs::create_dir_all(&blobs_dir)
            .map_err(|e| DockerProxyError::Cache(format!("Failed to create blobs dir: {}", e)))?;
        std::fs::create_dir_all(&manifests_dir).map_err(|e| {
            DockerProxyError::Cache(format!("Failed to create manifests dir: {}", e))
        })?;
        std::fs::create_dir_all(&charts_dir)
            .map_err(|e| DockerProxyError::Cache(format!("Failed to create charts dir: {}", e)))?;

        let max_size_bytes = max_size_gb
            .map(|gb| gb * 1024 * 1024 * 1024)
            .unwrap_or(u64::MAX);

        Ok(Self {
            base_dir,
            blobs_dir,
            manifests_dir,
            charts_dir,
            max_size_bytes,
            backend: CacheBackend::Filesystem,
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn with_s3_config(
        base_dir: PathBuf,
        max_size_gb: Option<u64>,
        config: S3CacheConfig,
    ) -> Result<Self> {
        let allow_http = config.endpoint.starts_with("http://");
        let store = AmazonS3Builder::new()
            .with_endpoint(config.endpoint)
            .with_bucket_name(config.bucket)
            .with_region(config.region)
            .with_access_key_id(config.access_key_id)
            .with_secret_access_key(config.secret_access_key)
            .with_allow_http(allow_http)
            .with_virtual_hosted_style_request(!config.force_path_style)
            .build()
            .map_err(|e| DockerProxyError::Cache(format!("Failed to build S3 cache: {}", e)))?;

        Self::with_object_store(base_dir, Arc::new(store), config.prefix, max_size_gb)
    }

    pub fn with_object_store(
        base_dir: PathBuf,
        store: Arc<dyn ObjectStore>,
        prefix: String,
        max_size_gb: Option<u64>,
    ) -> Result<Self> {
        let blobs_dir = base_dir.join("blobs").join("sha256");
        let manifests_dir = base_dir.join("manifests");
        let charts_dir = base_dir.join("charts");

        std::fs::create_dir_all(&blobs_dir)
            .map_err(|e| DockerProxyError::Cache(format!("Failed to create blobs dir: {}", e)))?;
        std::fs::create_dir_all(&manifests_dir).map_err(|e| {
            DockerProxyError::Cache(format!("Failed to create manifests dir: {}", e))
        })?;
        std::fs::create_dir_all(&charts_dir)
            .map_err(|e| DockerProxyError::Cache(format!("Failed to create charts dir: {}", e)))?;

        let max_size_bytes = max_size_gb
            .map(|gb| gb * 1024 * 1024 * 1024)
            .unwrap_or(u64::MAX);

        Ok(Self {
            base_dir,
            blobs_dir,
            manifests_dir,
            charts_dir,
            max_size_bytes,
            backend: CacheBackend::ObjectStore {
                store,
                prefix: normalize_prefix(&prefix),
            },
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn is_object_store_backed(&self) -> bool {
        matches!(self.backend, CacheBackend::ObjectStore { .. })
    }

    /// Get blob path for a digest
    pub fn blob_path(&self, digest: &str) -> PathBuf {
        // Remove "sha256:" prefix if present
        let digest = digest.strip_prefix("sha256:").unwrap_or(digest);
        self.blobs_dir.join(digest)
    }

    pub fn blob_object_key(&self, digest: &str) -> String {
        let digest = digest.strip_prefix("sha256:").unwrap_or(digest);
        self.object_key(["blobs", "sha256", digest])
    }

    pub fn manifest_object_key(&self, registry: &str, repository: &str, reference: &str) -> String {
        let ref_clean = reference.strip_prefix("sha256:").unwrap_or(reference);
        let ref_safe = ref_clean.replace(':', "_");
        self.object_key([
            "manifests",
            registry,
            repository,
            "refs",
            &format!("{}.json", ref_safe),
        ])
    }

    pub fn manifest_object_key_by_digest(
        &self,
        registry: &str,
        repository: &str,
        digest: &str,
    ) -> String {
        let digest = digest.strip_prefix("sha256:").unwrap_or(digest);
        self.object_key([
            "manifests",
            registry,
            repository,
            "sha256",
            &format!("{}.json", digest),
        ])
    }

    pub fn tag_digest_mapping_object_key(
        &self,
        registry: &str,
        repository: &str,
        tag: &str,
    ) -> String {
        let tag_safe = tag.replace(':', "_");
        self.object_key([
            "manifests",
            registry,
            repository,
            "tags",
            &format!("{}.digest", tag_safe),
        ])
    }

    pub fn chart_object_key(&self, repo: &str, chart: &str) -> String {
        self.object_key(["charts", repo, chart])
    }

    /// Check if blob exists
    pub async fn blob_exists(&self, digest: &str) -> bool {
        match &self.backend {
            CacheBackend::Filesystem => self.blob_path(digest).exists(),
            CacheBackend::ObjectStore { store, .. } => {
                let key = self.blob_object_key(digest);
                object_head(store.as_ref(), &key).await.is_ok()
            }
        }
    }

    /// Get blob size in bytes
    pub async fn blob_size(&self, digest: &str) -> Option<u64> {
        match &self.backend {
            CacheBackend::Filesystem => {
                let path = self.blob_path(digest);
                if let Ok(metadata) = tokio::fs::metadata(&path).await {
                    Some(metadata.len())
                } else {
                    None
                }
            }
            CacheBackend::ObjectStore { store, .. } => {
                let key = self.blob_object_key(digest);
                object_head(store.as_ref(), &key)
                    .await
                    .ok()
                    .map(|meta| meta.size)
            }
        }
    }

    /// Download blob with deduplication (singleflight pattern)
    /// If a download for the same digest is already in progress, waits for it instead of starting a new one
    pub async fn download_with_dedupe<F, Fut>(
        &self,
        digest: &str,
        download_fn: F,
    ) -> Result<Vec<u8>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<Vec<u8>>> + Send + 'static,
    {
        // Atomically check if already downloading and register if not
        let existing_rx = {
            let mut in_flight = self.in_flight.lock().await;
            // Use entry API to atomically check and insert
            match in_flight.entry(digest.to_string()) {
                std::collections::hash_map::Entry::Occupied(entry) => {
                    // Download already in progress - subscribe to existing broadcast
                    Some(entry.get().subscribe())
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    // No download in progress - create new broadcast channel
                    let (tx, _) = broadcast::channel(1);
                    entry.insert(tx.clone());
                    None
                }
            }
        };

        if let Some(mut existing_rx) = existing_rx {
            // Download already in progress - wait for it
            tracing::debug!("Waiting for existing download of blob {}", digest);
            match existing_rx.recv().await {
                Ok(result_arc) => {
                    // Extract Result from Arc - clone the data or reconstruct error
                    match Arc::try_unwrap(result_arc) {
                        Ok(result) => return result,
                        Err(arc) => match arc.as_ref() {
                            Ok(data) => return Ok(data.clone()),
                            Err(e) => return Err(e.clone_error()),
                        },
                    }
                }
                Err(e) => {
                    return Err(DockerProxyError::Cache(format!(
                        "Download channel error: {}",
                        e
                    )));
                }
            }
        }

        // Start new download (we're the first one)
        let digest_clone = digest.to_string();
        let in_flight_clone = self.in_flight.clone();
        let tx = {
            let in_flight = self.in_flight.lock().await;
            in_flight.get(digest).unwrap().clone()
        };

        let tx_clone = tx.clone();
        tokio::spawn(async move {
            let result = download_fn().await;
            let _ = tx_clone.send(Arc::new(result));

            // Remove from in-flight map when done
            let mut in_flight = in_flight_clone.lock().await;
            in_flight.remove(&digest_clone);
        });

        // Wait for download to complete
        let mut rx = tx.subscribe();
        match rx.recv().await {
            Ok(result_arc) => {
                // Extract Result from Arc
                match Arc::try_unwrap(result_arc) {
                    Ok(result) => result,
                    Err(arc) => match arc.as_ref() {
                        Ok(data) => Ok(data.clone()),
                        Err(e) => Err(e.clone_error()),
                    },
                }
            }
            Err(e) => Err(DockerProxyError::Cache(format!(
                "Download channel error: {}",
                e
            ))),
        }
    }

    /// Read blob content
    pub async fn read_blob(&self, digest: &str) -> Result<Vec<u8>> {
        match &self.backend {
            CacheBackend::Filesystem => {
                let path = self.blob_path(digest);

                // Touch the file to update access time for LRU eviction
                // This ensures recently accessed blobs are less likely to be evicted
                if let Ok(file) = std::fs::OpenOptions::new()
                    .read(true)
                    .write(false)
                    .open(&path)
                {
                    // Just opening the file with write=false won't update mtime on some systems
                    // But the read operation itself signals recent access
                    drop(file);
                }

                fs::read(&path).await.map_err(|e| {
                    DockerProxyError::Cache(format!("Failed to read blob {}: {}", digest, e))
                })
            }
            CacheBackend::ObjectStore { store, .. } => {
                let key = self.blob_object_key(digest);
                object_get(store.as_ref(), &key).await
            }
        }
    }

    /// Read blob content from byte offset to EOF.
    pub async fn read_blob_range(&self, digest: &str, start: u64) -> Result<Vec<u8>> {
        let size = self
            .blob_size(digest)
            .await
            .ok_or_else(|| DockerProxyError::Cache(format!("Blob not found: {}", digest)))?;
        if start >= size {
            return Ok(Vec::new());
        }

        match &self.backend {
            CacheBackend::Filesystem => {
                let mut file = fs::File::open(self.blob_path(digest)).await.map_err(|e| {
                    DockerProxyError::Cache(format!("Failed to open blob {}: {}", digest, e))
                })?;
                file.seek(std::io::SeekFrom::Start(start))
                    .await
                    .map_err(|e| {
                        DockerProxyError::Cache(format!("Failed to seek blob {}: {}", digest, e))
                    })?;
                let mut data = Vec::new();
                file.read_to_end(&mut data).await.map_err(|e| {
                    DockerProxyError::Cache(format!("Failed to read blob {}: {}", digest, e))
                })?;
                Ok(data)
            }
            CacheBackend::ObjectStore { store, .. } => {
                let key = self.blob_object_key(digest);
                object_get_range(store.as_ref(), &key, start, size).await
            }
        }
    }

    pub async fn open_blob(
        &self,
        digest: &str,
        range_start: Option<u64>,
    ) -> Result<Option<CachedBlob>> {
        let Some(size) = self.blob_size(digest).await else {
            return Ok(None);
        };
        if size == 0 {
            return Ok(None);
        }

        let start = range_start.unwrap_or(0);
        if start >= size {
            return Ok(None);
        }

        let body = match &self.backend {
            CacheBackend::Filesystem => {
                let mut file = fs::File::open(self.blob_path(digest)).await.map_err(|e| {
                    DockerProxyError::Cache(format!("Failed to open blob {}: {}", digest, e))
                })?;
                if start > 0 {
                    file.seek(std::io::SeekFrom::Start(start))
                        .await
                        .map_err(|e| {
                            DockerProxyError::Cache(format!(
                                "Failed to seek blob {}: {}",
                                digest, e
                            ))
                        })?;
                }
                Body::from_stream(ReaderStream::new(file))
            }
            CacheBackend::ObjectStore { store, .. } => {
                let key = self.blob_object_key(digest);
                let path = ObjectPath::from(key);
                let options = if start > 0 {
                    GetOptions {
                        range: Some((start..size).into()),
                        ..Default::default()
                    }
                } else {
                    GetOptions::default()
                };
                let result = store.get_opts(&path, options).await.map_err(|e| {
                    DockerProxyError::Cache(format!("Failed to open S3 blob {}: {}", digest, e))
                })?;
                Body::from_stream(result.into_stream())
            }
        };

        Ok(Some(CachedBlob {
            size,
            range_start: range_start.filter(|start| *start > 0),
            body,
        }))
    }

    /// Write blob content (atomic write)
    pub async fn write_blob(&self, digest: &str, data: &[u8]) -> Result<()> {
        if let CacheBackend::ObjectStore { store, .. } = &self.backend {
            let key = self.blob_object_key(digest);
            object_put(store.as_ref(), &key, Bytes::copy_from_slice(data)).await?;
            return Ok(());
        }

        let path = self.blob_path(digest);

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                DockerProxyError::Cache(format!("Failed to create blob dir: {}", e))
            })?;
        }

        // Check cache size and evict if necessary before writing
        self.evict_if_needed(data.len() as u64).await?;

        // Atomic write: write to temp file, then rename
        let temp_path = path.with_extension("tmp");
        fs::write(&temp_path, data).await.map_err(|e| {
            DockerProxyError::Cache(format!("Failed to write blob {}: {}", digest, e))
        })?;

        // Sync temp file to ensure it's written to disk before rename
        if let Ok(file) = tokio::fs::File::open(&temp_path).await {
            let _ = file.sync_all().await;
        }

        // Ensure parent directory exists right before rename (defensive check)
        // This handles race conditions where eviction or other operations might have removed it
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                DockerProxyError::Cache(format!(
                    "Failed to ensure blob dir exists before rename: {}",
                    e
                ))
            })?;
        }

        fs::rename(&temp_path, &path).await.map_err(|e| {
            DockerProxyError::Cache(format!("Failed to rename blob {}: {}", digest, e))
        })?;

        // Sync parent directory to ensure rename is visible
        if let Some(parent) = path.parent() {
            if let Ok(dir) = tokio::fs::File::open(parent).await {
                let _ = dir.sync_all().await;
            }
        }

        Ok(())
    }

    /// Stream blob from reader
    pub async fn write_blob_stream<R: AsyncRead + Unpin>(
        &self,
        digest: &str,
        mut reader: R,
    ) -> Result<u64> {
        if let CacheBackend::ObjectStore { store, .. } = &self.backend {
            let temp_path = self.blob_upload_temp_path(digest);
            if let Some(parent) = temp_path.parent() {
                fs::create_dir_all(parent).await.map_err(|e| {
                    DockerProxyError::Cache(format!("Failed to create blob dir: {}", e))
                })?;
            }

            let mut file = fs::File::create(&temp_path).await.map_err(|e| {
                DockerProxyError::Cache(format!("Failed to create blob temp file: {}", e))
            })?;
            let mut written = 0u64;
            let mut buffer = vec![0u8; 8192];

            loop {
                let n = reader.read(&mut buffer).await.map_err(|e| {
                    DockerProxyError::Cache(format!("Failed to read blob stream: {}", e))
                })?;
                if n == 0 {
                    break;
                }
                written += n as u64;
                file.write_all(&buffer[..n]).await.map_err(|e| {
                    DockerProxyError::Cache(format!("Failed to write blob temp file: {}", e))
                })?;
            }

            file.sync_all()
                .await
                .map_err(|e| DockerProxyError::Cache(format!("Failed to sync blob: {}", e)))?;
            drop(file);

            let key = self.blob_object_key(digest);
            let upload_result = object_put_file(store.as_ref(), &key, digest, &temp_path).await;
            let _ = fs::remove_file(&temp_path).await;
            upload_result?;
            return Ok(written);
        }

        let path = self.blob_path(digest);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                DockerProxyError::Cache(format!("Failed to create blob dir: {}", e))
            })?;
        }

        let temp_path = path.with_extension("tmp");
        let mut file = fs::File::create(&temp_path)
            .await
            .map_err(|e| DockerProxyError::Cache(format!("Failed to create blob file: {}", e)))?;

        let mut written = 0u64;
        let mut buffer = vec![0u8; 8192];

        loop {
            let n = reader.read(&mut buffer).await.map_err(|e| {
                DockerProxyError::Cache(format!("Failed to read blob stream: {}", e))
            })?;

            if n == 0 {
                break;
            }

            file.write_all(&buffer[..n])
                .await
                .map_err(|e| DockerProxyError::Cache(format!("Failed to write blob: {}", e)))?;
            written += n as u64;
        }

        file.sync_all()
            .await
            .map_err(|e| DockerProxyError::Cache(format!("Failed to sync blob: {}", e)))?;

        // Check if eviction needed after write (approximate check)
        if written > 0 {
            let _ = self.evict_if_needed(written).await;
        }

        fs::rename(&temp_path, &path)
            .await
            .map_err(|e| DockerProxyError::Cache(format!("Failed to rename blob: {}", e)))?;

        Ok(written)
    }

    pub async fn commit_blob_from_temp(&self, digest: &str, temp_path: &Path) -> Result<()> {
        match &self.backend {
            CacheBackend::Filesystem => {
                let path = self.blob_path(digest);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent).await.map_err(|e| {
                        DockerProxyError::Cache(format!("Failed to create blob dir: {}", e))
                    })?;
                }
                fs::rename(temp_path, &path).await.map_err(|e| {
                    DockerProxyError::Cache(format!("Failed to commit blob {}: {}", digest, e))
                })?;
            }
            CacheBackend::ObjectStore { store, .. } => {
                let key = self.blob_object_key(digest);
                object_put_file(store.as_ref(), &key, digest, temp_path).await?;
                let _ = fs::remove_file(temp_path).await;
            }
        }
        Ok(())
    }

    /// Get manifest path for a repository and reference (legacy tag-based)
    /// DEPRECATED: Use manifest_path_by_digest for new code
    pub fn manifest_path(&self, registry: &str, repository: &str, reference: &str) -> PathBuf {
        // Remove "sha256:" prefix if present for digest references
        let ref_clean = reference.strip_prefix("sha256:").unwrap_or(reference);
        let ref_safe = ref_clean.replace(":", "_");

        self.manifests_dir
            .join(registry)
            .join(repository)
            .join(format!("{}.json", ref_safe))
    }

    /// Get manifest path by digest (content-addressable storage)
    /// Path: manifests/{registry}/{repository}/sha256/{digest}.json
    pub fn manifest_path_by_digest(
        &self,
        registry: &str,
        repository: &str,
        digest: &str,
    ) -> PathBuf {
        // Remove "sha256:" prefix if present
        let digest_clean = digest.strip_prefix("sha256:").unwrap_or(digest);

        self.manifests_dir
            .join(registry)
            .join(repository)
            .join("sha256")
            .join(format!("{}.json", digest_clean))
    }

    /// Get tag-to-digest mapping path
    /// Path: manifests/{registry}/{repository}/tags/{tag}.digest
    pub fn tag_digest_mapping_path(&self, registry: &str, repository: &str, tag: &str) -> PathBuf {
        let tag_safe = tag.replace(":", "_");

        self.manifests_dir
            .join(registry)
            .join(repository)
            .join("tags")
            .join(format!("{}.digest", tag_safe))
    }

    /// Read tag-to-digest mapping
    pub async fn read_tag_digest_mapping(
        &self,
        registry: &str,
        repository: &str,
        tag: &str,
    ) -> Result<Option<String>> {
        if let CacheBackend::ObjectStore { store, .. } = &self.backend {
            let key = self.tag_digest_mapping_object_key(registry, repository, tag);
            return match object_get(store.as_ref(), &key).await {
                Ok(digest) => Ok(Some(String::from_utf8_lossy(&digest).trim().to_string())),
                Err(DockerProxyError::Cache(message)) if message.contains("not found") => Ok(None),
                Err(e) => {
                    tracing::warn!(
                        registry = %registry,
                        repository = %repository,
                        tag = %tag,
                        error = %e,
                        "Failed to read S3 tag-to-digest mapping"
                    );
                    Ok(None)
                }
            };
        }

        let path = self.tag_digest_mapping_path(registry, repository, tag);

        if !path.exists() {
            return Ok(None);
        }

        match fs::read_to_string(&path).await {
            Ok(digest) => Ok(Some(digest.trim().to_string())),
            Err(e) => {
                tracing::warn!(
                    registry = %registry,
                    repository = %repository,
                    tag = %tag,
                    error = %e,
                    "Failed to read tag-to-digest mapping"
                );
                Ok(None)
            }
        }
    }

    /// Write tag-to-digest mapping
    pub async fn write_tag_digest_mapping(
        &self,
        registry: &str,
        repository: &str,
        tag: &str,
        digest: &str,
    ) -> Result<()> {
        if let CacheBackend::ObjectStore { store, .. } = &self.backend {
            let key = self.tag_digest_mapping_object_key(registry, repository, tag);
            object_put(
                store.as_ref(),
                &key,
                Bytes::copy_from_slice(digest.as_bytes()),
            )
            .await?;
            tracing::debug!(
                registry = %registry,
                repository = %repository,
                tag = %tag,
                digest = %digest,
                "S3 tag-to-digest mapping written"
            );
            return Ok(());
        }

        let path = self.tag_digest_mapping_path(registry, repository, tag);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                DockerProxyError::Cache(format!("Failed to create tag mapping dir: {}", e))
            })?;
        }

        fs::write(&path, digest).await.map_err(|e| {
            DockerProxyError::Cache(format!(
                "Failed to write tag-to-digest mapping {}/{}:{} -> {}: {}",
                registry, repository, tag, digest, e
            ))
        })?;

        tracing::debug!(
            registry = %registry,
            repository = %repository,
            tag = %tag,
            digest = %digest,
            "Tag-to-digest mapping written"
        );

        Ok(())
    }

    /// Check if manifest exists by digest
    pub async fn manifest_exists_by_digest(
        &self,
        registry: &str,
        repository: &str,
        digest: &str,
    ) -> bool {
        match &self.backend {
            CacheBackend::Filesystem => self
                .manifest_path_by_digest(registry, repository, digest)
                .exists(),
            CacheBackend::ObjectStore { store, .. } => {
                let key = self.manifest_object_key_by_digest(registry, repository, digest);
                object_head(store.as_ref(), &key).await.is_ok()
            }
        }
    }

    /// Read manifest content by digest
    pub async fn read_manifest_by_digest(
        &self,
        registry: &str,
        repository: &str,
        digest: &str,
    ) -> Result<Vec<u8>> {
        if let CacheBackend::ObjectStore { store, .. } = &self.backend {
            let key = self.manifest_object_key_by_digest(registry, repository, digest);
            return object_get(store.as_ref(), &key).await;
        }

        let path = self.manifest_path_by_digest(registry, repository, digest);
        fs::read(&path).await.map_err(|e| {
            DockerProxyError::Cache(format!(
                "Failed to read manifest {}/{}:{}: {}",
                registry, repository, digest, e
            ))
        })
    }

    /// Write manifest content by digest (content-addressable)
    pub async fn write_manifest_by_digest(
        &self,
        registry: &str,
        repository: &str,
        digest: &str,
        data: &[u8],
    ) -> Result<()> {
        if let CacheBackend::ObjectStore { store, .. } = &self.backend {
            let key = self.manifest_object_key_by_digest(registry, repository, digest);
            object_put(store.as_ref(), &key, Bytes::copy_from_slice(data)).await?;
            tracing::debug!(
                key = %key,
                "Manifest written to S3 cache successfully by digest"
            );
            return Ok(());
        }

        let path = self.manifest_path_by_digest(registry, repository, digest);

        tracing::debug!(
            registry = %registry,
            repository = %repository,
            digest = %digest,
            path = %path.display(),
            data_len = data.len(),
            "Writing manifest to cache by digest"
        );

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                tracing::error!(
                    path = %parent.display(),
                    error = %e,
                    "Failed to create manifest directory"
                );
                DockerProxyError::Cache(format!("Failed to create manifest dir: {}", e))
            })?;
        }

        // Atomic write: write to temp file, then rename
        let temp_path = path.with_extension("tmp");
        fs::write(&temp_path, data).await.map_err(|e| {
            tracing::error!(
                path = %temp_path.display(),
                error = %e,
                "Failed to write manifest file"
            );
            DockerProxyError::Cache(format!(
                "Failed to write manifest {}/{}:{}: {}",
                registry, repository, digest, e
            ))
        })?;

        fs::rename(&temp_path, &path).await.map_err(|e| {
            tracing::error!(
                path = %path.display(),
                error = %e,
                "Failed to rename manifest file"
            );
            DockerProxyError::Cache(format!(
                "Failed to rename manifest {}/{}:{}: {}",
                registry, repository, digest, e
            ))
        })?;

        // Ensure the file is synced to disk
        if let Ok(file) = tokio::fs::File::open(&path).await {
            let _ = file.sync_all().await;
        }

        tracing::debug!(
            path = %path.display(),
            "Manifest written to cache successfully by digest"
        );

        Ok(())
    }

    /// Check if manifest exists
    pub async fn manifest_exists(&self, registry: &str, repository: &str, reference: &str) -> bool {
        match &self.backend {
            CacheBackend::Filesystem => {
                self.manifest_path(registry, repository, reference).exists()
            }
            CacheBackend::ObjectStore { store, .. } => {
                let key = self.manifest_object_key(registry, repository, reference);
                object_head(store.as_ref(), &key).await.is_ok()
            }
        }
    }

    /// Read manifest content
    pub async fn read_manifest(
        &self,
        registry: &str,
        repository: &str,
        reference: &str,
    ) -> Result<Vec<u8>> {
        if let CacheBackend::ObjectStore { store, .. } = &self.backend {
            let key = self.manifest_object_key(registry, repository, reference);
            return object_get(store.as_ref(), &key).await;
        }

        let path = self.manifest_path(registry, repository, reference);
        fs::read(&path).await.map_err(|e| {
            DockerProxyError::Cache(format!(
                "Failed to read manifest {}/{}:{}: {}",
                registry, repository, reference, e
            ))
        })
    }

    /// Delete manifest from cache
    pub async fn delete_manifest(
        &self,
        registry: &str,
        repository: &str,
        reference: &str,
    ) -> Result<()> {
        if let CacheBackend::ObjectStore { store, .. } = &self.backend {
            let key = self.manifest_object_key(registry, repository, reference);
            return object_delete(store.as_ref(), &key).await;
        }

        let path = self.manifest_path(registry, repository, reference);
        fs::remove_file(&path).await.map_err(|e| {
            DockerProxyError::Cache(format!(
                "Failed to delete manifest {}/{}:{}: {}",
                registry, repository, reference, e
            ))
        })
    }

    /// Write manifest content
    pub async fn write_manifest(
        &self,
        registry: &str,
        repository: &str,
        reference: &str,
        data: &[u8],
    ) -> Result<()> {
        if let CacheBackend::ObjectStore { store, .. } = &self.backend {
            let key = self.manifest_object_key(registry, repository, reference);
            object_put(store.as_ref(), &key, Bytes::copy_from_slice(data)).await?;
            tracing::debug!(
                key = %key,
                "Manifest written to S3 cache successfully"
            );
            return Ok(());
        }

        let path = self.manifest_path(registry, repository, reference);

        tracing::debug!(
            registry = %registry,
            repository = %repository,
            reference = %reference,
            path = %path.display(),
            data_len = data.len(),
            "Writing manifest to cache"
        );

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                tracing::error!(
                    path = %parent.display(),
                    error = %e,
                    "Failed to create manifest directory"
                );
                DockerProxyError::Cache(format!("Failed to create manifest dir: {}", e))
            })?;
        }

        fs::write(&path, data).await.map_err(|e| {
            tracing::error!(
                path = %path.display(),
                error = %e,
                "Failed to write manifest file"
            );
            DockerProxyError::Cache(format!(
                "Failed to write manifest {}/{}:{}: {}",
                registry, repository, reference, e
            ))
        })?;

        // Ensure the file is synced to disk
        if let Ok(file) = tokio::fs::File::open(&path).await {
            let _ = file.sync_all().await;
        }

        tracing::debug!(
            path = %path.display(),
            "Manifest written to cache successfully"
        );

        Ok(())
    }

    /// Get base directory
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Get chart path for a repo and chart filename
    /// Path: charts/{repo}/{chart-filename}
    pub fn chart_path(&self, repo: &str, chart: &str) -> PathBuf {
        self.charts_dir.join(repo).join(chart)
    }

    /// Check if chart exists in cache
    pub async fn chart_exists(&self, repo: &str, chart: &str) -> bool {
        match &self.backend {
            CacheBackend::Filesystem => self.chart_path(repo, chart).exists(),
            CacheBackend::ObjectStore { store, .. } => {
                let key = self.chart_object_key(repo, chart);
                object_head(store.as_ref(), &key).await.is_ok()
            }
        }
    }

    /// Read chart from cache
    pub async fn read_chart(&self, repo: &str, chart: &str) -> Result<Vec<u8>> {
        if let CacheBackend::ObjectStore { store, .. } = &self.backend {
            let key = self.chart_object_key(repo, chart);
            return object_get(store.as_ref(), &key).await;
        }

        let path = self.chart_path(repo, chart);
        fs::read(&path).await.map_err(|e| {
            DockerProxyError::Cache(format!("Failed to read chart {}/{}: {}", repo, chart, e))
        })
    }

    /// Write chart to cache (atomic write)
    pub async fn write_chart(&self, repo: &str, chart: &str, data: &[u8]) -> Result<()> {
        if let CacheBackend::ObjectStore { store, .. } = &self.backend {
            let key = self.chart_object_key(repo, chart);
            object_put(store.as_ref(), &key, Bytes::copy_from_slice(data)).await?;
            tracing::debug!(
                key = %key,
                "Helm chart written to S3 cache successfully"
            );
            return Ok(());
        }

        let path = self.chart_path(repo, chart);

        tracing::debug!(
            repo = %repo,
            chart = %chart,
            path = %path.display(),
            data_len = data.len(),
            "Writing Helm chart to cache"
        );

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                tracing::error!(
                    path = %parent.display(),
                    error = %e,
                    "Failed to create chart directory"
                );
                DockerProxyError::Cache(format!("Failed to create chart dir: {}", e))
            })?;
        }

        // Atomic write: write to temp file, then rename
        let temp_path = path.with_extension("tmp");
        fs::write(&temp_path, data).await.map_err(|e| {
            tracing::error!(
                path = %temp_path.display(),
                error = %e,
                "Failed to write chart file"
            );
            DockerProxyError::Cache(format!("Failed to write chart {}/{}: {}", repo, chart, e))
        })?;

        // Sync temp file to ensure it's written to disk before rename
        if let Ok(file) = tokio::fs::File::open(&temp_path).await {
            let _ = file.sync_all().await;
        }

        fs::rename(&temp_path, &path).await.map_err(|e| {
            tracing::error!(
                path = %path.display(),
                error = %e,
                "Failed to rename chart file"
            );
            DockerProxyError::Cache(format!("Failed to rename chart {}/{}: {}", repo, chart, e))
        })?;

        // Sync parent directory to ensure rename is visible
        if let Some(parent) = path.parent() {
            if let Ok(dir) = tokio::fs::File::open(parent).await {
                let _ = dir.sync_all().await;
            }
        }

        tracing::debug!(
            path = %path.display(),
            "Helm chart written to cache successfully"
        );

        Ok(())
    }

    /// Calculate total cache size in bytes
    async fn calculate_cache_size(&self) -> u64 {
        let mut total_size = 0u64;
        if let Ok(entries) = std::fs::read_dir(&self.blobs_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Ok(metadata) = std::fs::metadata(&path) {
                        total_size += metadata.len();
                    }
                }
            }
        }
        total_size
    }

    /// Evict blobs if cache size exceeds limit (LRU: evict oldest accessed files first)
    async fn evict_if_needed(&self, new_blob_size: u64) -> Result<()> {
        // Skip eviction if no limit set
        if self.max_size_bytes == u64::MAX {
            return Ok(());
        }

        let current_size = self.calculate_cache_size().await;

        // Check if adding this blob would exceed the limit
        if current_size + new_blob_size <= self.max_size_bytes {
            return Ok(());
        }

        // Collect blob metadata (path, size, modified time)
        let mut blobs: Vec<(PathBuf, u64, std::time::SystemTime)> = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.blobs_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Ok(metadata) = std::fs::metadata(&path) {
                        if let Ok(modified) = metadata.modified() {
                            blobs.push((path, metadata.len(), modified));
                        }
                    }
                }
            }
        }

        // Sort by modification time (oldest first) for LRU eviction
        blobs.sort_by_key(|(_, _, modified)| *modified);

        // Evict oldest blobs until we have enough space
        let mut freed_size = 0u64;
        let target_free = (current_size + new_blob_size).saturating_sub(self.max_size_bytes);

        for (path, size, _) in blobs {
            if freed_size >= target_free {
                break;
            }

            if let Err(e) = std::fs::remove_file(&path) {
                tracing::warn!("Failed to evict blob {:?}: {}", path, e);
            } else {
                freed_size += size;
                tracing::debug!("Evicted blob {:?} ({} bytes)", path, size);
            }
        }

        if freed_size > 0 {
            tracing::info!(
                "Cache eviction: freed {} bytes (target: {} bytes)",
                freed_size,
                target_free
            );
        }

        Ok(())
    }

    fn object_key<'a>(&self, parts: impl IntoIterator<Item = &'a str>) -> String {
        let mut segments: Vec<String> = Vec::new();
        if let CacheBackend::ObjectStore { prefix, .. } = &self.backend {
            if !prefix.is_empty() {
                segments.push(prefix.clone());
            }
        }
        segments.extend(
            parts
                .into_iter()
                .filter(|part| !part.is_empty())
                .map(str::to_string),
        );
        segments.join("/")
    }

    fn blob_upload_temp_path(&self, digest: &str) -> PathBuf {
        let digest = digest.strip_prefix("sha256:").unwrap_or(digest);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        self.blobs_dir.join(format!(
            "{}.{}.{}.upload.tmp",
            digest,
            std::process::id(),
            timestamp
        ))
    }
}

fn normalize_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_string()
}

async fn object_head(
    store: &dyn ObjectStore,
    key: &str,
) -> std::result::Result<object_store::ObjectMeta, object_store::Error> {
    store.head(&ObjectPath::from(key.to_string())).await
}

async fn object_get(store: &dyn ObjectStore, key: &str) -> Result<Vec<u8>> {
    store
        .get(&ObjectPath::from(key.to_string()))
        .await
        .map_err(|e| object_error("read", key, e))?
        .bytes()
        .await
        .map(|bytes| bytes.to_vec())
        .map_err(|e| object_error("read", key, e))
}

async fn object_get_range(
    store: &dyn ObjectStore,
    key: &str,
    start: u64,
    end: u64,
) -> Result<Vec<u8>> {
    store
        .get_range(&ObjectPath::from(key.to_string()), start..end)
        .await
        .map(|bytes| bytes.to_vec())
        .map_err(|e| object_error("read range", key, e))
}

async fn object_put(store: &dyn ObjectStore, key: &str, data: Bytes) -> Result<()> {
    store
        .put(&ObjectPath::from(key.to_string()), PutPayload::from(data))
        .await
        .map(|_| ())
        .map_err(|e| object_error("write", key, e))
}

async fn object_put_file(
    store: &dyn ObjectStore,
    key: &str,
    digest: &str,
    file_path: &Path,
) -> Result<u64> {
    let size = fs::metadata(file_path)
        .await
        .map_err(|e| {
            DockerProxyError::Cache(format!(
                "Failed to stat verified blob temp file {}: {}",
                file_path.display(),
                e
            ))
        })?
        .len();

    if size < S3_MULTIPART_MIN_PART_SIZE {
        let data = fs::read(file_path).await.map_err(|e| {
            DockerProxyError::Cache(format!(
                "Failed to read verified blob temp file {}: {}",
                file_path.display(),
                e
            ))
        })?;
        object_put(store, key, Bytes::from(data)).await?;
        return Ok(size);
    }

    let path = ObjectPath::from(key.to_string());
    let mut upload = store.put_multipart(&path).await.map_err(|e| {
        DockerProxyError::Cache(format!("Failed to start S3 blob upload {}: {}", digest, e))
    })?;
    let mut file = fs::File::open(file_path).await.map_err(|e| {
        DockerProxyError::Cache(format!(
            "Failed to open verified blob temp file {}: {}",
            file_path.display(),
            e
        ))
    })?;
    let mut written = 0u64;
    let mut buffer = vec![0u8; S3_MULTIPART_CHUNK_SIZE];

    loop {
        let n = file.read(&mut buffer).await.map_err(|e| {
            DockerProxyError::Cache(format!(
                "Failed to read verified blob temp file {}: {}",
                file_path.display(),
                e
            ))
        })?;
        if n == 0 {
            break;
        }

        written += n as u64;
        if let Err(e) = upload
            .put_part(Bytes::copy_from_slice(&buffer[..n]).into())
            .await
        {
            let _ = upload.abort().await;
            return Err(DockerProxyError::Cache(format!(
                "Failed to upload S3 blob part {}: {}",
                digest, e
            )));
        }
    }

    if written != size {
        let _ = upload.abort().await;
        return Err(DockerProxyError::Cache(format!(
            "Failed to upload S3 blob {}: expected {} bytes but read {}",
            digest, size, written
        )));
    }

    if let Err(e) = upload.complete().await {
        let _ = upload.abort().await;
        return Err(DockerProxyError::Cache(format!(
            "Failed to complete S3 blob {}: {}",
            digest, e
        )));
    }

    Ok(written)
}

async fn object_delete(store: &dyn ObjectStore, key: &str) -> Result<()> {
    store
        .delete(&ObjectPath::from(key.to_string()))
        .await
        .map_err(|e| object_error("delete", key, e))
}

fn object_error(action: &str, key: &str, error: object_store::Error) -> DockerProxyError {
    match error {
        object_store::Error::NotFound { .. } => {
            DockerProxyError::Cache(format!("S3 object not found during {}: {}", action, key))
        }
        other => {
            DockerProxyError::Cache(format!("Failed to {} S3 object {}: {}", action, key, other))
        }
    }
}
