//! Tests for the S3-compatible cache backend used by MinIO deployments.

use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use pulld::cache::CacheStorage;
use std::fmt::{Debug, Display, Formatter};
use std::io;
use std::sync::Arc;

#[derive(Debug)]
struct FailingPutStore {
    inner: InMemory,
}

impl Display for FailingPutStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "failing-put-store")
    }
}

#[async_trait::async_trait]
impl ObjectStore for FailingPutStore {
    async fn put_opts(
        &self,
        _location: &ObjectPath,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(object_store::Error::Generic {
            store: "failing-put-store",
            source: Box::new(io::Error::other("simulated upload failure")),
        })
    }

    async fn put_multipart_opts(
        &self,
        _location: &ObjectPath,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(object_store::Error::Generic {
            store: "failing-put-store",
            source: Box::new(io::Error::other("simulated multipart failure")),
        })
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn memory_s3_cache(prefix: &str) -> CacheStorage {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    CacheStorage::with_object_store(
        temp_dir.path().to_path_buf(),
        Arc::new(InMemory::new()) as Arc<dyn ObjectStore>,
        prefix.to_string(),
        Some(1),
    )
    .expect("cache storage")
}

#[tokio::test]
async fn s3_backend_uses_stable_content_addressed_keys() {
    let cache = memory_s3_cache("pulld-cache");

    assert_eq!(
        cache.blob_object_key("sha256:abc123"),
        "pulld-cache/blobs/sha256/abc123"
    );
    assert_eq!(
        cache.manifest_object_key_by_digest("docker.io", "library/alpine", "sha256:def456"),
        "pulld-cache/manifests/docker.io/library/alpine/sha256/def456.json"
    );
    assert_eq!(
        cache.tag_digest_mapping_object_key("docker.io", "library/alpine", "3.20"),
        "pulld-cache/manifests/docker.io/library/alpine/tags/3.20.digest"
    );
    assert_eq!(
        cache.chart_object_key("cilium", "cilium-1.17.7.tgz"),
        "pulld-cache/charts/cilium/cilium-1.17.7.tgz"
    );
}

#[tokio::test]
async fn s3_backend_supports_blob_hit_miss_size_and_range_reads() {
    let cache = memory_s3_cache("");
    let digest = "sha256:abcdef";

    assert!(!cache.blob_exists(digest).await);
    assert_eq!(cache.blob_size(digest).await, None);

    cache.write_blob(digest, b"0123456789").await.unwrap();

    assert!(cache.blob_exists(digest).await);
    assert_eq!(cache.blob_size(digest).await, Some(10));
    assert_eq!(cache.read_blob(digest).await.unwrap(), b"0123456789");
    assert_eq!(cache.read_blob_range(digest, 4).await.unwrap(), b"456789");
}

#[tokio::test]
async fn s3_backend_supports_manifest_tag_mapping_and_chart_cache() {
    let cache = memory_s3_cache("");

    cache
        .write_manifest_by_digest(
            "docker.io",
            "library/alpine",
            "sha256:manifest",
            br#"{"schemaVersion":2}"#,
        )
        .await
        .unwrap();
    cache
        .write_tag_digest_mapping("docker.io", "library/alpine", "latest", "sha256:manifest")
        .await
        .unwrap();
    cache
        .write_chart("cilium", "cilium-1.17.7.tgz", b"chart-data")
        .await
        .unwrap();

    assert!(
        cache
            .manifest_exists_by_digest("docker.io", "library/alpine", "sha256:manifest")
            .await
    );
    assert_eq!(
        cache
            .read_manifest_by_digest("docker.io", "library/alpine", "sha256:manifest")
            .await
            .unwrap(),
        br#"{"schemaVersion":2}"#
    );
    assert_eq!(
        cache
            .read_tag_digest_mapping("docker.io", "library/alpine", "latest")
            .await
            .unwrap(),
        Some("sha256:manifest".to_string())
    );
    assert!(cache.chart_exists("cilium", "cilium-1.17.7.tgz").await);
    assert_eq!(
        cache
            .read_chart("cilium", "cilium-1.17.7.tgz")
            .await
            .unwrap(),
        b"chart-data"
    );
}

#[tokio::test]
async fn s3_backend_surfaces_upload_errors() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let cache = CacheStorage::with_object_store(
        temp_dir.path().to_path_buf(),
        Arc::new(FailingPutStore {
            inner: InMemory::new(),
        }) as Arc<dyn ObjectStore>,
        String::new(),
        Some(1),
    )
    .expect("cache storage");

    let err = cache
        .write_blob("sha256:uploadfailure", b"data")
        .await
        .unwrap_err();

    assert!(err.to_string().contains("Failed to write S3 object"));
    assert!(!cache.blob_exists("sha256:uploadfailure").await);
}

#[tokio::test]
async fn filesystem_backend_still_works_without_s3_configuration() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let cache =
        CacheStorage::with_max_size(temp_dir.path().to_path_buf(), Some(1)).expect("cache storage");

    assert!(!cache.is_object_store_backed());

    cache
        .write_blob("sha256:filesystem", b"local")
        .await
        .unwrap();

    assert!(cache.blob_exists("sha256:filesystem").await);
    assert_eq!(
        cache.read_blob("sha256:filesystem").await.unwrap(),
        b"local"
    );
}
