//! Acceptance tests: drive a real running soma-server (over a TCP socket) with
//! the independent `object_store` S3 client — validating SigV4 and the S3 wire
//! protocol against a third-party implementation — plus full-stack restart
//! recovery.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::Path;
use std::sync::Arc;

use futures::StreamExt;
use object_store::aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path as OPath;
use object_store::{MultipartUpload, ObjectStore, PutMode, PutOptions, PutPayload};
use tempfile::TempDir;
use tokio::task::JoinHandle;

use soma_backend::{BackendConfig, CachingBackend, LocalFsBackend, StorageBackend};
use soma_meta::{BucketOpts, MetadataStore, RedbMetaStore};
use soma_s3::{router, Credentials, S3Service};

const BUCKET: &str = "testbucket";

/// Open the stores at `dir` and serve on an ephemeral port; returns the port and
/// the server task handle. Optionally creates the test bucket first.
async fn serve(dir: &Path, create_bucket: bool) -> (u16, JoinHandle<()>) {
    let meta = Arc::new(RedbMetaStore::open(dir.join("meta.redb")).unwrap());
    if create_bucket {
        meta.create_bucket(BUCKET, BucketOpts::default()).unwrap();
    }
    let backend = Arc::new(LocalFsBackend::open(dir, BackendConfig::default()).unwrap());
    let meta: Arc<dyn MetadataStore> = meta;
    let backend: Arc<dyn StorageBackend> = backend;
    let svc = S3Service::new(meta, backend, Credentials::single("AK", "SK"));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, router(svc)).await;
    });
    (port, handle)
}

/// Like [`serve`], but the storage backend is wrapped in the in-memory cache, so
/// the full S3 stack runs through `CachingBackend`.
async fn serve_cached(dir: &Path, create_bucket: bool) -> (u16, JoinHandle<()>) {
    let meta = Arc::new(RedbMetaStore::open(dir.join("meta.redb")).unwrap());
    if create_bucket {
        meta.create_bucket(BUCKET, BucketOpts::default()).unwrap();
    }
    let fs = Arc::new(LocalFsBackend::open(dir, BackendConfig::default()).unwrap());
    let meta: Arc<dyn MetadataStore> = meta;
    let backend: Arc<dyn StorageBackend> =
        Arc::new(CachingBackend::new(fs, 16 * 1024 * 1024, 1024 * 1024));
    let svc = S3Service::new(meta, backend, Credentials::single("AK", "SK"));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, router(svc)).await;
    });
    (port, handle)
}

/// Stop a server task and wait for it to fully drop (releasing the redb file).
async fn stop(handle: JoinHandle<()>) {
    handle.abort();
    let _ = handle.await;
}

fn client(port: u16) -> AmazonS3 {
    AmazonS3Builder::new()
        .with_endpoint(format!("http://127.0.0.1:{port}"))
        .with_region("us-east-1")
        .with_bucket_name(BUCKET)
        .with_access_key_id("AK")
        .with_secret_access_key("SK")
        .with_allow_http(true)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()
        .unwrap()
}

#[tokio::test]
async fn object_store_crud_roundtrip() {
    let dir = TempDir::new().unwrap();
    let (port, handle) = serve(dir.path(), true).await;
    let store = client(port);

    // PUT + GET.
    let key = OPath::from("docs/greeting.txt");
    let payload = b"hello soma via object_store";
    store
        .put(&key, PutPayload::from_static(payload))
        .await
        .unwrap();
    let got = store.get(&key).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), payload);

    // HEAD (size + etag).
    let meta = store.head(&key).await.unwrap();
    assert_eq!(meta.size, payload.len() as u64);
    assert!(meta.e_tag.is_some());

    // Range GET.
    let part = store.get_range(&key, 6u64..10).await.unwrap();
    assert_eq!(part.as_ref(), b"soma");

    // LIST by prefix.
    store
        .put(
            &OPath::from("docs/other.txt"),
            PutPayload::from_static(b"x"),
        )
        .await
        .unwrap();
    store
        .put(&OPath::from("root.txt"), PutPayload::from_static(b"y"))
        .await
        .unwrap();
    let mut keys: Vec<String> = store
        .list(Some(&OPath::from("docs")))
        .map(|r| r.unwrap().location.to_string())
        .collect::<Vec<_>>()
        .await;
    keys.sort();
    assert_eq!(keys, vec!["docs/greeting.txt", "docs/other.txt"]);

    // DELETE → subsequent GET is NotFound.
    store.delete(&key).await.unwrap();
    assert!(matches!(
        store.get(&key).await,
        Err(object_store::Error::NotFound { .. })
    ));

    stop(handle).await;
}

#[tokio::test]
async fn object_store_conditional_create() {
    let dir = TempDir::new().unwrap();
    let (port, handle) = serve(dir.path(), true).await;
    let store = client(port);

    let key = OPath::from("once.txt");
    let create = || PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };

    // First create succeeds; second fails (AlreadyExists).
    store
        .put_opts(&key, PutPayload::from_static(b"v1"), create())
        .await
        .unwrap();
    let second = store
        .put_opts(&key, PutPayload::from_static(b"v2"), create())
        .await;
    assert!(matches!(
        second,
        Err(object_store::Error::AlreadyExists { .. })
    ));

    stop(handle).await;
}

#[tokio::test]
async fn object_store_multipart() {
    let dir = TempDir::new().unwrap();
    let (port, handle) = serve(dir.path(), true).await;
    let store = client(port);

    let key = OPath::from("big.bin");
    let mut upload = store.put_multipart(&key).await.unwrap();
    let part1 = vec![1u8; 6 * 1024 * 1024]; // >5 MiB so object_store flushes a part
    let part2 = vec![2u8; 1024];
    upload
        .put_part(PutPayload::from(part1.clone()))
        .await
        .unwrap();
    upload
        .put_part(PutPayload::from(part2.clone()))
        .await
        .unwrap();
    upload.complete().await.unwrap();

    let got = store.get(&key).await.unwrap().bytes().await.unwrap();
    let mut expected = part1;
    expected.extend_from_slice(&part2);
    assert_eq!(got.len(), expected.len());
    assert_eq!(got.as_ref(), expected.as_slice());

    stop(handle).await;
}

#[tokio::test]
async fn object_store_roundtrip_through_cache() {
    let dir = TempDir::new().unwrap();
    let (port, handle) = serve_cached(dir.path(), true).await;
    let store = client(port);

    let key = OPath::from("cached.txt");
    let payload = b"served from the cache on the second read";
    store
        .put(&key, PutPayload::from_static(payload))
        .await
        .unwrap();

    // Two reads through the caching backend return identical bytes; the second
    // is served from memory.
    assert_eq!(
        store
            .get(&key)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        payload
    );
    assert_eq!(
        store
            .get(&key)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        payload
    );
    // A range read of the cached small object is also correct.
    assert_eq!(
        store.get_range(&key, 7u64..11).await.unwrap().as_ref(),
        &payload[7..11] // "from"
    );

    stop(handle).await;
}

/// A free local TCP port (bound then released; small race, fine for tests).
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Drive the full split topology: a metadata gRPC node + a storage gRPC node +
/// a gateway (MetaClient/StorageClient) serving S3, exercised by object_store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn split_topology_object_store_roundtrip() {
    use soma_cluster::{serve_meta, serve_storage, MetaClient, StorageClient};
    use std::time::Duration;

    let dir = TempDir::new().unwrap();
    let meta_port = free_port();
    let storage_port = free_port();

    // Metadata node (create the bucket on the shared store before serving).
    let meta_store: Arc<dyn MetadataStore> =
        Arc::new(RedbMetaStore::open(dir.path().join("meta.redb")).unwrap());
    meta_store
        .create_bucket(BUCKET, BucketOpts::default())
        .unwrap();
    let ms = meta_store.clone();
    tokio::spawn(async move {
        let _ = serve_meta(format!("127.0.0.1:{meta_port}").parse().unwrap(), ms).await;
    });

    // Storage node.
    let backend: Arc<dyn StorageBackend> =
        Arc::new(LocalFsBackend::open(dir.path(), BackendConfig::default()).unwrap());
    let sb = backend.clone();
    tokio::spawn(async move {
        let _ = serve_storage(format!("127.0.0.1:{storage_port}").parse().unwrap(), sb).await;
    });

    // Let the gRPC servers bind.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Gateway: remote clients behind the traits.
    let meta: Arc<dyn MetadataStore> = Arc::new(
        MetaClient::connect(format!("http://127.0.0.1:{meta_port}"))
            .await
            .unwrap(),
    );
    let storage: Arc<dyn StorageBackend> = Arc::new(
        StorageClient::connect(format!("http://127.0.0.1:{storage_port}"))
            .await
            .unwrap(),
    );
    let svc = S3Service::new(meta, storage, Credentials::single("AK", "SK"));
    let app = router(svc);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let s3_port = listener.local_addr().unwrap().port();
    let s3_handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    // PUT / GET / list / range / delete through gateway → gRPC → meta + storage.
    let store = client(s3_port);
    let key = OPath::from("split/object.bin");
    let payload = b"hello through the split cluster over gRPC";
    store
        .put(&key, PutPayload::from_static(payload))
        .await
        .unwrap();
    assert_eq!(
        store
            .get(&key)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        payload
    );
    assert_eq!(
        store.get_range(&key, 6u64..13).await.unwrap().as_ref(),
        &payload[6..13]
    );
    let keys: Vec<String> = store
        .list(Some(&OPath::from("split")))
        .map(|r| r.unwrap().location.to_string())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(keys, vec!["split/object.bin"]);

    store.delete(&key).await.unwrap();
    assert!(matches!(
        store.get(&key).await,
        Err(object_store::Error::NotFound { .. })
    ));

    s3_handle.abort();
}

#[tokio::test]
async fn full_stack_restart_persists_data() {
    let dir = TempDir::new().unwrap();

    // First boot: write an object, then stop the server.
    let key = OPath::from("durable/object.dat");
    let payload = b"survive the restart";
    {
        let (port, handle) = serve(dir.path(), true).await;
        let store = client(port);
        store
            .put(&key, PutPayload::from_static(payload))
            .await
            .unwrap();
        store.head(&key).await.unwrap(); // committed
        stop(handle).await;
    }

    // Second boot on the SAME data dir (bucket already exists): the object is
    // still readable — metadata persisted (redb) and bytes recovered from the
    // volume + .idx.
    {
        let (port, handle) = serve(dir.path(), false).await;
        let store = client(port);
        let got = store.get(&key).await.unwrap().bytes().await.unwrap();
        assert_eq!(got.as_ref(), payload);
        stop(handle).await;
    }
}
