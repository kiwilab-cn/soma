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

use soma_backend::{
    BackendConfig, CachingBackend, EncryptingBackend, LocalFsBackend, StaticKeyProvider,
    StorageBackend,
};
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

/// Like [`serve`], but the storage backend is wrapped in envelope encryption, so
/// the full S3 stack runs through `EncryptingBackend` and bytes at rest are sealed.
async fn serve_encrypted(dir: &Path, create_bucket: bool) -> (u16, JoinHandle<()>) {
    let meta = Arc::new(RedbMetaStore::open(dir.join("meta.redb")).unwrap());
    if create_bucket {
        meta.create_bucket(BUCKET, BucketOpts::default()).unwrap();
    }
    let fs = Arc::new(LocalFsBackend::open(dir, BackendConfig::default()).unwrap());
    let keys = StaticKeyProvider::new([42u8; 32]);
    let meta: Arc<dyn MetadataStore> = meta;
    let backend: Arc<dyn StorageBackend> = Arc::new(EncryptingBackend::new(fs, &keys));
    let svc = S3Service::new(meta, backend, Credentials::single("AK", "SK"));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, router(svc)).await;
    });
    (port, handle)
}

/// True if any regular file directly under `dir` contains `needle` verbatim.
fn dir_contains_bytes(dir: &Path, needle: &[u8]) -> bool {
    for entry in std::fs::read_dir(dir).unwrap() {
        let p = entry.unwrap().path();
        if p.is_file() {
            let bytes = std::fs::read(&p).unwrap();
            if bytes.windows(needle.len()).any(|w| w == needle) {
                return true;
            }
        }
    }
    false
}

/// Serve the full S3 stack with a multi-tenant QoS policy attached.
async fn serve_with_qos(dir: &Path, qos: soma_s3::QosPolicy) -> (u16, JoinHandle<()>) {
    let meta = Arc::new(RedbMetaStore::open(dir.join("meta.redb")).unwrap());
    meta.create_bucket(BUCKET, BucketOpts::default()).unwrap();
    let fs = Arc::new(LocalFsBackend::open(dir, BackendConfig::default()).unwrap());
    let meta: Arc<dyn MetadataStore> = meta;
    let backend: Arc<dyn StorageBackend> = fs;
    let svc = S3Service::new(meta, backend, Credentials::single("AK", "SK")).with_qos(qos);

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

// --- replication fault-injection (M2b) -------------------------------------

struct Cluster {
    _dir: TempDir,
    s3_port: u16,
    storage: Vec<JoinHandle<()>>,
    _meta: JoinHandle<()>,
    _s3: JoinHandle<()>,
}

fn sock(port: u16) -> std::net::SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

/// Stand up a cluster: 1 metadata node, `num_storage` storage nodes, and a
/// gateway with a quorum-replicated backend (`rf`/`wq`). Storage tasks are
/// returned so tests can "kill" a node by aborting it.
async fn cluster(num_storage: usize, rf: usize, wq: usize) -> Cluster {
    use soma_cluster::{serve_meta, serve_storage, MetaClient, ReplicatedBackend};

    let dir = TempDir::new().unwrap();

    let meta_port = free_port();
    let meta_store: Arc<dyn MetadataStore> =
        Arc::new(RedbMetaStore::open(dir.path().join("meta.redb")).unwrap());
    meta_store
        .create_bucket(BUCKET, BucketOpts::default())
        .unwrap();
    let ms = meta_store.clone();
    let meta_handle = tokio::spawn(async move {
        let _ = serve_meta(sock(meta_port), ms).await;
    });

    let mut storage = Vec::new();
    let mut endpoints = Vec::new();
    for i in 0..num_storage {
        let port = free_port();
        let sdir = dir.path().join(format!("storage-{i}"));
        std::fs::create_dir_all(&sdir).unwrap();
        let backend: Arc<dyn StorageBackend> =
            Arc::new(LocalFsBackend::open(&sdir, BackendConfig::default()).unwrap());
        storage.push(tokio::spawn(async move {
            let _ = serve_storage(sock(port), backend).await;
        }));
        endpoints.push(format!("http://127.0.0.1:{port}"));
    }

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let meta: Arc<dyn MetadataStore> = Arc::new(
        MetaClient::connect(format!("http://127.0.0.1:{meta_port}"))
            .await
            .unwrap(),
    );
    let repl: Arc<dyn StorageBackend> =
        Arc::new(ReplicatedBackend::connect(endpoints, rf, wq).await.unwrap());
    let svc = S3Service::new(meta, repl, Credentials::single("AK", "SK"));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let s3_port = listener.local_addr().unwrap().port();
    let s3 = tokio::spawn(async move {
        let _ = axum::serve(listener, router(svc)).await;
    });

    Cluster {
        _dir: dir,
        s3_port,
        storage,
        _meta: meta_handle,
        _s3: s3,
    }
}

async fn settle() {
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
}

/// Stand up an erasure-coded cluster: 1 metadata node, `k + m` storage nodes, and
/// a gateway whose backend stripes objects with Reed-Solomon `k+m`. Storage tasks
/// are returned so a test can "kill" a node by aborting it.
async fn ec_cluster(k: usize, m: usize) -> Cluster {
    use soma_cluster::{serve_meta, serve_storage, ErasureCodedBackend, MetaClient};

    let dir = TempDir::new().unwrap();

    let meta_port = free_port();
    let meta_store: Arc<dyn MetadataStore> =
        Arc::new(RedbMetaStore::open(dir.path().join("meta.redb")).unwrap());
    meta_store
        .create_bucket(BUCKET, BucketOpts::default())
        .unwrap();
    let ms = meta_store.clone();
    let meta_handle = tokio::spawn(async move {
        let _ = serve_meta(sock(meta_port), ms).await;
    });

    let mut storage = Vec::new();
    let mut endpoints = Vec::new();
    for i in 0..(k + m) {
        let port = free_port();
        let sdir = dir.path().join(format!("storage-{i}"));
        std::fs::create_dir_all(&sdir).unwrap();
        let backend: Arc<dyn StorageBackend> =
            Arc::new(LocalFsBackend::open(&sdir, BackendConfig::default()).unwrap());
        storage.push(tokio::spawn(async move {
            let _ = serve_storage(sock(port), backend).await;
        }));
        endpoints.push(format!("http://127.0.0.1:{port}"));
    }

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let meta: Arc<dyn MetadataStore> = Arc::new(
        MetaClient::connect(format!("http://127.0.0.1:{meta_port}"))
            .await
            .unwrap(),
    );
    let ec: Arc<dyn StorageBackend> = Arc::new(
        ErasureCodedBackend::connect(endpoints, k, m, 0)
            .await
            .unwrap(),
    );
    let svc = S3Service::new(meta, ec, Credentials::single("AK", "SK"));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let s3_port = listener.local_addr().unwrap().port();
    let s3 = tokio::spawn(async move {
        let _ = axum::serve(listener, router(svc)).await;
    });

    Cluster {
        _dir: dir,
        s3_port,
        storage,
        _meta: meta_handle,
        _s3: s3,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn erasure_survives_losing_parity_count_nodes() {
    let c = ec_cluster(4, 2).await; // 4 data + 2 parity, tolerate 2 losses
    let store = client(c.s3_port);
    let key = OPath::from("erasure/object.bin");
    // A multi-stripe payload so reconstruction really exercises the math.
    let payload: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();
    store
        .put(&key, PutPayload::from(payload.clone()))
        .await
        .unwrap();

    // Kill two of the six storage nodes (= m); any k=4 survivors reconstruct.
    c.storage[1].abort();
    c.storage[4].abort();
    settle().await;

    let got = store.get(&key).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), payload.as_slice());

    // Degraded range read returns the right window too.
    let part = store.get_range(&key, 100u64..150).await.unwrap();
    assert_eq!(part.as_ref(), &payload[100..150]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_survives_losing_replicas() {
    let c = cluster(3, 3, 2).await; // RF=3, W=2
    let store = client(c.s3_port);
    let key = OPath::from("replicated.bin");
    let payload = b"this object is replicated to all three nodes";
    store
        .put(&key, PutPayload::from_static(payload))
        .await
        .unwrap();

    // Kill two of the three storage nodes; the survivor still has a replica.
    c.storage[0].abort();
    c.storage[1].abort();
    settle().await;

    let got = store.get(&key).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), payload);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_quorum_tolerates_one_failure() {
    let c = cluster(3, 3, 2).await; // W=2 of 3
    c.storage[0].abort(); // one node down before the write
    settle().await;

    let store = client(c.s3_port);
    let key = OPath::from("quorum-ok.bin");
    let payload = b"two of three acks is enough";
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_quorum_fails_when_too_few_replicas() {
    let c = cluster(3, 3, 3).await; // W=3: every replica must ack
    c.storage[0].abort();
    settle().await;

    let store = client(c.s3_port);
    let key = OPath::from("quorum-fail.bin");
    // Only two replicas can ack (< 3) → the write fails.
    assert!(store
        .put(&key, PutPayload::from_static(b"x"))
        .await
        .is_err());
}

/// A storage backend whose availability can be toggled, simulating a node going
/// down/up without rebinding its socket. Offline → every op fails as if the node
/// were unreachable.
struct ToggleBackend {
    inner: Arc<dyn StorageBackend>,
    online: Arc<std::sync::atomic::AtomicBool>,
}

impl ToggleBackend {
    fn offline(&self) -> soma_backend::Result<Vec<u8>> {
        Err(soma_backend::Error::Remote("node offline".to_string()))
    }
}

impl StorageBackend for ToggleBackend {
    fn put(&self, object_id: u64, data: &[u8]) -> soma_backend::Result<()> {
        if self.online.load(std::sync::atomic::Ordering::Relaxed) {
            self.inner.put(object_id, data)
        } else {
            self.offline().map(|_| ())
        }
    }
    fn get(
        &self,
        object_id: u64,
        range: Option<soma_backend::ByteRange>,
    ) -> soma_backend::Result<Vec<u8>> {
        if self.online.load(std::sync::atomic::Ordering::Relaxed) {
            self.inner.get(object_id, range)
        } else {
            self.offline()
        }
    }
    fn delete(&self, object_id: u64) -> soma_backend::Result<()> {
        if self.online.load(std::sync::atomic::Ordering::Relaxed) {
            self.inner.delete(object_id)
        } else {
            self.offline().map(|_| ())
        }
    }
    fn sync(&self) -> soma_backend::Result<()> {
        self.inner.sync()
    }
    fn checkpoint(&self) -> soma_backend::Result<()> {
        self.inner.checkpoint()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_repair_refills_a_node_that_missed_writes() {
    use soma_cluster::{serve_meta, serve_storage, MetaClient, ReplicatedBackend};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    let dir = TempDir::new().unwrap();

    // Metadata node.
    let meta_port = free_port();
    let meta_store: Arc<dyn MetadataStore> =
        Arc::new(RedbMetaStore::open(dir.path().join("meta.redb")).unwrap());
    meta_store
        .create_bucket(BUCKET, BucketOpts::default())
        .unwrap();
    {
        let ms = meta_store.clone();
        tokio::spawn(async move {
            let _ = serve_meta(sock(meta_port), ms).await;
        });
    }

    // Three toggleable storage nodes.
    let mut online = Vec::new();
    let mut endpoints = Vec::new();
    for i in 0..3 {
        let port = free_port();
        let sdir = dir.path().join(format!("s{i}"));
        std::fs::create_dir_all(&sdir).unwrap();
        let inner: Arc<dyn StorageBackend> =
            Arc::new(LocalFsBackend::open(&sdir, BackendConfig::default()).unwrap());
        let flag = Arc::new(AtomicBool::new(true));
        online.push(flag.clone());
        let backend: Arc<dyn StorageBackend> = Arc::new(ToggleBackend {
            inner,
            online: flag,
        });
        tokio::spawn(async move {
            let _ = serve_storage(sock(port), backend).await;
        });
        endpoints.push(format!("http://127.0.0.1:{port}"));
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let meta: Arc<dyn MetadataStore> = Arc::new(
        MetaClient::connect(format!("http://127.0.0.1:{meta_port}"))
            .await
            .unwrap(),
    );
    // RF=3, W=2, and NO read cache (so reads actually hit the backends).
    let repl: Arc<dyn StorageBackend> =
        Arc::new(ReplicatedBackend::connect(endpoints, 3, 2).await.unwrap());
    let svc = S3Service::new(meta, repl, Credentials::single("AK", "SK"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let s3_port = listener.local_addr().unwrap().port();
    let _s3 = tokio::spawn(async move {
        let _ = axum::serve(listener, router(svc)).await;
    });
    let store = client(s3_port);

    // Node 0 offline during the write → it misses the object (W=2 from nodes 1,2).
    online[0].store(false, Ordering::Relaxed);
    let key = OPath::from("repair.bin");
    let payload = b"refilled by read-repair";
    store
        .put(&key, PutPayload::from_static(payload))
        .await
        .unwrap();

    // Node 0 back online; a full GET triggers read-repair to refill it.
    online[0].store(true, Ordering::Relaxed);
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
    tokio::time::sleep(Duration::from_millis(200)).await; // let the repair put land

    // Prove node 0 now holds the object: take nodes 1 and 2 offline; the read can
    // only be served from node 0.
    online[1].store(false, Ordering::Relaxed);
    online[2].store(false, Ordering::Relaxed);
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

/// Full S3 stack over `EncryptingBackend`: reads return plaintext (and ranges),
/// but the bytes persisted on disk never contain the plaintext marker.
#[tokio::test]
async fn encrypted_roundtrip_and_ciphertext_at_rest() {
    let dir = TempDir::new().unwrap();
    let (port, handle) = serve_encrypted(dir.path(), true).await;
    let store = client(port);

    let key = OPath::from("vault/secret.txt");
    let marker: &'static [u8] = b"TOP-SECRET-PLAINTEXT-MARKER-0123456789";
    store
        .put(&key, PutPayload::from_static(marker))
        .await
        .unwrap();

    // Reads come back decrypted through the gateway, including ranges.
    let got = store.get(&key).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), marker);
    let part = store.get_range(&key, 4u64..14).await.unwrap();
    assert_eq!(part.as_ref(), &marker[4..14]);

    stop(handle).await;

    // The plaintext marker must appear in no file at rest (it lives in the volume
    // only as ciphertext; metadata holds names, not payloads).
    assert!(
        !dir_contains_bytes(dir.path(), marker),
        "plaintext marker found in data dir — payload was not encrypted at rest"
    );

    // A second boot with the SAME master key still decrypts the object.
    let (port, handle) = serve_encrypted(dir.path(), false).await;
    let store = client(port);
    let got = store.get(&key).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), marker);
    stop(handle).await;
}

/// A tenant's byte quota rejects the write that would exceed it (full S3 stack),
/// and a delete frees the space for a subsequent write.
#[tokio::test]
async fn tenant_byte_quota_rejects_oversized_writes() {
    use soma_s3::{QosPolicy, TenantPolicy};
    use std::collections::HashMap;

    let dir = TempDir::new().unwrap();
    let mut tenants = HashMap::new();
    tenants.insert(
        "AK".to_string(),
        TenantPolicy {
            max_bytes: 20,
            max_objects: 0,
            rps: 0.0,
            burst: 0.0,
        },
    );
    let (port, handle) = serve_with_qos(dir.path(), QosPolicy::new(tenants)).await;
    let store = client(port);

    let fifteen = PutPayload::from_static(b"123456789012345"); // 15 bytes
    store.put(&OPath::from("a"), fifteen.clone()).await.unwrap();
    // A second 15-byte object would total 30 > 20 → rejected.
    assert!(store.put(&OPath::from("b"), fifteen.clone()).await.is_err());

    // Deleting the first frees its bytes, so the second now fits.
    store.delete(&OPath::from("a")).await.unwrap();
    store.put(&OPath::from("b"), fifteen).await.unwrap();

    stop(handle).await;
}

/// Membership + PG-table operations round-trip over the real meta gRPC path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_and_pg_table_over_grpc() {
    use soma_cluster::{serve_meta, MetaClient};
    use soma_meta::PgPlacement;

    let dir = TempDir::new().unwrap();
    let meta_port = free_port();
    let store: Arc<dyn MetadataStore> =
        Arc::new(RedbMetaStore::open(dir.path().join("meta.redb")).unwrap());
    {
        let s = store.clone();
        tokio::spawn(async move {
            let _ = serve_meta(sock(meta_port), s).await;
        });
    }
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let client = Arc::new(
        MetaClient::connect(format!("http://127.0.0.1:{meta_port}"))
            .await
            .unwrap(),
    );

    // Drive registration, heartbeat, and PG-table seeding entirely over gRPC.
    let c = client.clone();
    tokio::task::spawn_blocking(move || {
        c.register_node("node-0", "http://node-0:9200", 10).unwrap();
        c.register_node("node-1", "http://node-1:9200", 10).unwrap();
        c.heartbeat("node-0", 20).unwrap();
        assert!(matches!(
            c.heartbeat("ghost", 20),
            Err(soma_meta::Error::UnknownNode(_))
        ));

        let entries = vec![(
            0u32,
            PgPlacement {
                node_ids: vec!["node-0".to_string(), "node-1".to_string()],
                generation: 1,
            },
        )];
        assert!(c.seed_pg_table(&entries).unwrap());
        assert!(!c.seed_pg_table(&entries).unwrap()); // second seed is a no-op

        let mut members = c.list_members().unwrap();
        members.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        assert_eq!(members.len(), 2);
        assert_eq!(members[0].node_id, "node-0");
        assert_eq!(members[0].last_heartbeat, 20);

        let table = c.list_pg_table().unwrap();
        assert_eq!(table.len(), 1);
        assert_eq!(
            table[0].1.node_ids,
            vec!["node-0".to_string(), "node-1".to_string()]
        );
    })
    .await
    .unwrap();
}
