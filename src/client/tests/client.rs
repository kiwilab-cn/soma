//! Integration tests: the local short-circuit decision (with a fake gateway and a
//! real local socket), and a real signed GET against a running s3 server.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use soma_backend::{BackendConfig, LocalFsBackend, LocalReader, StorageBackend};
use soma_client::{Located, Remote, Result, SomaClient};
use soma_localfd::serve_local_reads;
use tempfile::TempDir;

/// A stand-in gateway: returns a fixed location and a sentinel body, so a test can
/// tell whether a read took the local short-circuit or the remote fallback.
struct FakeRemote {
    located: Option<Located>,
    remote_bytes: Vec<u8>,
}

impl Remote for FakeRemote {
    fn locate(&self, _bucket: &str, _key: &str) -> Result<Option<Located>> {
        Ok(self.located.clone())
    }
    fn get(&self, _bucket: &str, _key: &str) -> Result<Vec<u8>> {
        Ok(self.remote_bytes.clone())
    }
}

const REMOTE_SENTINEL: &[u8] = b"REMOTE-FALLBACK-BODY";

/// Serve a backend with one object (id 1) on a local socket; return the socket
/// path, the payload, and the live server (kept alive by the caller).
fn serve_one(dir: &TempDir) -> (String, Vec<u8>, soma_localfd::LocalServer) {
    let backend = Arc::new(LocalFsBackend::open(dir.path(), BackendConfig::default()).unwrap());
    let payload = b"LOCAL-ZERO-COPY-PAYLOAD".to_vec();
    backend.put(1, &payload).unwrap();
    let reader: Arc<dyn LocalReader> = backend;
    let server = serve_local_reads(dir.path().join("storage.sock"), reader).unwrap();
    let socket = server.path().to_string_lossy().into_owned();
    (socket, payload, server)
}

#[test]
fn short_circuits_to_local_when_co_located() {
    let dir = TempDir::new().unwrap();
    let (socket, payload, _server) = serve_one(&dir);

    let remote = Box::new(FakeRemote {
        located: Some(Located {
            object_id: 1,
            size: payload.len() as u64,
            hosts: vec!["myhost".into()],
        }),
        remote_bytes: REMOTE_SENTINEL.to_vec(),
    });
    let client = SomaClient::with_remote(remote, "myhost".into(), socket);

    // Co-located with a holder → bytes come from the local descriptor, not remote.
    assert_eq!(client.get("b", "k").unwrap(), payload);
}

#[test]
fn falls_back_to_remote_when_local_object_missing() {
    let dir = TempDir::new().unwrap();
    let (socket, _payload, _server) = serve_one(&dir);

    // Co-located by host, but the id is not actually on the node (a stale/raced
    // location) → the local read 404s and the client falls back to the gateway.
    let remote = Box::new(FakeRemote {
        located: Some(Located {
            object_id: 999,
            size: 0,
            hosts: vec!["myhost".into()],
        }),
        remote_bytes: REMOTE_SENTINEL.to_vec(),
    });
    let client = SomaClient::with_remote(remote, "myhost".into(), socket);
    assert_eq!(client.get("b", "k").unwrap(), REMOTE_SENTINEL);
}

#[test]
fn reads_remote_when_not_co_located() {
    let dir = TempDir::new().unwrap();
    let (socket, _payload, _server) = serve_one(&dir);

    // No holder on this host → straight to the gateway, no local attempt.
    let remote = Box::new(FakeRemote {
        located: Some(Located {
            object_id: 1,
            size: 0,
            hosts: vec!["other-host".into()],
        }),
        remote_bytes: REMOTE_SENTINEL.to_vec(),
    });
    let client = SomaClient::with_remote(remote, "myhost".into(), socket);
    assert_eq!(client.get("b", "k").unwrap(), REMOTE_SENTINEL);
}

#[test]
fn no_locality_config_reads_remote() {
    // Empty host/socket disables short-circuiting entirely.
    let remote = Box::new(FakeRemote {
        located: Some(Located {
            object_id: 1,
            size: 0,
            hosts: vec!["myhost".into()],
        }),
        remote_bytes: REMOTE_SENTINEL.to_vec(),
    });
    let client = SomaClient::with_remote(remote, String::new(), String::new());
    assert_eq!(client.get("b", "k").unwrap(), REMOTE_SENTINEL);
}

// --- real signed GET against a running s3 server ---------------------------

use soma_meta::{BucketOpts, ETag, MetadataStore, ObjectPut, PutCondition, RedbMetaStore};
use soma_s3::{router, Credentials, S3Service};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_get_over_real_gateway() {
    let dir = TempDir::new().unwrap();
    let meta = Arc::new(RedbMetaStore::open(dir.path().join("meta.redb")).unwrap());
    let backend = Arc::new(LocalFsBackend::open(dir.path(), BackendConfig::default()).unwrap());

    // Seed an object directly through the metadata + storage layers.
    let body = b"hello over the wire".to_vec();
    meta.create_bucket("bkt", BucketOpts::default()).unwrap();
    let oid = meta.next_object_id().unwrap();
    backend.put(oid, &body).unwrap();
    meta.put_object(
        "bkt",
        "obj",
        ObjectPut {
            object_id: oid,
            size: body.len() as u64,
            etag: ETag("test".into()),
            created_at: 0,
            encrypted: false,
        },
        PutCondition::None,
    )
    .unwrap();

    let meta_dyn: Arc<dyn MetadataStore> = meta;
    let backend_dyn: Arc<dyn StorageBackend> = backend;
    let svc = S3Service::new(meta_dyn, backend_dyn, Credentials::single("AK", "SK"));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, router(svc)).await;
    });

    // The blocking client (no locality config) signs and GETs over HTTP. `?location`
    // 501s on this single-node server, so it falls back to the signed S3 GET.
    let body_clone = body.clone();
    let got = tokio::task::spawn_blocking(move || {
        let client = SomaClient::new(soma_client::ClientConfig {
            gateway_endpoint: format!("http://127.0.0.1:{port}"),
            access_key: "AK".into(),
            secret_key: "SK".into(),
            region: "us-east-1".into(),
            my_host: String::new(),
            local_socket_path: String::new(),
        });
        client.get("bkt", "obj")
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(got, body_clone);

    handle.abort();
}
