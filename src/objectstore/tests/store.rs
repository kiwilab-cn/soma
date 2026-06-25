//! `SomaStore` short-circuits local range reads and falls back to the inner store.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path as OPath;
use object_store::{ObjectStore, PutPayload};
use soma_backend::{BackendConfig, LocalFsBackend, LocalReader, StorageBackend};
use soma_client::{Located, Remote};
use soma_localfd::serve_local_reads;
use soma_object_store::SomaStore;
use tempfile::TempDir;

/// A locator that returns a fixed `?location` answer.
struct FakeLocator {
    located: Option<Located>,
}
impl Remote for FakeLocator {
    fn locate(&self, _bucket: &str, _key: &str) -> soma_client::Result<Option<Located>> {
        Ok(self.located.clone())
    }
    fn get(&self, _bucket: &str, _key: &str) -> soma_client::Result<Vec<u8>> {
        Ok(Vec::new()) // unused: SomaStore reads remote through `inner`, not the locator
    }
}

/// Serve object id 1 (a >64 KiB payload) on a local socket; also seed `inner` with
/// the same bytes for the fallback path. Returns (store-pieces, payload, server).
async fn setup(
    dir: &TempDir,
    hosts: Vec<String>,
    my_host: &str,
    socket_override: Option<String>,
) -> (SomaStore, Vec<u8>, soma_localfd::LocalServer) {
    let backend = Arc::new(LocalFsBackend::open(dir.path(), BackendConfig::default()).unwrap());
    let payload: Vec<u8> = (0..200 * 1024).map(|i| (i * 31 + 7) as u8).collect();
    backend.put(1, &payload).unwrap();
    let reader: Arc<dyn LocalReader> = backend;
    let server = serve_local_reads(dir.path().join("s.sock"), reader).unwrap();
    let socket = socket_override.unwrap_or_else(|| server.path().to_string_lossy().into_owned());

    // The inner (remote) store holds the same object for fallback.
    let inner = Arc::new(InMemory::new());
    inner
        .put(&OPath::from("obj"), PutPayload::from(Bytes::from(payload.clone())))
        .await
        .unwrap();

    let locator = Arc::new(FakeLocator {
        located: Some(Located {
            object_id: 1,
            size: payload.len() as u64,
            hosts,
        }),
    });
    let store = SomaStore::with_locator(inner, locator, "bkt".into(), my_host.into(), socket);
    (store, payload, server)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_range_short_circuits_local() {
    let dir = TempDir::new().unwrap();
    let (store, payload, _server) = setup(&dir, vec!["myhost".into()], "myhost", None).await;

    // Whole-object range → the CRC-verified local mmap path.
    let whole = store
        .get_range(&OPath::from("obj"), 0..payload.len() as u64)
        .await
        .unwrap();
    assert_eq!(whole.as_ref(), payload.as_slice());

    // A partial range → local mmap sub-slice.
    let part = store.get_range(&OPath::from("obj"), 100..200).await.unwrap();
    assert_eq!(part.as_ref(), &payload[100..200]);

    // Multiple ranges in one locate + descriptor.
    let multi = store
        .get_ranges(&OPath::from("obj"), &[0..16, 1000..1064])
        .await
        .unwrap();
    assert_eq!(multi[0].as_ref(), &payload[0..16]);
    assert_eq!(multi[1].as_ref(), &payload[1000..1064]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn falls_back_to_inner_when_not_co_located() {
    let dir = TempDir::new().unwrap();
    // The only holder is on another host → no local attempt → inner serves it.
    let (store, payload, _server) = setup(&dir, vec!["other-host".into()], "myhost", None).await;
    let got = store.get_range(&OPath::from("obj"), 0..64).await.unwrap();
    assert_eq!(got.as_ref(), &payload[0..64]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_locality_uses_inner() {
    let dir = TempDir::new().unwrap();
    // Empty socket disables short-circuiting; reads go to the inner store.
    let (store, payload, _server) =
        setup(&dir, vec!["myhost".into()], "myhost", Some(String::new())).await;
    let got = store.get_range(&OPath::from("obj"), 10..42).await.unwrap();
    assert_eq!(got.as_ref(), &payload[10..42]);
}
