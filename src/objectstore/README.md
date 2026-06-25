# soma-object-store — `SomaStore`

An [`object_store::ObjectStore`](https://crates.io/crates/object_store) that adds
soma's **data-locality short-circuit reads** on top of a standard S3 backend. If your
engine already reads through the `object_store` crate, swap your `AmazonS3` for a
`SomaStore` and you get zero-copy local reads when co-located with the data — **no
read-path rewrite**, transparent fallback to the gateway otherwise.

> Integration reference for a compute/storage-separated consumer. See also
> [`docs/LOCALITY_DESIGN.md`](../../docs/LOCALITY_DESIGN.md) (the mechanism),
> [`docs/CONDITIONAL_WRITES.md`](../../docs/CONDITIONAL_WRITES.md) (manifest-commit
> CAS), and [`docs/OBJECT_SIZING.md`](../../docs/OBJECT_SIZING.md) (how big to make
> objects/segments).

## What it does

`SomaStore` wraps an `inner: Arc<dyn ObjectStore>` (an `AmazonS3` pointed at the soma
gateway). It **delegates every operation to `inner`** — *except* `get_range` /
`get_ranges`, which first try a **local** read:

1. resolve the object's holders via the gateway's `?location` oracle;
2. if a holder is on **this** host, obtain the volume file descriptor over the node's
   unix socket and `mmap` the requested byte range — **zero-copy**, sharing the
   storage node's page cache, no gateway and no network;
3. on **any** miss (not co-located, no oracle, a raced/missing id, a socket hiccup,
   an out-of-range request) → fall back to `inner.get_range(...)`.

So the **range API is the accelerated, zero-copy hot path** (the columnar pattern:
footer + row-group range reads). Everything else — `put` / `get` (full) / `head` /
`list` / `delete` / multipart / `copy` / conditional put — is plain S3 through
`inner`. Locality is a pure optimization: reads always succeed if the object exists,
so the same store works on- and off-cluster.

## Add the dependency

Not published to crates.io; depend on it from git (it pulls `soma-client` and
`soma-localfd` transitively):

```toml
[dependencies]
soma-object-store = { git = "https://github.com/kiwilab-cn/soma" }
object_store = { version = "0.12", features = ["aws"] }
```

## Construct it

```rust
use std::sync::Arc;
use object_store::ObjectStore;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use soma_object_store::{SomaStore, LocalityConfig};

fn make_store() -> SomaStore {
    let endpoint = "http://soma-gateway:9000"; // the soma S3 gateway
    let bucket = "my-tenant";                   // one store per bucket (tenant)
    let (access_key, secret_key, region) = ("AK", "SK", "us-east-1");

    // The remote backend: a standard S3 store pointed at the gateway. Enable
    // conditional put so manifest-commit CAS (If-Match / If-None-Match) works.
    let inner: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_region(region)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_allow_http(true) // cluster-internal http
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()
            .expect("build inner s3"),
    );

    SomaStore::new(inner, LocalityConfig {
        gateway_endpoint: endpoint.into(),
        access_key: access_key.into(),
        secret_key: secret_key.into(),
        region: region.into(),
        bucket: bucket.into(),
        // This pod's k8s node name (inject from the downward API: spec.nodeName).
        // Empty disables short-circuiting → behaves as a plain S3 store.
        my_host: std::env::var("SOMA_HOST").unwrap_or_default(),
        // The co-located storage node's local-read socket, mounted via a shared
        // hostPath. Empty disables short-circuiting.
        local_socket_path: std::env::var("SOMA_LOCAL_SOCKET_PATH").unwrap_or_default(),
    })
}
```

Then use it as any `object_store::ObjectStore` — range reads are short-circuited
automatically:

```rust
# async fn demo(store: &dyn object_store::ObjectStore, key: &object_store::path::Path) -> object_store::Result<()> {
let footer = store.get_range(key, 0..8192).await?;            // local mmap when co-located
let groups = store.get_ranges(key, &[0..4096, 1_000_000..1_050_000]).await?;
store.put(key, object_store::PutPayload::from_static(b"...")).await?; // delegated to inner
# Ok(()) }
```

## `LocalityConfig`

| field | meaning |
| --- | --- |
| `gateway_endpoint` | soma gateway base URL, for the `?location` oracle. |
| `access_key` / `secret_key` / `region` | SigV4 credentials for signing `?location` (match the inner store's). |
| `bucket` | the bucket this store is scoped to (object_store is single-bucket). |
| `my_host` | this process's host (k8s node name). **Empty → no short-circuit.** |
| `local_socket_path` | the co-located node's local-read socket path. **Empty → no short-circuit.** |

## Behavior & guarantees

- **Zero-copy.** A local range read returns `bytes::Bytes` backed by the `mmap`
  (`Bytes::from_owner`), sharing the page cache; the mapping is lazy, so a partial
  range faults in only the touched pages. Works for objects of any size.
- **Integrity.** A *whole-object* range read (`0..len`) is CRC-verified against the
  needle CRC; *partial* ranges are not (the sub-range can't be checked against the
  whole-needle CRC) — rely on soma's background scrub and your own format/block CRCs.
- **Full `get`** returns an owned buffer via `inner`; use `get_range` for the
  zero-copy path.
- **Connection reuse.** The local socket connection is reused and reconnected on
  error; reads through one `SomaStore` are serialized over it — open multiple stores
  for concurrent local reads.
- **CAS / manifest commit.** Use `put_opts` with `PutMode::Create` (create-if-absent)
  / `PutMode::Update(UpdateVersion { e_tag, .. })` (update-if-unchanged) — delegated
  to `inner`, fenced atomically by soma. See `docs/CONDITIONAL_WRITES.md`.

## Deployment prerequisites for the local path to engage

If any of these is missing, `SomaStore` silently behaves as a normal S3 store
(graceful — locality is opt-in and best-effort):

1. **soma deployed as a cluster** (gateway + storage + meta). A single-node gateway
   has no oracle and `?location` returns 501 → always falls back to remote.
2. **storage `localRead` enabled** (the node binds the socket on a hostPath) and the
   compute pod is **co-located** with a storage pod (`podAffinity`, hostname
   topology) and **mounts the same hostPath socket dir**. See
   `deploy/examples/compute-colocation.yaml` and `docs/LOCALITY_DESIGN.md` §5.
3. **`my_host` set** to the pod's node name so it can match a holding node's host.
4. A **shared-kernel container runtime** (runc); Kata/gVisor can't receive the fd and
   fall back to the gateway.
5. Until per-tenant socket scoping (P6) lands, the socket is unauthenticated — use it
   in **single-tenant / dedicated** deployments only.
