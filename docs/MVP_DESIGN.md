# Soma MVP (M0) Design

> Detailed design for milestone **M0 — single-node skeleton**.
> Parent: [`ARCHITECTURE.md`](./ARCHITECTURE.md). This document refines M0 only;
> later milestones get their own design docs.

## 1. Goal

Ship the smallest thing that is **real and useful**: a single-node, S3-compatible
object store that downstream database engines can connect to today via the Rust
`object_store` crate (or any S3 SDK / `aws-cli`), with a storage layout and trait
boundaries that the distributed milestones (M1–M4) extend rather than rewrite.

**Definition of done:** an S3 client can `PutObject` / `GetObject` (incl. range) /
`HeadObject` / `DeleteObject` / `ListObjectsV2` / multipart-upload / create &
delete buckets against Soma, with SigV4 auth, strong read-after-write consistency,
and conditional writes (`If-Match` / `If-None-Match`) — all backed by the volume +
needle on-disk format and durable across restarts.

### In scope (M0)

- S3 subset (see §5), SigV4 authentication.
- `LocalFsBackend`: volume + needle storage on local disk, with `.idx` checkpoint
  and crash-safe rebuild.
- `MetadataStore` trait + a `redb` implementation, with conditional writes and
  per-object versioning semantics.
- Single-node write/read paths following the §6 protocol from `ARCHITECTURE.md`.
- The Cargo workspace + crate boundaries that later milestones build on.

### Out of scope (M0 — deferred to later milestones)

Raft / distribution (M2) · replication & EC (M2/M4) · NVMe/memory cache tier (M1) ·
AI ingest / vectorization (M4) · Kubernetes manifests & operator (M1+) · encryption
at rest & multi-tenant QoS (M4). M0 runs as one process against one data directory.

The trait boundaries are chosen so these arrive **behind the traits**, not as
rewrites: e.g. the single-node `MetadataStore` is later wrapped by a Raft-replicated
implementation; `LocalFsBackend` is joined by `ReplicatedBackend` / `ErasureCodedBackend`.

---

## 2. Cargo workspace layout

A multi-crate workspace from day one, organized the same way as the sibling Rust
projects (kokedb): each member crate lives under `src/<name>/`, with shared
version, lints, and **centralized dependency versions** declared once in the root
`[workspace.*]` tables. Each crate then lists only the dependencies it needs via
`<dep>.workspace = true`, so dependency surfaces stay minimal and per-crate while
versions never drift.

```
soma/
├── Cargo.toml                 # [workspace] — members, [workspace.package/lints/dependencies]
├── src/
│   ├── core/                  # crate soma-core: needle/volume codec, hot index, ids, Error
│   │   ├── Cargo.toml
│   │   └── src/
│   ├── backend/               # crate soma-backend: StorageBackend + LocalFsBackend
│   ├── meta/                  # crate soma-meta: MetadataStore + RedbMetaStore
│   ├── s3/                    # crate soma-s3: S3 protocol + SigV4
│   └── server/                # crate soma-server: binary, wires the above together
└── docs/
```

Each crate keeps its own `tests/` for integration tests. Dependency direction (no
cycles):

```
soma-server → soma-s3 → { soma-meta, soma-backend } → soma-core
```

Keep the base crate light: `soma-core` pulls no async runtime and no heavy deps,
so it stays embeddable — mirroring how kokedb forbids DataFusion deps in its base
`common` crate.

### Crate responsibilities

| Crate | Owns | Key public types |
| --- | --- | --- |
| `soma-core` | On-disk needle/volume encoding & decoding, the in-RAM hot index, checksums, object/needle IDs, shared `Error`/`Result`. No IO policy, no S3. | `Needle`, `NeedleHeader`, `VolumeId`, `NeedleLoc`, `HotIndex`, `Checksum`, `Error` |
| `soma-meta` | The authority for `name → location + version`. Trait + `redb` impl. Conditional writes, versioning. | `MetadataStore` (trait), `RedbMetaStore`, `ObjectMeta`, `Version`, `PutCondition` |
| `soma-backend` | Durability: writing/reading needles to/from volumes, fsync, `.idx` checkpoint & rebuild. Trait + local-FS impl. | `StorageBackend` (trait), `LocalFsBackend`, `Volume`, `WriteAggregator` |
| `soma-s3` | S3 wire protocol: request parsing, XML responses, error codes, SigV4 verification. Maps S3 ops onto meta + backend. | `S3Service`, `SigV4Verifier`, request/response models |
| `soma-server` | Process entry: config, HTTP server (`axum`/`hyper`), assembly, graceful shutdown. | `main`, `Config` |

### Workspace lints (inherited by every crate)

```toml
[workspace.lints.clippy]
unwrap_used = "deny"
expect_used = "deny"
panic = "deny"
```

Fallible paths use `Result`; `?` and explicit error variants only.

---

## 3. Object & key model

- **Bucket**: a namespace. M0 stores bucket records in the metadata store
  (creation time, versioning flag, owner).
- **Object key**: arbitrary UTF-8 S3 key (may contain `/`; it is *not* a directory
  — listing emulates prefixes/delimiters over a flat keyspace).
- **Object id**: an internal `u64` (monotonic) assigned per object version. Used as
  the needle key, decoupling the (possibly long) S3 key from the compact in-RAM
  index.

Metadata maps `(bucket, key) → current ObjectMeta`, where `ObjectMeta` holds the
version chain head. Each version points at its needle location(s).

---

## 4. On-disk format (`soma-core` + `LocalFsBackend`)

### 4.1 Data directory layout

```
<data_dir>/
├── volumes/
│   ├── 0000000001.vol         # append-only needle container
│   ├── 0000000001.idx         # hot-index checkpoint for that volume
│   ├── 0000000002.vol
│   └── ...
└── meta/
    └── soma.redb              # redb file (metadata store)
```

### 4.2 Needle layout (within a `.vol` file)

```
needle:
  ┌────────────────────────── header (fixed) ──────────────────────────┐
  │ magic:u32 │ version:u8 │ flags:u8 │ object_id:u64 │ data_len:u32    │
  │ header_crc:u32 │ data_crc:u32                                       │
  └─────────────────────────────────────────────────────────────────────┘
  ┌─ data ─┐ ┌─ padding to 8-byte alignment ─┐
  │ bytes  │ │ 0x00 ...                       │
  └────────┘ └───────────────────────────────┘
```

- `magic` — needle sentinel, used to resync when scanning/rebuilding.
- `flags` — bit 0 = tombstone (delete marker); reserved bits for chunked/large.
- `data_crc` — CRC32C over the data bytes; verified on read (bitrot guard).
- `header_crc` — CRC32C over the header (excluding itself); detects torn headers.
- Needles are 8-byte aligned so a scan can step deterministically.

A **tombstone** needle (flags bit 0) records a delete; physical reclamation happens
during compaction (post-M0; M0 GC may be a no-op stub, space is reclaimed by the
later compactor).

### 4.3 `.idx` checkpoint & rebuild

The `.idx` file is a periodically-flushed snapshot of the hot index for one volume:
a packed array of `(object_id: u64, offset: u64, size: u32, flags: u8)` entries.

**Recovery on startup** (per volume):

1. Load the `.idx` checkpoint (gives index up to `checkpoint_offset`).
2. Scan the `.vol` from `checkpoint_offset` to EOF: for each valid needle (magic +
   `header_crc` ok), apply it to the index; stop at the first torn/partial needle
   (truncate the volume to the last good needle boundary — torn-tail discipline).
3. The hot index is now current. No metadata is trusted from the volume; the
   metadata store remains the authority for which version is live.

This is the SeaweedFS `.idx` pattern: the volume file is self-describing enough to
rebuild its own index. The hot index is a **derived cache** (per `ARCHITECTURE.md`
§4.4); losing it is never data loss.

---

## 5. S3 API surface (M0)

| Operation | Notes |
| --- | --- |
| `PutObject` | Writes a needle, commits metadata. Supports `If-Match` / `If-None-Match`. |
| `GetObject` | Including `Range` requests. |
| `HeadObject` | Metadata only (size, etag, version). |
| `DeleteObject` | Writes tombstone, removes/links version in metadata. |
| `ListObjectsV2` | Prefix + delimiter emulation over the flat keyspace; pagination via continuation token. |
| `CreateBucket` / `DeleteBucket` / `ListBuckets` | Bucket lifecycle. |
| `CreateMultipartUpload` / `UploadPart` / `CompleteMultipartUpload` / `AbortMultipartUpload` | Each part is written to the backend as a needle on upload; upload state is held in memory (ephemeral). On complete, parts are assembled into one object and the multipart ETag (`md5(concat of part md5s)-N`) is returned. (True chunked objects — no assembly buffering — come with large-object chunking later.) |
| Auth | **AWS SigV4** request signing verification. |

**ETag**: M0 uses the object's content hash (or, for multipart, the S3-style
`hash-of-hashes-N` form) so existing S3 clients and `If-Match` work as expected.

**Errors**: standard S3 XML error responses (`NoSuchKey`, `NoSuchBucket`,
`PreconditionFailed`, `SignatureDoesNotMatch`, etc.).

Deliberately **excluded** from M0: ACL APIs, bucket policies/versioning-list APIs
(beyond a versioning toggle), tagging, lifecycle, CORS, website, replication config.

---

## 6. Trait drafts

These signatures are the contract later milestones implement behind. The storage
and metadata layers are **synchronous**: they do blocking, fsync-bound IO (like
the embedded engines they wrap), and the async edge — the S3 HTTP server —
bridges to them via `spawn_blocking`. Keeping them sync makes the cores
exhaustively testable without a runtime and avoids async-in-disk-IO footguns.
(Final form may adjust during implementation.)

### 6.1 `MetadataStore` (`soma-meta`)

```rust
pub trait MetadataStore: Send + Sync {
    // Buckets
    fn create_bucket(&self, name: &str, opts: BucketOpts) -> Result<()>;
    fn delete_bucket(&self, name: &str) -> Result<()>;
    fn list_buckets(&self) -> Result<Vec<BucketMeta>>;

    // Objects — `cond` carries If-Match / If-None-Match.
    // Returns the committed version (CAS evaluated atomically inside the store).
    fn put_object(
        &self,
        bucket: &str,
        key: &str,
        loc: ObjectLocation,      // needle location, size, content hash
        cond: PutCondition,
    ) -> Result<Version>;

    fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>>;
    fn delete_object(&self, bucket: &str, key: &str, cond: PutCondition) -> Result<()>;

    fn list_objects(&self, bucket: &str, req: ListRequest) -> Result<ListResult>;

    // Internal id allocation (monotonic object ids).
    fn next_object_id(&self) -> Result<u64>;
}

pub enum PutCondition {
    None,
    IfMatch(ETag),        // overwrite only if current ETag matches
    IfNoneMatch,          // create only if absent  (S3 `If-None-Match: *`)
}
```

The CAS is evaluated **inside a single redb write transaction** in M0 — that is the
linearization point now; under Raft (M2) it moves into the state-machine `apply()`
without changing this trait (per `ARCHITECTURE.md` §5.3).

### 6.2 `StorageBackend` (`soma-backend`)

```rust
pub trait StorageBackend: Send + Sync {
    /// Append `data` as a needle, fsync it, return its durable location.
    fn put(&self, object_id: ObjectId, data: &[u8]) -> Result<ObjectLocation>;

    /// Read object bytes (optionally a byte range), verifying the payload CRC.
    fn get(&self, loc: ObjectLocation, range: Option<ByteRange>) -> Result<Vec<u8>>;

    /// Append a tombstone (delete marker); return its location. Physical reclaim
    /// happens later via compaction.
    fn delete(&self, object_id: ObjectId) -> Result<ObjectLocation>;

    /// Flush all volumes (fsync).
    fn sync(&self) -> Result<()>;

    /// Write a `.idx` checkpoint per volume so recovery can skip a full scan.
    fn checkpoint(&self) -> Result<()>;
}
```

`LocalFsBackend` is the M0 implementation: bytes live in append-only
`<id>.vol` files, one needle per object, fsynced on `put` before the location is
returned (durability before metadata commit). On open it recovers each volume by
loading the `.idx` checkpoint and scanning the tail forward, truncating any torn
tail. M0 fsyncs per `put` for correctness; coalescing concurrent writes into one
fsync (write aggregation) is a later performance pass.

---

## 7. Single-node write & read paths (M0)

**PUT** (follows `ARCHITECTURE.md` §6, collapsed to one node):

1. Verify SigV4. Resolve bucket.
2. `object_id = meta.next_object_id()`.
3. `loc = backend.put(object_id, body)` → needle appended + **fsync** (bytes
   durable).
4. `version = meta.put_object(bucket, key, loc, cond)` → atomic redb txn; evaluates
   `If-Match` / `If-None-Match`; on failure returns `PreconditionFailed` and the
   needle becomes an orphan (reclaimed later — safe, never read).
5. Update the in-RAM hot index. ACK with ETag + version.

**GET**:

1. Verify SigV4. `meta.get_object(bucket, key)` → current version's `loc`.
2. `backend.get(loc, range)` — hot index gives `(volume, offset, size)`; one
   `pread`; verify `data_crc`; stream out.

**DELETE**: SigV4 → `meta.delete_object` (version/tombstone in metadata) →
`backend.delete` writes a tombstone needle.

---

## 8. Configuration

```toml
# soma.toml
data_dir   = "/var/lib/soma"
listen     = "0.0.0.0:9000"
volume_max = "4GiB"          # rotate to a new .vol at this size
idx_flush  = "5s"            # hot-index checkpoint cadence

[[credentials]]              # M0: static keys; IAM proper comes later
access_key = "soma"
secret_key = "..."
```

Config via file + env overrides (`SOMA_*`). Static credentials in M0; a real IAM /
tenant model lands with security hardening (M4).

---

## 9. Acceptance criteria

1. **S3 smoke**: `aws-cli` (`--endpoint-url`) can create a bucket, put/get/head/
   delete an object, range-get, and run a multipart upload.
2. **Consumer integration**: a downstream engine using the `object_store` crate
   (S3 backend pointed at Soma) round-trips objects under its real workload shape
   (write segment, read segment, list, conditional overwrite).
3. **Durability**: kill the process mid-test; on restart, committed objects are
   readable and the hot index rebuilds from `.vol` + `.idx`.
4. **Conditional writes**: `If-None-Match: *` rejects an overwrite;
   `If-Match: <etag>` succeeds only on a matching current ETag.

---

## 10. Testing (M0)

- **Unit / property** (`soma-core`): needle encode→decode round-trip; CRC catches
  flipped bits; index rebuild from a scan equals the live index; torn-tail
  truncation finds the right boundary.
- **`soma-meta`**: CAS semantics (concurrent conditional puts; exactly one wins);
  versioning chain; list pagination & delimiter emulation.
- **`soma-backend`**: write-aggregation correctness; fsync ordering; `.idx`
  checkpoint + rebuild equivalence; crash-in-the-middle of an append truncates
  cleanly.
- **Integration** (`tests/`): full S3 flows via an HTTP client; restart-recovery;
  the consumer `object_store` round-trip.

Crash/restart tests are first-class even in M0 — the write protocol's safety claim
(§6 of `ARCHITECTURE.md`) must be demonstrated, not assumed.

---

## 11. Branch plan for M0 implementation

Once this design is approved, implementation lands as a sequence of reviewed
branches (each PR'd to `main` and merged before the next):

1. `feat/m0-workspace` — workspace skeleton, crates, lints, CI scaffold, `soma-core`
   needle codec + hot index (+ unit/property tests).
2. `feat/m0-backend` — `StorageBackend` + `LocalFsBackend` (volumes, aggregation,
   `.idx`, rebuild).
3. `feat/m0-meta` — `MetadataStore` + `RedbMetaStore` (CAS, versioning, listing).
4. `feat/m0-s3` — S3 protocol + SigV4 in `soma-s3` (bucket lifecycle, single-part
   object CRUD, range reads, `ListObjectsV2`, conditional writes), wired into
   `soma-server`. Multipart upload is split out to keep this PR reviewable.
5. `feat/m0-multipart` — multipart upload (`CreateMultipartUpload` / `UploadPart` /
   `CompleteMultipartUpload` / `AbortMultipartUpload`); until then those routes
   return `NotImplemented`.
6. `feat/m0-integration` — end-to-end tests, consumer round-trip, restart recovery,
   acceptance criteria.

Each branch is independently testable and reviewable.
