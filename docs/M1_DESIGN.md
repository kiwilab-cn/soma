# Soma M1 Design — Read Cache + Cloud-Native Readiness

> Detailed design for milestone **M1**.
> Parent: [`ARCHITECTURE.md`](./ARCHITECTURE.md). Builds on
> [`MVP_DESIGN.md`](./MVP_DESIGN.md) (M0, complete). M1 stays **single-node** and
> lays the foundation for the distributed milestones without building plumbing
> they would throw away.

## 1. Goal & framing

The architecture (§13) labels M1 "stateless gateway + cache + k8s". A *truly*
stateless gateway requires the metadata and storage to be externalized — which is
M2 (Raft + distributed backend). Splitting the process before then would build
throwaway plumbing, and a local-NVMe cache tier in front of a local-NVMe backend
is redundant (the OS page cache already serves it).

So M1 is scoped to what delivers value **today, single-node**, and seeds M2:

> **M1 = an in-memory read cache + cloud-native operational readiness.**

### In scope (M1)

- **Read cache**: an in-memory tier (`foyer`) on the GET read path, keyed by the
  immutable object location, tuned for the small-file hot set.
- **Structured config**: `figment` (file + env), replacing M0's ad-hoc env parsing.
- **Operational endpoints**: `/healthz`, `/readyz`, `/metrics` on a separate admin
  port (no SigV4, no S3 path collision).
- **Metrics**: Prometheus — cache hit/miss/bytes, S3 request counts/latency.
- **Packaging**: multi-stage `Dockerfile` + a Helm chart (single-node StatefulSet
  with a data volume, Service, ConfigMap/Secret, health probes).

### Out of scope (deferred to M2+)

True stateless gateway **process** split, the **NVMe cache tier**, consistent-hash
placement, Raft metadata, replication, and the Kubernetes **operator/CRDs**. These
arrive when the system is actually distributed and there is a cluster to operate.

### No new architectural seam needed

M2's stateless split slots in **behind the traits that already exist**:
`MetadataStore` and `StorageBackend`. M2 swaps `RedbMetaStore` → a Raft-backed
store and `LocalFsBackend` → a replicated/EC backend, with no change to the S3
layer. The M1 read cache is itself just a `StorageBackend` decorator (§3), which
proves the seam. M1 introduces **no premature abstraction**.

---

## 2. How M1 relates to the small-file story

The **small-file storage** is already solved in M0 — the volume + needle packing
(`ARCHITECTURE.md` §4.2): many small objects packed into large append-only volume
files, an in-RAM hot index giving O(1) reads, never one-file-per-object.

M1's read cache is the **read-latency** half of the same story: it keeps the hot
*small* objects in memory so repeated reads skip the `pread` + CRC verification.
Admission is biased toward small objects (§3.3), so the cache serves exactly the
small-file hot set.

What remains for small files is **write-path** and **space** work, both deferred
(not part of M1): **write aggregation / group commit** (batch many small needle
appends into one sequential write + one fsync, preserving the durability ordering)
and **GC/compaction** of dead needles. These are optimizations over an
already-correct M0; they land once there is real workload pressure to measure
against.

---

## 3. Read cache

### 3.1 Shape: a `StorageBackend` decorator

The cache is a `CachingBackend` that wraps an inner `StorageBackend`:

```
S3Service ─▶ StorageBackend (trait)
                 └── CachingBackend { inner: Arc<dyn StorageBackend>, cache }
                          └── LocalFsBackend            (the real bytes)
```

It is transparent: the S3 layer still sees a `StorageBackend`. `put` / `delete` /
`sync` / `checkpoint` pass straight through; only `get` consults the cache. This
keeps the cache composable and — importantly — relocatable: in M2 the same
decorator moves to the stateless gateway tier in front of the network backend.

### 3.2 Key = the immutable `ObjectLocation`

The cache is keyed by `ObjectLocation` (`volume_id, offset, size`), **not**
`(bucket, key)`. An object location is immutable: overwriting a key allocates a
**new** needle at a **new** location (M0 never mutates a written needle). So:

- **No invalidation on overwrite** — the new version has a new key; the old
  entry simply ages out by eviction.
- **No invalidation on delete** — the metadata mapping is removed, so the location
  is never looked up again; its entry ages out.

This immutability is what makes the cache correct without an invalidation
protocol.

### 3.3 Admission: bias toward small objects

Only objects with `size ≤ cache.max_object_bytes` (default **1 MiB**) are cached.
Larger objects bypass the cache and stream from the backend. This:

- protects the cache for the small-file hot set (one large object can't evict
  thousands of small ones), and
- bounds per-entry cost.

### 3.4 Read-path behavior

- **Full GET**, `size ≤ max_object_bytes`: `cache.get(location)` → on hit, return
  cached bytes (no IO, no CRC); on miss, `inner.get(location, None)` (reads needle
  + verifies CRC), insert into cache, return.
- **Full GET**, `size > max_object_bytes`: `inner.get` directly; not cached.
- **Range GET**: if the full object is already cached (small), slice the range
  from the cached bytes; otherwise `inner.get(location, range)` directly. M1 does
  **not** cache partial reads.
- **HEAD**: metadata only (from `MetadataStore`); never touches the cache.

Writes do not populate the cache in M1 (a freshly written object is not assumed
hot; it enters the cache on first read).

### 3.5 Capacity, eviction, library

- **Library**: `foyer` (used by kokedb). M1 uses its **in-memory** cache only;
  foyer's memory→SSD hybrid is the zero-migration path to the M2 NVMe tier.
- **Capacity**: bounded by total bytes, `cache.max_bytes` (default e.g. 512 MiB),
  weighted by payload length.
- **Eviction**: foyer's W-TinyLFU admission/eviction.
- The cache can be disabled (`cache.enabled = false`) → `CachingBackend` is not
  inserted and reads go straight to the backend.

### 3.6 Metrics

`soma_cache_hits_total`, `soma_cache_misses_total`, `soma_cache_bytes`,
`soma_cache_evictions_total` (see §6).

---

## 4. Structured configuration

Replace M0's ad-hoc `SOMA_*` env reads with a typed `Config` loaded via `figment`,
merging in precedence order **defaults → config file (TOML) → environment**.

```toml
# soma.toml
listen        = "0.0.0.0:9000"   # S3 endpoint
admin_listen  = "0.0.0.0:9001"   # health + metrics (separate port)
data_dir      = "/var/lib/soma"

[storage]
volume_max = "4GiB"

[cache]
enabled          = true
max_bytes        = "512MiB"
max_object_bytes = "1MiB"

[[credentials]]
access_key = "soma"
secret_key = "..."             # overridable via env / k8s Secret
```

- File path from `--config <path>` or `SOMA_CONFIG`; env overrides via `SOMA_*`
  (e.g. `SOMA_LISTEN`, `SOMA_CACHE__MAX_BYTES`).
- Secrets (credentials) are overridable by environment so they can come from a
  Kubernetes `Secret` rather than the ConfigMap.

---

## 5. Operational endpoints (separate admin port)

Liveness, readiness, and metrics live on a **separate admin listener**
(`admin_listen`, default `:9001`), not the S3 port. This avoids two problems:

1. **Path collision** — a bucket could legitimately be named `metrics`/`healthz`;
   keeping ops endpoints off the S3 router removes the ambiguity.
2. **Exposure** — `/metrics` need not be reachable by S3 clients; the admin port
   is typically cluster-internal only.

The admin router carries **no SigV4** (these are infrastructure endpoints):

| Endpoint | Meaning |
| --- | --- |
| `GET /healthz` | Liveness: the process is up. Always `200` unless the process is wedged. |
| `GET /readyz` | Readiness: stores opened and serving. `200` when ready, `503` otherwise. |
| `GET /metrics` | Prometheus text exposition. |

Kubernetes probes point at `/healthz` and `/readyz` on the admin port.

---

## 6. Metrics

- **Library**: `metrics` facade + `metrics-exporter-prometheus`, rendered at
  `/metrics`.
- **Series** (initial set):
  - cache: `soma_cache_hits_total`, `soma_cache_misses_total`, `soma_cache_bytes`,
    `soma_cache_evictions_total`.
  - requests: `soma_s3_requests_total{op,status}`,
    `soma_s3_request_duration_seconds{op}` (histogram).
  - backend: `soma_backend_get_total`, `soma_backend_put_total`.

Instrumentation is added at the S3 dispatch boundary (per-op count/latency) and in
`CachingBackend` (hits/misses).

---

## 7. Containerization & Kubernetes (single-node)

### 7.1 Dockerfile

Multi-stage: `cargo build --release` in a Rust builder, copy the `soma-server`
binary into a slim runtime (debian-slim or distroless). Non-root user; data dir
as a `VOLUME`; expose the S3 and admin ports.

### 7.2 Helm chart (`deploy/helm/soma/`)

M1 is single-node, so a **StatefulSet with `replicas: 1`** and a persistent data
volume:

| Resource | Purpose |
| --- | --- |
| `StatefulSet` (replicas: 1) | The soma node; `volumeClaimTemplates` for the data dir (local PV / block storage). |
| `Service` | ClusterIP exposing the S3 port (+ headless for stable identity, ready for M2). |
| `ConfigMap` | `soma.toml` (non-secret config). |
| `Secret` | Access/secret keys, injected via env. |
| Probes | `livenessProbe` → `/healthz`, `readinessProbe` → `/readyz` on the admin port. |

`values.yaml` exposes: image/tag, resources, storage size/class, cache sizes,
ports, and a credentials `secretRef`. `helm lint` / `helm template` run in CI.

Multi-replica, anti-affinity, and the operator are **M2** (they need the
distributed plane to be meaningful).

---

## 8. Testing (M1)

- **Cache unit** (`CachingBackend` over a counting in-memory `StorageBackend`):
  miss then hit (inner `get` called once for two reads); large object bypasses the
  cache; eviction under capacity; range served from a cached small object.
- **Config**: `figment` precedence (defaults < file < env); bad values rejected.
- **Ops endpoints**: `/healthz` and `/readyz` return `200` **without** auth;
  `/metrics` renders valid exposition and reflects cache counters after some reads.
- **Integration** (extend `src/s3/tests/integration.rs`): the existing
  `object_store` round-trip still passes with the caching backend wired in; a
  repeated GET is served from cache (asserted via the backend `get` counter /
  metrics).
- **Helm**: `helm lint` and `helm template` succeed in CI.

---

## 9. Branch plan for M1 implementation

Each lands as its own reviewed PR to `main` (per the project workflow):

1. `feat/m1-config` — typed `Config` via `figment` (file + env); rework
   `soma-server` startup; tests.
2. `feat/m1-cache` — `CachingBackend` decorator (foyer, memory tier), wired into
   the read path behind a config switch; unit tests.
3. `feat/m1-observability` — admin listener + `/healthz` `/readyz` `/metrics`;
   `metrics` instrumentation for cache + requests.
4. `feat/m1-k8s` — `Dockerfile` + Helm chart (`deploy/helm/soma/`) + CI
   `helm lint`/`template`.

Order reflects dependencies: config underpins the cache; metrics report on the
cache; packaging ships the lot.

---

## 10. Technology choices (additions over M0)

| Concern | Choice |
| --- | --- |
| Read cache | `foyer` (in-memory tier; hybrid mem+SSD reserved for M2) |
| Configuration | `figment` (TOML file + env) |
| Metrics facade | `metrics` + `metrics-exporter-prometheus` |
| HTTP (admin) | `axum` (already used for S3) |
| Container | multi-stage `Dockerfile`, slim runtime |
| Deployment | Helm chart (StatefulSet, single replica) |
