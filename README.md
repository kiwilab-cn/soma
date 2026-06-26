# Soma

**A fast, secure, and resilient object storage system tailored for the AI era.**

100% Rust. Soma swallows, stores, and streams massive raw datasets for modern
vector databases and data lakes — and makes them *understood*, not just *stored*.

Soma is a distributed object store in the class of MinIO / SeaweedFS / JuiceFS,
with one differentiator the others structurally cannot copy:

> **Objects ingested into Soma are automatically vectorized and graph-ified** —
> the store hands back semantically searchable, graph-traversable data, not just
> bytes.

## Design pillars

- **Distributed & infinitely scalable** — add nodes; data rebalances in the
  background, imperceptibly. Reliability over rebalance speed.
- **Stateless serving tier** — gateway pods hold no durable state; kill, scale, and
  roll them at will.
- **Kubernetes-native** — operator + CRDs, rolling upgrade, autoscaling, local-PV
  cache affinity.
- **High performance** — zero-copy IO, async, sequential write aggregation, O(1)
  reads via a compact in-RAM index over packed small objects.
- **High security** — TLS in transit, envelope encryption at rest, S3 SigV4 auth,
  hard multi-tenant isolation.
- **Built-in cache** — local NVMe + memory hot tier on the read path.
- **S3-compatible** — works with existing S3 SDKs and the Rust `object_store` crate.

## Architecture

Soma splits into three planes: a **stateless data plane**, a small
**strongly-consistent Raft metadata plane**, and a **pluggable storage backend**
(local FS → replication → erasure coding → cloud delegation). Small objects are
packed into large append-only *volume* files as *needles* with a compact in-RAM
index — the design lesson shared by Facebook Haystack, SeaweedFS, and CDN caches
(Apache Traffic Server, Squid Rock store).

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — the full system design.
- [`docs/MVP_DESIGN.md`](docs/MVP_DESIGN.md) — the M0 (single-node skeleton) design.
- [`docs/M1_DESIGN.md`](docs/M1_DESIGN.md) — the M1 (read cache + cloud-native readiness) design.
- [`docs/M2_DESIGN.md`](docs/M2_DESIGN.md) — the M2 (distributed durability) design.
- [`docs/M3_DESIGN.md`](docs/M3_DESIGN.md) — the M3 (elastic scale: membership, rebalance, self-heal) design.
- [`docs/M4_DESIGN.md`](docs/M4_DESIGN.md) — the M4 (erasure coding, encryption, multi-tenant QoS) design.

## Run

```sh
cargo run --bin soma-server                  # defaults below
cargo run --bin soma-server -- --config soma.toml   # or a TOML config file
# defaults: listen 0.0.0.0:9000, data ./soma-data, key soma / soma-secret
```

Configuration is layered **defaults → TOML file (`--config` / `SOMA_CONFIG`) →
environment**. Env overrides use the `SOMA_` prefix with `__` for nesting, e.g.
`SOMA_LISTEN`, `SOMA_DATA_DIR`, `SOMA_ACCESS_KEY`, `SOMA_SECRET_KEY`,
`SOMA_CACHE__MAX_BYTES`. See [`docs/M1_DESIGN.md`](docs/M1_DESIGN.md#4-structured-configuration).

A separate **admin port** (`SOMA_ADMIN_LISTEN`, default `:9001`) serves
`GET /healthz` (liveness), `GET /readyz` (readiness), and `GET /metrics`
(Prometheus) — no auth, off the S3 endpoint.

Point any S3 client at it (path-style, region `us-east-1`). With the Rust
`object_store` crate:

```rust
let store = object_store::aws::AmazonS3Builder::new()
    .with_endpoint("http://127.0.0.1:9000")
    .with_region("us-east-1")
    .with_bucket_name("my-bucket")
    .with_access_key_id("soma")
    .with_secret_access_key("soma-secret")
    .with_allow_http(true)
    .build()?;
```

## Deploy

A complete local cluster (one meta, three storage, one gateway) in one command:

```sh
docker compose -f deploy/compose/docker-compose.yml up --build
# S3 at http://localhost:9000, admin at http://localhost:9001
```

On Kubernetes, via the Helm chart:

```sh
docker build -t soma:0.1.0 .                     # one image, all roles
helm install soma deploy/helm/soma \
  --set image.repository=soma --set image.tag=0.1.0 \
  --set storage.replicaCount=3 \
  --set credentials.accessKey=... --set credentials.secretKey=...
```

See **[`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md)** for the full guide — local
(Compose or bare processes), Kubernetes, the S3 smoke test, how a consumer
connects, and the configuration reference.

The chart deploys the distributed three-role topology: a stateless **gateway**
`Deployment` (S3 + admin), a **metadata** `StatefulSet` (1 replica, PV), and a
**storage** `StatefulSet` (`storage.replicaCount` replicas, PVs) — wired together
by gRPC, with N-way quorum replication (`replication.factor` / `writeQuorum`). The
gateway is reached via its `Service` on the S3 port; tune everything via
[`deploy/helm/soma/values.yaml`](deploy/helm/soma/values.yaml).

## Status

Early development. **M0 (single-node skeleton) is complete**: an S3-compatible
endpoint (bucket lifecycle, single-part object CRUD, range reads, ListObjectsV2,
multipart upload, conditional writes, SigV4) over the volume + needle on-disk
format, durable across restarts — validated end-to-end with the `object_store`
client. Implementation proceeds by milestones (M0 → M1 stateless + cache → M2
distributed durability → M3 elastic scale → M4 hardening + AI ingest). See the
milestone table in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#13-milestones).

## License

Apache-2.0. See [LICENSE](LICENSE).
