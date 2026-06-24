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

## Status

Early development. The architecture is settled and implementation proceeds by
milestones (M0 single-node skeleton → M1 stateless + cache → M2 distributed
durability → M3 elastic scale → M4 hardening + AI ingest). See the milestone table
in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#13-milestones).

## License

Apache-2.0. See [LICENSE](LICENSE).
