# Deploying Soma

How to stand up a real Soma cluster — locally for development and integration
testing, or on Kubernetes — and how a consumer (a database engine, a data
pipeline, any S3 client) connects to it.

A cluster is three roles, all the **same** `soma-server` binary (role chosen by
`SOMA_ROLE` / config `role`):

| Role | What it is | Listens | State |
|---|---|---|---|
| `meta` | Strongly-consistent metadata node (gRPC) — buckets, key→object map, membership, placement | `:9100` | redb on a PV |
| `storage` | Stores object bytes as needles in volume files (gRPC); registers with `meta` | `:9200` | volumes on a PV |
| `gateway` | Stateless S3 front-end; resolves placement from `meta`, reads/writes `storage` | `:9000` S3, `:9001` admin | none |

`standalone` packs all of it into one process (the quickest smoke test:
`cargo run --bin soma-server`). For anything resembling production you run the
three roles separately, with object bytes replicated (or erasure-coded) across
storage nodes.

The gateway **waits for `replication_factor` storage nodes to register** with the
meta node before it serves, so start order sorts itself out — bring everything up
together and the cluster converges in a few seconds.

---

## 1. Local cluster with Docker Compose (recommended)

[`deploy/compose/docker-compose.yml`](../deploy/compose/docker-compose.yml)
brings up the full topology — one meta, **three** storage nodes, one gateway —
from the single image, with 3-way replication (write quorum 2):

```sh
docker compose -f deploy/compose/docker-compose.yml up --build
```

Or via the Makefile (`make help` lists everything):

```sh
make up      # build + start the cluster
make ready   # wait for the gateway to report ready
make smoke   # S3 create/put/get/delete roundtrip (needs python3 + boto3)
make logs    # tail all roles
make down    # stop (keeps data volumes; `make clean` also wipes them)
```

The gateway publishes:

- **`http://localhost:9000`** — the S3 endpoint (point clients here)
- **`http://localhost:9001`** — admin: `/healthz`, `/readyz`, `/metrics`

Wait for readiness, then it is serving:

```sh
curl -fsS http://localhost:9001/readyz && echo "  cluster ready"
```

Tear down (add `-v` to also wipe the data volumes):

```sh
docker compose -f deploy/compose/docker-compose.yml down
```

### Erasure coding (instead of replication)

By default the cluster 3-way **replicates** (3× storage, survives 2 node losses).
To run it **erasure-coded** instead — Reed-Solomon `k=4 + m=2`, **1.5×** storage,
still survives 2 node losses — layer the erasure overlay, which grows the cluster
to the required **6** storage nodes (one shard per distinct node) and switches the
gateway and meta to erasure mode:

```sh
make ec-up         # 6 storage + gateway/meta in erasure mode (k=4 m=2)
make ready
make ec-degraded   # write an object, kill 2 of 6 nodes, read it back reconstructed
make ec-down       # or ec-clean to wipe volumes
```

Erasure coding is **opt-in and a deploy-time choice** (it is not a live migration
from a populated replicated cluster). It needs **at least `data_shards +
parity_shards` storage nodes** — the gateway refuses to serve until that many
register. The same `erasure.enabled` / `data_shards` / `parity_shards` settings
must be set on **both** the gateway (which encodes) and the meta node (whose
rebalance controller reconstructs shards); storage nodes need nothing — they hold
opaque shard bytes. Tune `write_quorum` (`0` → `data_shards + 1`) to trade write
availability against the durability margin. The same knobs exist in the Helm
chart under `erasure.*` (see §5).

## 2. Local cluster without Docker

The same five processes, straight from a release build — handy when you don't
want a container runtime. Each role is just the binary with different env vars:

```sh
cargo build --release --bin soma-server
BIN=target/release/soma-server

# metadata node
SOMA_ROLE=meta SOMA_LISTEN=127.0.0.1:9100 SOMA_DATA_DIR=/tmp/soma/meta $BIN &

# three storage nodes (each its own port, id, advertised endpoint, data dir)
for n in 0 1 2; do
  SOMA_ROLE=storage SOMA_LISTEN=127.0.0.1:920$n \
  SOMA_META_ENDPOINT=http://127.0.0.1:9100 \
  SOMA_NODE_ID=storage-$n SOMA_ADVERTISE_ENDPOINT=http://127.0.0.1:920$n \
  SOMA_DATA_DIR=/tmp/soma/storage-$n $BIN &
done

# stateless gateway (S3 on :9000)
SOMA_ROLE=gateway SOMA_LISTEN=127.0.0.1:9000 SOMA_ADMIN_LISTEN=127.0.0.1:9001 \
SOMA_META_ENDPOINT=http://127.0.0.1:9100 \
SOMA_REPLICATION_FACTOR=3 SOMA_WRITE_QUORUM=2 \
SOMA_ACCESS_KEY=soma SOMA_SECRET_KEY=soma-secret $BIN &

curl -fsS --retry-connrefused --retry 40 --retry-delay 1 http://127.0.0.1:9001/readyz
```

## 3. Smoke test (S3 roundtrip)

Any S3 SDK works — **path-style**, region **`us-east-1`**, keys
`soma` / `soma-secret`. With Python/boto3:

```python
import boto3
from botocore.config import Config
s3 = boto3.client(
    "s3", endpoint_url="http://127.0.0.1:9000",
    aws_access_key_id="soma", aws_secret_access_key="soma-secret",
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
)
s3.create_bucket(Bucket="demo")
s3.put_object(Bucket="demo", Key="seg/0001.bin", Body=b"hello")
assert s3.get_object(Bucket="demo", Key="seg/0001.bin")["Body"].read() == b"hello"
```

Create / put / get / range-read / list / delete all behave as S3 expects.

---

## 4. Connecting a consumer (e.g. a database engine)

A consumer reaches Soma exactly like any S3 service — through the **gateway**:

- **Endpoint** — the gateway S3 address (`http://gateway:9000` in-cluster, or the
  published address). TLS terminates at your ingress/load balancer in production.
- **Region** — `us-east-1` (Soma ignores it, but SDKs require one).
- **Addressing** — **path-style** (`endpoint/bucket/key`), not virtual-host.
- **Credentials** — the configured access/secret key (SigV4).
- **Bucket** — create it once (`CreateBucket`) or let the consumer create its own.

From the Rust `object_store` crate (what many Rust engines use):

```rust
let store = object_store::aws::AmazonS3Builder::new()
    .with_endpoint("http://gateway:9000")
    .with_region("us-east-1")
    .with_bucket_name("my-bucket")
    .with_access_key_id("soma")
    .with_secret_access_key("soma-secret")
    .with_allow_http(true) // drop for TLS
    .build()?;
```

Soma also ships a first-party `object_store` implementation, **SomaStore**, that
adds locality-aware reads and other extras on top of the plain S3 path — see
[`src/objectstore/README.md`](../src/objectstore/README.md) for the full surface
(and [`STREAMING_APPEND.md`](STREAMING_APPEND.md) for streaming/append). A
consumer can start on the generic S3 client and move to SomaStore later without
changing its data model.

---

## 5. Kubernetes (Helm)

The chart in [`deploy/helm/soma`](../deploy/helm/soma) deploys the same topology
as a stateless gateway `Deployment` plus `meta`/`storage` `StatefulSet`s with
PVs, wired by gRPC:

```sh
docker build -t soma:0.1.0 .
helm install soma deploy/helm/soma \
  --set image.repository=soma --set image.tag=0.1.0 \
  --set storage.replicaCount=3 \
  --set credentials.accessKey=... --set credentials.secretKey=...
```

Consumers reach it via the gateway `Service` on the S3 port. Tune replication,
erasure coding, cache, durability, encryption, and per-bucket QoS through
[`values.yaml`](../deploy/helm/soma/values.yaml). For data-locality
(short-circuit local reads) point `storage.persistence.storageClass` at a
node-local PV class — see [`LOCALITY_DESIGN.md`](LOCALITY_DESIGN.md).

---

## 6. Configuration reference

Config layers **defaults → TOML (`--config` / `SOMA_CONFIG`) → environment**.
Env keys use the `SOMA_` prefix with `__` for nesting
(`SOMA_STORAGE__HEARTBEAT_INTERVAL_SECS`). The knobs that matter most for a
cluster:

| Setting | Env | Role | Meaning |
|---|---|---|---|
| `role` | `SOMA_ROLE` | all | `meta` / `storage` / `gateway` / `standalone` |
| `meta_endpoint` | `SOMA_META_ENDPOINT` | storage, gateway | where to reach the meta node |
| `node_id` | `SOMA_NODE_ID` | storage | stable identity (e.g. pod name) |
| `advertise_endpoint` | `SOMA_ADVERTISE_ENDPOINT` | storage | address other nodes reach this node at |
| `replication_factor` | `SOMA_REPLICATION_FACTOR` | gateway | replicas per object |
| `write_quorum` | `SOMA_WRITE_QUORUM` | gateway | replicas that must ack a write |
| `storage.durability` | `SOMA_STORAGE__DURABILITY` | storage | `per_write` / `group_commit` / `async` (see [STORAGE_MODEL.md](STORAGE_MODEL.md)) |
| `erasure.enabled` | `SOMA_ERASURE__ENABLED` | gateway, meta | stripe with Reed-Solomon `k+m` instead of replication |
| access / secret key | `SOMA_ACCESS_KEY` / `SOMA_SECRET_KEY` | gateway | S3 credentials |

Full schema and defaults live in `src/server/src/config.rs` and
[`M1_DESIGN.md`](M1_DESIGN.md#4-structured-configuration); the admin endpoints
(`/healthz`, `/readyz`, `/metrics`) are described in the top-level
[`README.md`](../README.md).
