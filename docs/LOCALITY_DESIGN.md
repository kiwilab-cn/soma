# Soma Data-Locality Design — Short-Circuit Reads

> Detailed design for **data-locality-aware reads**: letting co-located compute
> (long scans, bulk ingest) read from the storage node that physically holds the
> data, bypassing the gateway and the network — the HDFS DataNode model adapted to
> soma's object-store architecture.
> Parent: [`ARCHITECTURE.md`](./ARCHITECTURE.md).

## 1. Motivation & framing

Soma's heaviest consumers run **long scans and batch ingest**. For those, moving
compute to data beats streaming bytes through the gateway: a co-located reader can
read the local volume directly, with no network hop and no gateway round-trip.

This consciously bends the "gateway separate from storage" boundary: for these
workloads, compute pods **co-locate** with storage pods, and reads short-circuit to
the local node. Pure-S3 clients are unaffected — they keep using the gateway.

The information needed already exists inside soma: an object maps to a placement
group, and the PG table names the holding nodes. The gaps are (1) **exposing** that
mapping, (2) **topology** so a scheduler can tell "local" from "remote", and (3) a
**local data path** that avoids the network. This document covers the phased plan;
**P1 (the locations oracle + topology) is implemented**.

## 2. Phasing

| Phase | Delivers | Status |
| --- | --- | --- |
| **P1 — locations oracle + topology** | `GET object?location` reports the holding nodes, their zone/host, and the data layout; `NodeInfo` carries `zone`/`host`. | **done** |
| **P2 — local data API** | storage node serves reads over a host-local unix socket, passing the volume **file descriptor** (`SCM_RIGHTS`). | **done** |
| **P3 — client reader** | `soma-client`: short-circuits to the local socket when co-located, falls back to a signed gateway GET otherwise (transparent). | **done** |
| **P4 — deployment** | Helm wiring (storage `localRead` hostPath socket + init container) and a co-location example for the compute side. | **done** |
| **P5 — zero-copy** | `mmap` the passed fd for GB-scale scans (the protocol already carries the framing; the reader chooses `pread` or `mmap`). | planned |
| **P6 — multi-tenant isolation** | per-bucket volume partitioning + per-tenant sockets, so short-circuit reads are safe in a shared multi-tenant deployment (§6). | planned |

The transport for the local path is **fd-passing** (the reader then `pread`s or
`mmap`s the descriptor) — chosen for the scan/ingest workload. The fd is for the
whole **volume** file (raw-fd, not a per-object copy): true zero-copy and shared
page cache. In a single-tenant / dedicated deployment that is safe as-is; for the
**shared multi-tenant** model (tenant = bucket), §6 specifies how per-bucket volume
partitioning + per-tenant sockets keep raw-fd safe without giving up zero-copy.

## 2a. The local data API (P2)

The storage node optionally binds a unix-domain socket (`local_socket_path`,
default off) at a node-local `hostPath`. A reader sends an object id; the node
resolves it to a needle and replies with `{payload_offset, len, crc}` plus the
**volume file descriptor** attached via `SCM_RIGHTS`. The reader reads the payload
straight from the descriptor at `[payload_offset, payload_offset+len)` and verifies
it against `crc` — **no object bytes cross the socket, only the descriptor**. The
crate `soma-localfd` provides both the server (`serve_local_reads`) and a client
(`LocalClient`); P3 wraps the client with gateway fallback.

Two properties hold by construction (see §5): the server reads only the 32-byte
needle header (never the payload), so integrity is the reader's job (it checks the
CRC); and because compaction is copy-to-new + atomic rename, a held descriptor pins
the old inode and reads a consistent snapshot even across a compaction.

## 2b. The client reader (P3)

`soma-client` is the drop-in reader a compute engine links. `SomaClient::get(bucket,
key)` is **transparent**: configured with this process's host and the path to the
co-located node's socket, it

1. resolves the object's holders via the gateway's `?location` oracle;
2. if a holder is on **this** host, reads the bytes through the local socket
   (passed descriptor, `pread`, CRC-verified) — no gateway, no network;
3. otherwise — or on *any* local miss (not co-located, no oracle, a raced/missing
   id, a socket hiccup) — falls back to a **signed S3 GET** against the gateway.

The fallback means reads always succeed if the object exists, so the same client
works on- and off-cluster; locality is a pure optimization. The local socket
connection is reused across reads (reconnected on error). The gateway calls are a
self-contained blocking HTTP + SigV4 signer that mirrors the gateway's verifier
(no AWS SDK dependency). Short-circuiting is disabled by leaving the host or socket
path empty, making `SomaClient` a plain S3 reader.

## 3. The locations oracle (P1)

### Topology

`NodeInfo` gains `zone` (failure domain) and `host` (the unit at which a reader can
short-circuit). A storage node reports them at registration, sourced from the
orchestrator — `kubernetes.io/hostname` for the host (via the downward API
`spec.nodeName`), `topology.kubernetes.io/zone` for the zone (set per nodepool;
node labels aren't exposable through the downward API). Empty fields mean "unknown",
so locality degrades to a no-op rather than an error.

### API

`GET /{bucket}/{key}?location` — a soma extension (SigV4-authenticated like any
object request) returning a JSON document:

```json
{
  "key": "data/part-0001",
  "object_id": 12345,
  "size": 1048576,
  "layout": { "type": "replicated", "width": 3 },
  "nodes": [
    { "node_id": "soma-storage-0", "endpoint": "http://...:9200",
      "zone": "az-a", "host": "node-7", "role": "replica" }
  ]
}
```

For erasure-coded objects, `layout` is `{"type":"erasure","data_shards":k,"parity_shards":m}`
and each node's `role` is `data:i` or `parity:i`. The endpoint returns `501` in
single-node deployments (no oracle — nothing to schedule across).

A scheduler resolves an object once, places its scan task on a node whose `host`
matches a holding node, and (in P2/P3) reads locally there.

### Internals

The gateway already maintains a live `Placement` view (membership + PG table); it is
wrapped by a `PlacementOracle` that adds the cluster's data layout to assign each
holding node a role and attach its topology. The oracle is a cheap, read-only
projection sharing the gateway's existing placement `Arc` — no new state, no extra
round-trips beyond the object's metadata lookup.

## 4. The erasure-coding caveat

Replicated objects have clean per-node locality: any one replica node serves the
whole object. Erasure-coded objects do not — a range read reconstructs the object
from `k` shards across `k` distinct nodes, so there is no single local node. Locality
is therefore a **replicated-data** property: keep hot/scan-heavy data replicated and
reserve erasure coding for cold data. The oracle reports the EC layout faithfully so
a scheduler can make that call.

## 5. Kubernetes volume & scheduling requirements

Locality only pays off if "the node the storage pod runs on" is genuinely where the
bytes are, and stays put. That constrains how the storage volume is provisioned and
how compute is scheduled.

### The data volume must be node-local — but it is still a PVC

Use a **local** volume (the `local` PV type, or a local-path provisioner) for the
storage StatefulSet's data, **not** network block storage (EBS / PD / Ceph-RBD).
This is *not* "don't use a PVC" — a `local` PV is a normal PVC/PV, just with
`nodeAffinity`. Network block storage breaks locality two ways:

1. **Bytes still cross the network.** Even with compute co-located, the storage pod
   reads the block device over the storage network — you save the gateway hop but
   never get a true local-disk read, which is the whole point for scans.
2. **The host drifts.** A network PV lets the storage pod reschedule onto another
   node and re-attach, so the `host` the oracle reported goes stale and any compute
   placed by it is now misplaced.

A `local` PV's `nodeAffinity` **pins** the storage pod to the node holding its disk —
which is exactly what makes the locality chain stable:

```
compute pod --podAffinity(hostname)--> storage pod --localPV.nodeAffinity--> node --> local disk
```

The usual objection to local volumes — "if the node dies, the pod can't migrate and
the data is stranded" — does not apply here, because **durability lives at the soma
layer** (replication / erasure coding across nodes), not at the volume layer. So the
pairing is deliberate: node-local volumes for speed, soma replication for durability.

Provisioner options: TopoLVM or the sig-storage local-static-provisioner or OpenEBS
LocalPV for production (scheduler-aware, capacity-tracked); the Rancher
local-path-provisioner for simple setups. The chart's
`storage.persistence.storageClass` selects it.

### Compute needs the socket, not the data volume

For the short-circuit read (P2/P3) the compute pod does **not** mount the data
volume. The storage node passes the open volume file's descriptor over a unix socket
(`SCM_RIGHTS`); the received fd references the same kernel *open file description*,
independent of mount namespaces and paths, so the compute process can `mmap`/`read`
it directly (sharing the page cache — genuinely zero-copy). What compute *does* need
is to reach the **socket**, so the socket directory is shared between the two pods
via a node-local **`hostPath`** (an `emptyDir` is per-pod and cannot be shared across
pods). Plus a `podAffinity` (topologyKey `kubernetes.io/hostname`, selecting the
storage pods) to co-schedule compute onto a node holding a replica.

Two caveats on the fd path:

- It requires a **shared-kernel runtime** (runc). VM-isolated runtimes (Kata,
  gVisor) give each pod its own kernel, so a passed host fd does not cross the
  boundary — those deployments fall back to the gateway read path.
- Passing the **raw volume fd** grants the reader access to the whole volume file
  (which holds many objects' needles). That is fine for a single-tenant / dedicated
  deployment. For shared multi-tenant SaaS, §6 keeps raw-fd safe with per-bucket
  volume partitioning + per-tenant sockets (preferred — preserves zero-copy); a
  per-object `memfd` copy is the fallback where partitioning is not possible.

### Alternative: DaemonSet + hostPath

Running storage as a **DaemonSet over a `hostPath` data dir** (one storage pod per
node, owning that node's disk) is a simpler pure-node-local model — compute then uses
plain node affinity. It changes the identity model (node id per node rather than per
StatefulSet ordinal), so the current design keeps **StatefulSet + local PV**; the
DaemonSet model is recorded as a viable alternative.

### Enabling it in the chart (P4)

Short-circuit reads are **off by default**. Setting `storage.localRead.enabled=true`
makes the storage StatefulSet:

- mount a node-local `hostPath` (`storage.localRead.hostPath`, default `/run/soma`),
- run a small **root init container** that prepares that directory (so the non-root
  soma process can bind the socket there), and
- set `SOMA_LOCAL_SOCKET_PATH` so the storage role binds the socket on bind, which the
  server then `chmod`s to `0666` so a co-located reader of any uid can connect.

The compute side is **not** part of the chart (it is your engine). `deploy/examples/
compute-colocation.yaml` shows the three things it needs: a `podAffinity` to storage
pods (hostname topology), a `hostPath` mount of the same socket directory, and the
env that feeds the client's config (`SOMA_HOST` from `spec.nodeName`, the socket path,
the gateway endpoint + keys). The reader short-circuits when co-located and falls back
to the gateway otherwise, so over-/under-provisioning compute replicas is safe.

Caveats (consistent with §5/§6): this needs a **shared-kernel runtime** and a root
init container (so it does not satisfy the `restricted` Pod Security Standard — gate
it behind the opt-in), and the unscoped `0666` socket means it is for **single-tenant
/ dedicated** deployments until per-tenant socket scoping (§6) lands.

## 6. Multi-tenant isolation (P6 — planned)

The target deployment is a **shared multi-tenant SaaS**: one soma cluster serves
many tenants, with **tenant = bucket**, plus a shared `global` bucket that every
tenant may **read** (but not write). This makes the raw-fd exposure a real concern:
soma packs objects into volumes in write order, so a single volume file mixes many
buckets' (tenants') needles. A per-tenant reader handed the **whole** volume's
descriptor could `pread` another tenant's bytes — and the local socket (P2) has no
authorization today, so any process that reaches it can request any object id.

This section is the design to make short-circuit reads safe under that model. It is
**not yet implemented**; until it is, short-circuit reads are for single-tenant /
dedicated deployments only (a shared deployment must keep the socket off and read
through the gateway, except possibly for the `global` bucket).

### Design: partition volumes by bucket, scope sockets per tenant

The tenant boundary (a bucket) maps directly onto the storage layout:

1. **Partition volumes by bucket.** A volume only ever holds **one bucket's**
   objects; the shared `global` bucket gets its own volumes. So one volume file =
   one trust domain. (Today objects from all buckets share the active volume.)
2. **Tag each volume with its owning bucket.** Storage nodes are bucket-blind today
   (they key on `object_id` only); partitioning makes `volume → bucket` known
   node-locally, which is what lets the node enforce the boundary at the socket.
3. **One local socket per tenant.** A tenant's compute mounts only its **own**
   tenant's socket — a `hostPath` subdirectory whose permissions exclude other
   tenants. soma serves, on that socket, descriptors **only** for that tenant's
   volumes plus the shared `global` volumes, and refuses any other object id. Two
   independent barriers hold: the OS (a tenant cannot reach another tenant's socket)
   and soma (the socket will not hand out a foreign volume's descriptor).
4. **`global` is read-shared**, so exposing a `global` volume's descriptor on any
   tenant's socket leaks nothing — every tenant is already entitled to read it.

This keeps the **raw-fd zero-copy** that the scan workload needs: the descriptor a
reader receives is for a volume that holds only data it is entitled to. A per-object
`memfd` copy stays as the fallback for environments where per-bucket partitioning is
impractical, trading one copy for isolation.

### Out of scope here: write authorization

Short-circuiting is **read-only**, so this design covers cross-tenant *reads*.
Enforcing "a tenant may write only its own bucket (and read `global`)" on the write
path is **per-bucket write authorization** (bucket policy / IAM) at the gateway —
soma's auth today only checks that the access key is valid, with no per-bucket
policy. That is a separate prerequisite for the SaaS model, tracked independently of
the locality work.

## 7. Scope boundary

Soma provides the **oracle** and (later) the **short-circuit data path**. It does
**not** schedule compute — placing scan tasks onto nodes is the compute engine's
planner (or a Kubernetes scheduler plugin). Keeping the scheduler out of soma
preserves the plane separation and avoids unbounded scope.
