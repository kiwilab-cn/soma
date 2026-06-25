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
| **P3 — client reader** | a reader that short-circuits to the local socket when co-located, and falls back to the gateway otherwise (transparent). | planned |
| **P4 — deployment** | Helm wiring (host socket dir, co-location affinity) + operator docs. | planned |
| **P5 — zero-copy** | `mmap` the passed fd for GB-scale scans (the protocol already carries the framing; the reader chooses `pread` or `mmap`). | planned |

The transport for the local path is **fd-passing** (the reader then `pread`s or
`mmap`s the descriptor) — chosen for the scan/ingest workload. The fd is for the
whole **volume** file (raw-fd, not a per-object copy): true zero-copy and shared
page cache, valid for soma's trust model (§5); a per-object `memfd` mode is the
isolation fallback if untrusted/multi-tenant compute is ever co-located.

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
  (which holds many objects' needles). That is fine for a trusted, same-tenant
  compute engine; for stronger isolation, P2 can copy the needle into a per-object
  `memfd` and pass that instead, trading one copy for isolation.

### Alternative: DaemonSet + hostPath

Running storage as a **DaemonSet over a `hostPath` data dir** (one storage pod per
node, owning that node's disk) is a simpler pure-node-local model — compute then uses
plain node affinity. It changes the identity model (node id per node rather than per
StatefulSet ordinal), so the current design keeps **StatefulSet + local PV**; the
DaemonSet model is recorded as a viable alternative.

## 6. Scope boundary

Soma provides the **oracle** and (later) the **short-circuit data path**. It does
**not** schedule compute — placing scan tasks onto nodes is the compute engine's
planner (or a Kubernetes scheduler plugin). Keeping the scheduler out of soma
preserves the plane separation and avoids unbounded scope.
