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
| **P2 — local data API** | storage node serves reads over a host-local unix socket (fd-passing). | planned |
| **P3 — client reader** | a reader that short-circuits to the local socket when co-located, and falls back to the gateway otherwise (transparent). | planned |
| **P4 — deployment** | Helm wiring (host socket dir, co-location affinity) + operator docs. | planned |
| **P5 — zero-copy** | `mmap` the passed fd for GB-scale scans, with compaction/CRC/fd-lifetime handled. | planned |

The transport for the local path (P2/P3) is **fd-passing + `mmap` zero-copy**, not a
byte copy — chosen for the scan/ingest workload.

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

## 5. Scope boundary

Soma provides the **oracle** and (later) the **short-circuit data path**. It does
**not** schedule compute — placing scan tasks onto nodes is the compute engine's
planner (or a Kubernetes scheduler plugin). Keeping the scheduler out of soma
preserves the plane separation and avoids unbounded scope.
