# Soma M2 Design — Distributed Durability

> Detailed design for milestone **M2**.
> Parent: [`ARCHITECTURE.md`](./ARCHITECTURE.md). Builds on M0 (single-node store,
> complete) and M1 (cache + cloud-native readiness, complete). M2 makes object
> **bytes** distributed and durable across a cluster.

## 1. Goal & framing

M0/M1 ran as one process holding the metadata store and storage backend in-memory
behind two traits. M2 turns that monolith into a **networked, multi-role cluster**
so object bytes survive node loss and the serving tier becomes stateless.

Two decisions shape the whole milestone:

**No consensus in soma.** Distributed durability of *bytes* does **not** need Raft.
Replication is **quorum-based** (write N replicas, ack after W succeed) with the
**metadata as the single source of truth** for what is committed — the Dynamo / S3
model. Raft would only be needed for the *metadata* plane, and even there soma
will not own it: metadata high-availability is delegated to a future distributed
metadata engine (see §11). For M2 the metadata runs **single-node behind the
`MetadataStore` trait** — durable (on a PV) but not yet HA.

**lethe-store is not part of M2.** lethe-store is soma's *AI index engine* (auto
vectorize / graph-ify ingested content) and lands in **M4**, sitting on top of the
stored objects. The small, structural metadata (bucket/object index) does not need
it and stays `redb`. M2 touches neither lethe-store nor Raft.

### In scope (M2)

- Split the monolith into three roles (gateway / storage node / metadata),
  one binary selected by `--role`.
- A network protocol (gRPC / `tonic`) between them.
- N-way replication of needles with quorum writes and read failover.
- Consistent-hash **placement groups** deciding which storage nodes hold which
  objects.
- Failure self-heal (re-replication on node loss, bitrot scrub) and background
  rebalance on scale-out.

### Out of scope (deferred)

- **Metadata HA / consensus** — delegated to a future distributed metadata engine
  via the `MetadataStore` trait; M2 metadata is single-node.
- **Erasure coding** — M4 (M2 ships N-way replication first).
- **AI ingest + lethe-store** — M4.
- **True chunked large objects**, GC/compaction maturity, write aggregation —
  carried forward.

---

## 2. The model change: logical needle id + node-local index

In M0 an `ObjectLocation` is a *physical* `{volume, offset, size}`. That breaks in
a cluster: the same needle written to N storage nodes sits at a **different local
offset on each node**, so the metadata cannot store one offset.

The fix (also SeaweedFS's model, and it **reuses M0's hot index unchanged**):

> Metadata stores only **logical** info: `(bucket, key) → {object_id, size, etag,
> version, created_at}`. Each **storage node keeps its own local hot index**
> (`object_id → local offset`) — exactly M0's `HotIndex`. A read computes the
> object's replica nodes from the placement ring, asks any live node for
> `object_id = N`, and the node resolves it locally.

Consequences:

- M0's `LocalFsBackend` (volume + needle + hot index) becomes the **storage
  node's local engine**, essentially as-is.
- The gateway no longer holds a backend; it talks to storage nodes over the
  network.
- `ObjectLocation`'s physical `{volume, offset}` leaves the metadata; placement is
  computed (§4), and offsets live only in node-local indexes.

---

## 3. Roles — one binary, `--role`

```
                         S3 clients
                              │
            ┌─────────────────▼──────────────────┐  stateless · k8s Deployment · scale freely
            │  GATEWAY  (--role gateway)          │  S3 + SigV4 · ring · read cache (M1)
            │  MetaClient ─┐     ┌─ StorageClient │
            └──────────────┼─────┼────────────────┘
                  gRPC     │     │  gRPC
            ┌──────────────▼─┐  ┌▼───────────────────────┐  StatefulSet · scalable
            │ METADATA       │  │ STORAGE NODE (--role     │  local volume+needle+index (M0)
            │ (--role meta)  │  │ storage) × N             │  serves PutNeedle/GetNeedle
            │ redb · 1 node  │  └──────────────────────────┘
            └────────────────┘
```

- **Gateway** — stateless. Owns the S3 protocol + SigV4, the placement ring, the
  read cache (M1's `CachingBackend` moves here), and clients to the metadata and
  storage services. No durable local state; kill/scale/roll freely.
- **Storage node** — owns local volumes. Runs M0's engine behind a `StorageService`
  gRPC API (`PutNeedle`, `GetNeedle`, `DeleteNeedle`, plus `ListNeedles` / `Scrub`
  for repair). Scales horizontally as a `StatefulSet`.
- **Metadata** — runs `RedbMetaStore` behind a `MetaService` gRPC API. Single node
  in M2 (durable, not HA). The gateway's `MetaClient` implements the
  `MetadataStore` trait against it, so the rest of the gateway is unchanged.

The `MetadataStore` and `StorageBackend` traits are the seams: a remote client on
the gateway side, a local impl on the node side.

---

## 4. Placement — consistent-hash placement groups

To keep per-object metadata tiny **and** make rebalance tractable, placement is by
**group**, not per object (the Ceph PG / SeaweedFS volume model):

- A fixed number of **placement groups** `G` (e.g. 4096). `group = hash(object_id)
  % G`, computed — never stored per object.
- A small **cluster-state** table `group → [replica node ids]` (G entries) lives in
  the metadata store, derived from the current storage-node membership and the
  consistent-hash ring (virtual nodes for balance).
- Per-object metadata therefore stays `{object_id, size, etag, version}`; the
  replica set is `group_table[hash(object_id) % G]`.

Why groups rather than per-object hashing:

- Per-object metadata stays minimal (no node list per object).
- Membership change reassigns **groups**, and rebalance migrates **groups** — a
  bounded, coarse unit — instead of touching every object independently.

> **M2b ships the simpler form first**: a direct consistent-hash ring over the
> configured storage nodes (virtual nodes for balance) computes an object's
> replica set straight from `hash(object_id)` — no `group_table` indirection,
> since the node set is static. The placement-**group** table and the
> membership-driven rebalance it enables land with **M2c** (self-heal +
> rebalance), where a mutable mapping is what's actually needed.

---

## 5. Replication & the write/read protocol

N-way replication (default **N = 3**), quorum write **W** (default 2), read any
live replica. The write order preserves M0's durability rule — **bytes durable on
a quorum before the metadata commit** — so a crash is always safe.

**PUT** (at the gateway):

```
1. SigV4 + resolve bucket (MetaClient).
2. object_id = MetaClient.next_object_id().            // cluster-unique, monotonic
3. nodes = group_table[hash(object_id) % G].           // N replica nodes
4. PutNeedle(object_id, bytes) -> each node appends a needle + fsync.
   Wait for W acks.  (< W reachable -> fail / retry other nodes.)
5. MetaClient.put_object(bucket, key, {object_id, size, etag, ...}, cond).
   // CAS evaluated here; the object becomes live only now.
6. ACK.
```

A crash between 4 and 5 leaves needles on storage nodes that no committed object
references — orphans, reclaimed by GC; never read (read-after-write only sees
committed). Durability holds because metadata commits only after W durable copies.

**GET**:

```
1. meta = MetaClient.get_object(bucket, key).           // {object_id, size, ...}
2. nodes = group_table[hash(meta.object_id) % G].
3. GetNeedle(object_id, range) from a live node; on failure try the next replica.
4. Stream bytes. (Gateway read cache serves hot objects first.)
```

All committed replicas are byte-identical (a needle is immutable), so any replica
read is correct. **DELETE** removes the metadata mapping; needles become orphans
(tombstone + GC).

---

## 6. Consistency contract (preserved)

The contract M0 offered consumers is unchanged in M2:

- **Strong read-after-write** — the metadata is the authority and is strongly
  consistent (single node in M2). A committed PUT is immediately visible.
- **Conditional writes** (`If-Match` / `If-None-Match`) — still evaluated as a CAS
  inside the metadata store's transaction (exactly as M0), now reached over the
  network. (When metadata HA arrives via a distributed engine, the CAS moves into
  that engine's transaction — the trait is unchanged.)
- **Object versioning** — unchanged.

The data plane stays consistent because "live" is defined solely by the metadata,
and a write commits metadata only after a durable quorum.

---

## 7. Metadata model & cluster state

`RedbMetaStore` gains/changes:

- `ObjectRecord`: `{object_id, size, etag, version, created_at}` — the physical
  `ObjectLocation` is gone (placement is computed; offsets are node-local).
- **Cluster state** tables: storage-node membership (`node_id → {address,
  status, generation}`) and the `group → [node ids]` placement table. These are
  small, strongly-consistent records the gateway caches and watches.
- `next_object_id` remains the monotonic allocator (cluster-unique ids).

`MetadataStore` trait additions: cluster-state read/update (membership, group
table) and a way to enumerate objects by group (for repair/rebalance).

---

## 8. Network protocol (gRPC / `tonic`)

`tonic` + `prost` (matching the house style — kokedb uses `tonic`). Protobuf
service definitions, generated via `tonic-build`.

- **`StorageService`** (gateway → storage node, and node → node for repair):
  `PutNeedle(object_id, stream bytes)`, `GetNeedle(object_id, range) -> stream`,
  `DeleteNeedle(object_id)`, `ListNeedles(group) -> stream` (repair),
  `Scrub(...)`.
- **`MetaService`** (gateway → metadata): the `MetadataStore` surface
  (bucket/object CRUD, list, `next_object_id`, conditional writes) plus
  cluster-state get/update.

Streaming `PutNeedle` / `GetNeedle` keeps large transfers off a single message and
ready for chunked objects later.

---

## 9. Membership & discovery (Kubernetes)

- **Storage nodes**: `StatefulSet` `soma-storage-{0..N-1}` with a headless Service
  for stable per-pod DNS. On start each node registers with the metadata service
  (address, ordinal); the metadata service maintains the membership table.
- **Metadata**: `StatefulSet`, `replicas: 1` (M2), on a PV.
- **Gateways**: `Deployment` + HPA; discover the metadata service by DNS and watch
  cluster state.
- The **group → node** table is (re)computed from the current membership by a
  controller (the metadata node owns this in M2) and stored in cluster state.

---

## 10. Failure self-heal & rebalance

Background, low-priority, reliability-over-speed (an explicit project constraint):

- **Bitrot scrub** — each storage node periodically re-verifies needle CRCs and
  reports/repairs corruption from a healthy replica.
- **Re-replication on node loss** — when a node is down past a grace period, the
  groups it held drop below N replicas; a repair coordinator picks new nodes,
  copies each affected group's needles from a surviving replica (`ListNeedles` +
  `PutNeedle`), and updates the group table once a group is whole again.
- **Rebalance on scale-out** — a new node joins → some groups are reassigned to it;
  their needles migrate in the background, then the group table flips. Slow and
  throttled so it never disrupts serving.

Reads during migration use the group table, which only flips to new nodes **after**
the data is copied, so reads always find a complete replica set.

---

## 11. What stays single-node, and the path to metadata HA

In M2 the **metadata is single-node** — durable on a PV, recovered on restart, but
a momentary single point of unavailability if that node fails. The **data plane is
fully replicated and HA**; only the small metadata core is not yet HA.

Metadata HA is **not** solved with soma-owned Raft. It arrives by swapping the
`MetadataStore` trait implementation for a client of a **distributed metadata
engine** (the in-house engine, once it is distributed) — no change to the gateway,
storage, or S3 layers. soma never writes consensus code. (This is the same engine
that serves the M4 AI index, but the *metadata* and the *AI index* are distinct
roles over distinct data; see `ARCHITECTURE.md` §5/§8.)

---

## 12. Phasing

M2 is large and lands as three sub-phases, each a reviewed branch series:

| Phase | Delivers | Risk |
| --- | --- | --- |
| **M2a — role split + gRPC** | one binary `--role`; `tonic` `MetaService` + `StorageService`; gateway ↔ **one** storage node + **one** metadata node; stateless gateway; remote `MetaClient` / `StorageClient` behind the traits. No replication yet. | medium (new network surface) |
| **M2b — replication + placement** | placement groups + consistent-hash ring; N-way quorum writes; multiple storage nodes; read failover; cluster-state tables. | high |
| **M2c — self-heal + rebalance** | bitrot scrub; re-replication on node loss; group migration on scale-out (throttled). | high (data-safety; heavy fault injection) |

Each phase: design refinement → implementation branches → tests (fault injection
for M2b/M2c) → PR.

---

## 13. Testing

- **M2a**: gateway/node/meta integration over real gRPC (in-process channels and
  real sockets); the existing `object_store` acceptance suite passes against the
  split topology.
- **M2b/M2c — fault injection is mandatory** (full-scenario, not happy path):
  - kill a storage node mid-write / mid-read; verify quorum write still acks and
    reads fail over.
  - lose a node permanently; verify re-replication restores N copies.
  - corrupt a needle on disk; verify scrub detects and repairs it.
  - network partition between gateway and a subset of nodes.
  - scale-out during load; verify rebalance preserves correctness and reads.
  - metadata node restart; verify the cluster recovers (data plane unaffected).
- **Property**: an object readable after a PUT remains byte-identical through any
  single-node failure and any rebalance.

---

## 14. Technology choices (additions over M0/M1)

| Concern | Choice |
| --- | --- |
| RPC | `tonic` + `prost`, codegen via `tonic-build` |
| Storage node engine | M0 `LocalFsBackend` (reused as-is) |
| Metadata (M2) | `RedbMetaStore`, single node, behind `MetadataStore` |
| Gateway read cache | M1 `CachingBackend` (relocated to the gateway) |
| Placement | consistent-hash ring + fixed placement groups |
| Metadata HA (future) | distributed in-house engine behind the trait (no soma Raft) |

---

## 15. Kubernetes deployment changes

The M1 single-pod chart becomes a multi-role topology:

- `Deployment` **gateway** (+ HPA), Service exposes the S3 port; the admin port
  (health/metrics) stays per role.
- `StatefulSet` **storage** (`replicas: N`) + headless Service + per-pod PVs.
- `StatefulSet` **metadata** (`replicas: 1`) + PV.
- Helm `values.yaml` grows per-role sections (counts, resources, storage). The
  operator/CRDs that automate membership and rebalance remain a later concern; M2
  uses the StatefulSet + a controller loop in the metadata role.
