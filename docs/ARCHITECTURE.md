# Soma Architecture

> A fast, secure, and resilient object storage system tailored for the AI era.
> 100% Rust. It swallows, stores, and streams massive raw datasets — and makes
> them *understood*, not just *stored*.

This document is the cornerstone design for Soma. It captures the positioning,
the layered architecture, the storage model, the consistency contract, the
AI-native ingest pipeline, the evolution path, and the testing strategy. Detailed
per-milestone designs live in sibling documents (e.g. `MVP_DESIGN.md`).

---

## 1. Positioning

Soma is a distributed object store in the class of MinIO / SeaweedFS / JuiceFS,
but with one differentiator that the others structurally cannot copy:

> **Objects ingested into Soma are automatically vectorized and graph-ified.**
> Soma does not just hand back bytes — it makes the bytes *semantically
> searchable and graph-traversable*.

It is the storage substrate for the author's database products — **lethe-store**
(the LSM + columnar engine inside the `lethe` project) and **kokedb** (a columnar
query accelerator). Both already speak the S3 protocol through the Rust
`object_store` crate, so Soma's primary external API is **S3-compatible**.

Where a plain object store gives a bucket of blobs, Soma gives an **AI-native data
lake**: data lands, and a background pipeline turns it into vector / graph / full-text
indexes via lethe-store. This is the moat. It is a hard differentiator because it
rides on the author's existing strength in lethe-store's multi-modal indexing.

### Non-goals (for now)

- POSIX filesystem semantics (FUSE mount). Consumers want a blob API, not a
  filesystem. May revisit later as a gateway.
- Being a general compute engine. Soma stores and indexes; query lives in
  lethe / kokedb.

---

## 2. Design pillars

| Pillar | What it means concretely |
| --- | --- |
| **Distributed** | No single node holds all data or all truth. Scales across nodes. |
| **Unlimited horizontal scale** | Add nodes; data rebalances in the background, imperceptibly. Reliability over rebalance speed. |
| **Stateless serving tier** | Gateway pods hold no durable state. They can be killed, scaled, and rolled at will. |
| **Kubernetes-native** | Operator + CRDs, health/readiness, rolling upgrade, autoscaling, local-PV affinity. |
| **High performance** | Zero-copy IO (`bytes::Bytes`), async (`tokio`), sequential write aggregation, O(1) reads. |
| **High security** | TLS in transit, envelope encryption at rest, S3 SigV4 auth, hard multi-tenant isolation. |
| **Built-in cache** | Local NVMe + memory hot tier on the read path. |

---

## 3. High-level architecture

Soma is split into three planes. The split is the load-bearing idea of the whole
system: serving is stateless, truth is a small strongly-consistent core, and bytes
live behind a pluggable durability backend.

```
                          S3 SDK (lethe-store, kokedb, aws-cli, ...)
                                         │
   ┌─────────────────────────────────────────────────────────────────────┐
   │  DATA PLANE  — stateless, horizontally scalable (k8s Deployment)      │
   │  ─────────────────────────────────────────────────────────────────   │
   │  S3 protocol · SigV4 auth · request routing · multipart               │
   │  small-object packing (volume + needle) · large-object chunking       │
   │  in-RAM hot index (derived cache) · NVMe/memory read cache            │
   └───────────────┬───────────────────────────────────┬──────────────────┘
                   │ internal gRPC                      │
   ┌───────────────┴──────────────────┐   ┌─────────────┴──────────────────┐
   │  METADATA PLANE — stateful Raft   │   │  STORAGE BACKEND — pluggable    │
   │  (k8s StatefulSet, 3/5 replicas)  │   │  trait `StorageBackend`         │
   │  ───────────────────────────────  │   │  ─────────────────────────────  │
   │  openraft + embedded engine       │   │  local FS volumes               │
   │  (redb → lethe-store)             │   │   → replication                 │
   │  name → location, version,        │   │   → erasure coding (Reed-Solomon)│
   │  IAM, tenant quotas, EC layout    │   │  bitrot scrub · reconstruction  │
   │  CAS / If-Match in apply()        │   │  background rebalance / GC       │
   └───────────────────────────────────┘   └─────────────────────────────────┘
                   ▲
   ┌───────────────┴──────────────────────────────────────────────────────┐
   │  AI INGEST PLANE — stateless workers (k8s Deployment, async)          │
   │  durable queue → extract → embed + entity/graph → write lethe-store   │
   │  → auto-built HNSW / graph-CSR / BM25 indexes (semantic + graph query)│
   └──────────────────────────────────────────────────────────────────────┘
```

- **Data plane** is stateless: it holds no durable truth, only caches. Killing a
  pod loses nothing.
- **Metadata plane** is the *only* authority. It is a small, strongly-consistent
  Raft group.
- **Storage backend** owns durability and is swappable (FS → replication → EC →
  cloud S3 delegation).
- **AI ingest plane** is async and eventually-consistent; it never blocks writes.

---

## 4. Data plane

### 4.1 S3-compatible API

Soma exposes a practical subset of the S3 API, sized to what lethe-store / kokedb
need first, then broadened:

- Object: `PUT`, `GET` (incl. range), `HEAD`, `DELETE`, `List`
- Multipart upload (large objects)
- Bucket: create / delete / list, versioning toggle, per-bucket policy
- **Conditional writes**: `If-Match` / `If-None-Match` (critical — see §6)
- Auth: AWS SigV4

### 4.2 Small-object storage — the volume + needle model

The single most important storage decision. The hard-won lesson from CDN caches
(Apache Traffic Server, Squid Rock store) and Facebook Haystack / SeaweedFS is:

> **Never store one object per file.** Inode pressure and IOPS amplification from
> millions of tiny files are what cripple naive object stores — and exactly where
> small files are Soma's stated focus.

Instead, Soma packs many small objects into large append-only **volume** files.
Each object is a **needle**:

```
volume file (e.g. 1–32 GB, append-only):
  ┌──────────┬───────────┬──────────┬───────────┬──────────┬─────
  │ needle A │ needle B  │ needle C │ needle D  │ needle E │ ...
  └──────────┴───────────┴──────────┴───────────┴──────────┴─────
  needle = [ header (key, size, flags, checksum) ][ data bytes ]
```

This converges with the entire prior art:

| System | Container | Per-object unit | In-RAM index | Write path | Delete |
| --- | --- | --- | --- | --- | --- |
| ATS | stripe (ring buffer) | fragment (~1 MB) | **10 B/obj**, <0.2% of volume | write-aggregation buffer | mark dir entry only, 0 disk IO |
| Squid Rock | one db file | fixed slot (~16–32 KB), chained | in-RAM slot index | disker batched writes | mark slot |
| SeaweedFS / Haystack | volume (32 GB) | needle `(volId, offset, size)` | **16 B/file**, fully resident, O(1) | append to volume tail | mark, compact later |
| **Soma** | volume (append-only) | needle `[header][data]` | `key→(vol,offset,size)` ~10–16 B/obj | write-aggregation → sequential | mark; GC reclaims |

**Consequences adopted by Soma:**

1. **Write aggregation.** Buffer many small writes and flush them as one large
   sequential IO. Turns random small-write IOPS into streaming bandwidth.
2. **Compact in-RAM hot index.** `key → (volume_id, offset, size)`, ~10–16 bytes
   per object. One billion objects ≈ 16 GB RAM → fully resident → **O(1) reads,
   one disk IO per object**. (See §4.4 for its consistency story.)
3. **Cheap deletes.** A delete marks the needle dead; physical space is reclaimed
   later by background compaction / GC. This matches the "rebalance slowly, no
   speed requirement, only reliability" constraint.

**What NOT to borrow:** Varnish's `file` backend is non-persistent and fragments;
its strength is the in-memory workspace and the VCL caching state machine, not
durable storage. We borrow only the idea of a RAM hot index + slab/arena
allocation, not its on-disk layout.

### 4.3 Large objects

Objects above a threshold (aligned to consumers' SST / columnar segment sizes,
e.g. 4–16 MB chunks) are split into chunks spread across volumes, with a chunk
list recorded in metadata. Multipart upload maps onto this directly.

### 4.4 In-RAM hot index — a derived cache, never an authority

The volume-node hot index is a **performance cache, not a source of truth**. This
is the rule that makes it safe (see §6 for the full protocol):

- **Authority** for "what object name → which needle, which version is current"
  lives in the **metadata plane** (Raft).
- The hot index is **always rebuildable**: each volume periodically checkpoints
  its index to a local `.idx` file; on restart, Soma replays needle headers from
  the volume tail past the last checkpoint to reconstruct the increment. (This is
  exactly SeaweedFS's `.idx` recovery.)
- Therefore a crashed volume node loses **no data** and creates **no
  inconsistency** — it rebuilds its cache and rejoins.

---

## 5. Metadata plane

### 5.1 Raft is replication, not storage

A recurring confusion worth stating plainly: **Raft does not store anything.** Raft
keeps N copies of an operation log identical and elects a leader. Each node applies
that log to a **state machine**, and the state machine needs an embedded storage
engine behind it. So "Raft with no database" is a category error — Raft's state
machine *is* a (small, embedded) database.

The payoff: that engine does **not** have to be an external server (no Postgres,
no etcd, no TiKV to operate). It can be embedded in-process:

> **`openraft` + an embedded engine = a self-contained, strongly-consistent,
> zero-external-dependency metadata service.** This is what makes the
> Kubernetes-native, self-contained story real.

openraft needs two stores — a **log store** (raft log entries) and a
**state-machine store** (applied state). Both are satisfied by the embedded engine.

### 5.2 `MetadataStore` trait — redb now, lethe-store later

The engine sits behind a `MetadataStore` trait so the project is not blocked on
lethe-store's extraction:

- **MVP: `redb`** — pure-Rust embedded ACID B-tree. It ships transactions and
  compare-and-swap out of the box, so the *semantic* layer (versioning, conditional
  writes) can be built and tested today.
- **Later: `lethe-store`** — once extracted standalone from `../lethe`. Analysis
  shows a strong fit: embeddable single-binary, WAL + fsync crash safety, atomic
  `WriteBatch`, MVCC snapshots, time-travel (`HistoricalView`), and **deterministic
  ordered-log apply** with monotonic seqnos — exactly the determinism a Raft state
  machine wants.

### 5.3 Conditional writes — Raft makes CAS free

lethe-store has no built-in compare-and-swap. **Under Raft this is a non-issue.**
Because every write is serialized through the leader into a single ordered log,
`If-Match` / CAS is implemented inside the state-machine `apply()` function: read
the current version, compare, conditionally apply. Raft's serialization point *is*
the linearization point. So the consumers' hard requirements — strong
read-after-write, conditional writes for atomic manifest/materialized-view
publication, object versioning — are all satisfied.

### 5.4 What metadata holds

- `bucket / object name → needle location(s)`, current version, size, checksum
- object version history (when versioning is enabled)
- erasure-coding layout (which shards on which nodes) / replica placement
- IAM: access keys, policies, tenant boundaries, quotas
- AI-pipeline state (per-object ingest/index status)

Metadata is small relative to data and changes slowly — a good fit for a Raft core.

---

## 6. Consistency & the write protocol

The contract Soma offers consumers:

- **Strong read-after-write consistency** — a successful `PUT` is immediately
  visible from any node.
- **Conditional writes** (`If-Match` / `If-None-Match`) — atomic, linearizable.
- **Object versioning** — opt-in per bucket; supports rollback / time-travel.

The write protocol that delivers this, ordered so a crash at **any** step is safe:

```
1. Append needle to volume file, then fsync.        → bytes are now durable
2. Commit (name → location, version) to Raft meta.  → globally visible, linearized
3. Update the volume-node hot index (cache only).   → losable; rebuildable
4. ACK to client.
```

Crash analysis:

| Crash point | Outcome | Why it's safe |
| --- | --- | --- |
| Between 1 and 2 | Orphan needle in volume | Not committed → never read (read-after-write only sees committed). Space reclaimed by GC. |
| After 2, before hot-index persist | Hot index stale on that node | Rebuilt on restart from `.idx` checkpoint + needle-header replay. No data loss. |
| After 2 | Fully durable & visible | Bytes on volume + committed in Raft. |

Two invariants hold the whole thing together:

1. **Metadata commit happens only after the bytes are durable.**
2. **The hot index is a derived cache, never an authority.**

The AI ingest pipeline (§8) is deliberately **eventually consistent** — semantic
indexes lagging by seconds is fine and never affects object durability or reads.

---

## 7. Storage backend

A `StorageBackend` trait abstracts durability so Soma can run self-contained
on-prem *or* delegate to the cloud — and so the project can deliver incrementally
without first solving erasure coding.

```
StorageBackend (trait)
├── LocalFsBackend        — volumes on local disk (single node)        [M0]
├── ReplicatedBackend     — N-way replication across nodes             [M2]
├── ErasureCodedBackend   — Reed-Solomon (k data + m parity)           [M4]
└── S3DelegatingBackend   — delegate durability to cloud object store  [opt]
```

### 7.1 Replication first, erasure coding later

The first distributed durability is **N-way replication** — simple and robust.
**Erasure coding** (Reed-Solomon, e.g. the `reed-solomon-simd` crate) lands as a
later, isolated branch.

The Reed-Solomon **math is the easy part**. The real risk — and where the
engineering rigor goes — is the distributed failure semantics:

- quorum writes (how many of `k + m` shards must land to ack)
- degraded reads (reconstruct an object with up to `m` shards missing)
- reconstruction / repair (recompute lost shards from survivors when a node dies)
- **bitrot detection + background scrub** (silent disk corruption is the #1 killer
  of object stores)
- rebalance on scale-out (background, low priority, imperceptible)

### 7.2 Cloud delegation

On a public cloud, layering Soma's own EC on top of already-redundant cloud disks
is double redundancy and wasteful. The `S3DelegatingBackend` lets the same Soma
run on-prem with EC and on-cloud delegating to S3/cloud disks. The trait makes this
a deployment choice, not a fork.

---

## 8. AI-native ingest pipeline (the differentiator)

Iron rule: **embedding must never block the write path.**

```
S3 PUT ─→ store bytes in volume ─→ commit metadata ─→ ACK   (milliseconds)
                                          │ (async, eventually consistent)
                                          ▼
                              durable work queue
                                          │
              stateless embedder workers (independently scalable)
                                          │
   content-type detect → extract (pdf/doc/img/text) → embed (vector)
                                          + entity / relation extraction
                                          │
                       write content as a lethe-store MemoryEntry
                                          │
        lethe-store Optimizer auto-builds derived indexes:
          .lhns (HNSW vector) · .gcsr (graph CSR) · .lbm25 (BM25 full-text)
                                          │
                       → semantic search + graph traversal
```

Design points:

- **Per-bucket toggle.** Buckets holding SST / columnar blobs are *not* vectorized;
  document buckets are. Indexing is opt-in.
- **Pluggable embedder.** Local model (`candle` / the sibling `rust-bert`) or a
  remote API.
- **Failure = retry.** The pipeline is at-least-once; failures retry and never
  affect object durability.
- lethe-store's derived indexes — which are *inseparable* from its KV core and thus
  a liability for plain metadata — are here exactly the **feature**.

**Open product boundary** (to settle as lethe-store goes standalone): most likely
Soma owns *blob storage + S3 + the ingest pipeline*, and lethe owns the
*vector / graph query surface*. Clean separation, no duplicated query engine.

---

## 9. Security

- **In transit:** TLS via `rustls`.
- **At rest:** envelope encryption — a per-object data key (DEK) wrapped by a KMS
  master key.
- **AuthN:** AWS SigV4 signed requests.
- **AuthZ + multi-tenancy:** bucket policies; hard tenant isolation (consumers
  lethe / kokedb are themselves multi-tenant). Per-tenant quotas and QoS so one
  tenant's scan cannot starve another's online queries.

---

## 10. Kubernetes-native deployment

| Component | Workload | Notes |
| --- | --- | --- |
| Data plane (gateway) | `Deployment` + HPA | Stateless; scale and roll freely. |
| Metadata plane | `StatefulSet`, 3 or 5 replicas | Raft group; stable identity. |
| Volume / storage nodes | `StatefulSet` + local PV | Data locality; NVMe cache affinity. |
| AI ingest workers | `Deployment` + HPA | Stateless; scale on queue depth. |
| Operator | CRDs | `SomaCluster` etc.; rolling upgrade, health, autoscaling. |

Hot-cache affinity is the subtle constraint: when a pod moves, its NVMe cache
warmth is lost. Local PVs + consistent-hashing virtual nodes smooth the migration.

---

## 11. Horizontal scaling & rebalance

Adding a node triggers **background, low-priority rebalance**: data migrates slowly
and imperceptibly, prioritizing reliability over speed (an explicit project
constraint). This reuses the same machinery as compaction / GC: background tasks
that never block the foreground path.

---

## 12. Implementation challenges (ranked by danger)

1. **Self-built EC + distributed durability** — the most likely source of
   data-loss-class bugs. Isolated, heavily fault-tested.
2. **Rebalance on scale-out** — migrate without disrupting service or saturating
   bandwidth. Mitigated by the "slow is fine" constraint.
3. **Strong-consistency metadata vs throughput** — addressed by keeping the Raft
   core small (metadata only) and the data plane lock-free.
4. **Massive small objects** — addressed head-on by the volume + needle model.
5. **Hot-cache vs pod lifecycle** — addressed by derived-cache rebuild + local PV.
6. **Multi-tenant QoS isolation** — per-tenant quotas and scheduling.

---

## 13. Milestones

| Milestone | Scope |
| --- | --- |
| **M0 — single-node skeleton** | S3 subset (PUT/GET/DELETE/List/Multipart) · `MetadataStore` trait + redb · `LocalFsBackend` (volume + needle) · SigV4 · **lethe-store & kokedb connect and run today** |
| **M1 — stateless + cache** | Split out stateless gateway · NVMe/memory read cache · k8s manifests (Helm / operator) |
| **M2 — distributed durability** | Raft metadata (openraft) · replication · consistent-hash placement · failure self-heal |
| **M3 — elastic scale** | Online scale-out + background rebalance |
| **M4 — hardening** | Erasure coding · envelope encryption · multi-tenant QoS · AI ingest pipeline GA |

The deliberate choice: get "stateless serving + cache + k8s + S3 API + both
databases connected" working on a local-FS backend **first**, and defer the
hardest piece (EC) — so every milestone ships something usable and each lands as
its own reviewed branch.

---

## 14. Testing strategy

- **Unit + property tests** for needle encoding, index rebuild, codec round-trips.
- **Deterministic fault injection** is mandatory for the durability layers — not
  happy-path only. Full-scenario coverage:
  - kill a node mid-write and mid-read
  - corrupt shards / detect bitrot
  - disk-full
  - network partition
  - second failure *during* reconstruction
- **Crash-recovery tests** for the write protocol (§6) at every step boundary.
- **Consumer integration tests**: lethe-store and kokedb running their real
  workloads against Soma.
- **Benchmarks**: small-object throughput, read latency (O(1) claim), write
  aggregation effectiveness, vs SeaweedFS / MinIO where meaningful.

---

## 15. Technology choices (Rust)

| Concern | Choice |
| --- | --- |
| Async runtime | `tokio` (evaluate thread-per-core `monoio` / `glommio` for the IO hot path) |
| Zero-copy buffers | `bytes` |
| Consensus | `openraft` |
| MVP metadata engine | `redb` |
| Long-term metadata engine | `lethe-store` (once standalone) |
| Erasure coding | `reed-solomon-simd` |
| TLS | `rustls` |
| Embeddings (local) | `candle` / `rust-bert` |
| Derived vector/graph/text indexes | `lethe-store` (HNSW / graph-CSR / BM25) |

Match the consumers' rigor: lethe and kokedb deny `unwrap` / `expect` / `panic` in
their lint config — Soma should hold the same line.
