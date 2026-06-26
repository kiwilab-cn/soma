# Soma Storage Model

How soma stores object bytes on a storage node, and how it makes small-file
writes fast without sacrificing the durability and consistency the metadata
plane relies on.

This document covers the **physical** byte layer (needles in volumes) and the
**logical** object layer (single-needle today, chunk-manifest for the large /
streaming / append cases — see `STREAMING_APPEND.md`), then the write path:
group-commit batched fsync and configurable durability.

Soma is **self-contained**: small-file efficiency comes from its own
needle/volume packing, durability/HA from erasure coding across nodes, and
metadata from its own store behind the pluggable `MetadataStore` trait. Nothing
here depends on any external system.

---

## 1. Two layers: physical needles, logical objects

### 1.1 Physical: needles in append-only volumes

A storage node holds a small set of **volume** files (`volumes/<id>.vol`), each an
append-only log of immutable **needles**. A needle is one contiguous record:

```
+--------------------- 32-byte header ---------------------+----------- payload -----------+--- pad ---+
| magic | ver | flags | object_id | data_len | data_crc | header_crc | <data_len bytes>     | 0..7 zero |
|  u32  | u8  |  u8   |    u64    |   u32    |   u32    |    u32     |                      |           |
+----------------------------------------------------------+-------------------------------+-----------+
```

- `magic` = `"SOMA"`, `ver` = format version, `flags` bit0 = tombstone.
- `data_crc` guards the payload (bitrot); `header_crc` guards bytes `[0..28)` so a
  scan can trust the framing before it reads the payload.
- Needles are **8-byte aligned**, so recovery steps deterministically from one to
  the next, and a torn tail needle is detected and truncated.

This is the Haystack / SeaweedFS / WiscKey lineage: **many small objects packed
into few large files**, so a small write is an append (not a new inode), and a
small read is one `mmap`+CRC (not an `open`/`seek`/`read` per object). The OS
page cache, not a bespoke block cache, serves hot needles.

An in-memory **HotIndex** (`object_id → (volume, offset, len)`) is the read path.
It is rebuilt on startup by scanning volume tails, accelerated by a per-volume
`.idx` checkpoint (a pure optimization — never required for correctness). Deletes
append a **tombstone** needle; space is reclaimed later by **compaction**
(copy-live-to-new-volume + atomic rename, fd-pinning keeps in-flight reads safe).

### 1.2 Logical: object = one needle (today), or a chunk manifest

Most objects are a **single needle** — the whole payload in one record. This is
optimal for the small-file and small/medium contiguous cases that dominate AI
workloads (embeddings, features, segments, model shards).

For objects that exceed a volume, or arrive via **streaming** / **append**, an
object becomes a **manifest of 1..N chunk-needles** (each chunk is just a needle
with the same layout — there is no second format). The manifest and the append
semantics (`x-amz-write-offset-bytes`, offset-as-CAS-fence) are specified in
`STREAMING_APPEND.md`; object-size thresholds in `OBJECT_SIZING.md`. The key
point for this document: **a chunk is a small needle**, so everything below about
the write path applies uniformly whether an object is one needle or many.

---

## 2. The write path and the durability question

A `put` does three things under the node lock: append the needle bytes, update
the HotIndex, and bump a monotone `write_seq`. The open question is **when the
bytes hit the platter** — i.e. when `fsync` runs.

`fsync` per write is the simple, safe choice, but for small files it dominates
latency and throughput: every 4 KB object pays a full device flush, and
concurrent writers each pay it independently even though they could share one
flush. SeaweedFS's answer is instructive — by default it does **not** fsync per
write; durability comes from async flush plus replication. Soma generalizes this
into an explicit, per-deployment choice plus a coalescing optimization.

### 2.1 Group-commit batched fsync

When many writers append concurrently, their bytes are already in the same few
volume files. One `fsync` per volume flushes **all** of their appends at once. So
instead of N writers issuing N flushes, they elect a **leader** that issues one
flush covering everyone, and the **followers** wait for it.

Mechanism (`LocalFsBackend`, `group_commit` / `fsync_dirty`):

- Each `put` appends under the lock and records its `write_seq`. Group-commit
  appends mark their volume **dirty** but do **not** fsync inline.
- After releasing the data lock, a writer calls `group_commit(seq)` and waits on
  a condvar until a shared watermark, `done_through`, reaches its `seq`.
- The first waiter becomes **leader**: it fsyncs every dirty volume (one flush
  per volume, covering all appends so far), advances `done_through` to the
  `write_seq` captured at flush time, and `notify_all`. Followers wake, see their
  `seq <= done_through`, and return durable.
- Lock order is **gc.state → inner**, and the leader releases `gc.state` before
  taking `inner` to fsync, so the data path never blocks behind a flush.

The result: under load, fsyncs **coalesce** — throughput scales with device flush
rate, not with object count — while each writer still returns only once its bytes
are genuinely durable. Under no load, a lone writer is its own leader and flushes
immediately, so latency is one fsync (same as per-write).

### 2.2 Configurable durability

The flush policy is a deployment knob — `BackendConfig::durability`, wired from
server config `storage.durability`:

| Mode | `storage.durability` | Behavior | Use when |
|---|---|---|---|
| **Per-write** | `per_write` | fsync inline before `put` returns | Strictest single-node durability; small write rate; no replication to lean on. |
| **Group-commit** | `group_commit` *(default)* | coalesced shared fsync; `put` returns once a flush covers it | General case — durable on return, high concurrent throughput. |
| **Async** | `async` | no fsync on the write path; OS flushes in the background; an explicit `sync()` is a barrier | Highest throughput; durability delegated to **erasure coding / replication** across nodes (a lost un-flushed tail on one node is reconstructable), à la SeaweedFS default. |

All three share **one** append path and **one** on-disk format — they differ only
in *when* `fsync` happens. Switching modes never changes how bytes are laid out
or read, so a volume written under one mode is readable under any other.

### 2.3 Group-commit on the metadata plane

The byte fsync is only half the per-object write cost. The other half is the
**metadata commit** — the `key → object_id` transaction that is the object's
commit point (§3). The meta store (redb) is a single-writer B-tree, so each
commit is its own transaction and its own fsync; under bursty small-object PUTs
that commit fsync becomes the bottleneck, exactly as the byte fsync was.

The fix is the same idea applied one plane over. The meta node runs a **commit
batcher** (`serve_meta`): concurrent `put_object` commits are funnelled to a
single worker that drains everything queued and applies the whole set in **one**
redb write transaction via `MetadataStore::put_object_batch` — one B-tree commit,
one fsync, for the entire batch. Because the drain happens *after* the previous
transaction's commit returns, each batch is naturally just the requests that
piled up during that commit: large batches under load, a batch of one when idle,
so no latency is traded away by a timer.

Each item in a batch keeps its **own** CAS/quota/bucket evaluation and its own
result — one item's precondition failure records an error for that item alone and
never aborts its neighbours. Same-key items chain correctly (reads in the
transaction see prior items' writes), so the batched path is semantically
identical to committing them one at a time, just durably cheaper. This batching
is internal to the meta node — the `PutObject` RPC is unchanged, so it
transparently coalesces commits arriving from *all* gateways, not just one.

(Object-id allocation, `next_object_id`, is still a per-call transaction — folding
it into a hi-lo allocator is a separate follow-up. Delete commits are not yet
batched either.)

---

## 3. Consistency: commit = durable **and** visible

The metadata plane (key → `object_id`) is the source of truth for what exists.
The storage node is **bucket-blind**: it stores `object_id → bytes` and knows
nothing about names or commits. This separation defines the consistency model:

1. **Write bytes, then commit metadata.** The gateway puts the needle(s), then
   commits the `key → object_id` mapping in the metadata store (an atomic
   CAS/quota/authz transaction). The metadata commit is the single
   **commit point**.
2. **Durable before commit.** A `put` returns only when its chosen durability
   level is satisfied (fsynced for per-write/group-commit; OS-buffered for async,
   where cross-node redundancy is the durability guarantee). So by the time the
   metadata commit runs, the bytes are as durable as the deployment promises.
3. **Uncommitted ⇒ invisible.** A needle whose object was never committed in
   metadata is unreachable by any reader — there is no name pointing at it. Such
   **orphan needles** (writer crashed between byte-write and metadata-commit, or a
   retried/abandoned upload) are harmless and reclaimed by compaction/GC. They are
   neither visible nor a correctness problem.
4. **No partial reads.** A reader resolves a name to an `object_id` via committed
   metadata, then fetches bytes. It can only ever see fully-committed objects;
   half-written needles (torn tail) fail their CRC/framing check and are ignored
   by recovery before any index entry exists for them.

The async mode subtly shifts the durability boundary **outward**: a single node
may ack bytes that are not yet on its own platter, but the object is only
*committed* once metadata records it, and its bytes survive node loss because
they are erasure-coded/replicated across the cluster. Durability becomes a
cluster property, not a per-node fsync — which is exactly why an object store can
afford async local writes where a single-node database engine cannot.

---

## 4. What this is not

- **Not a WAL+LSM.** Soma does not write a redo log and compact sorted runs to
  make small writes fast. Small-write speed comes from needle packing + lean
  metadata + coalesced/async fsync. A consumer that *needs* LSM semantics (e.g. a
  database engine) builds that **on top of** soma's object API; it is not soma's
  job. (See `soma-vision` — soma is general AI infra, not any one engine's
  storage layer.)
- **Metadata commits are batched too.** §2.1 is the *storage*-side group-commit;
  §2.3 applies the same coalescing to the *metadata* commit, so a burst of small
  PUTs pays one B-tree commit/fsync instead of one per object. Still outstanding:
  hi-lo object-id allocation and batched delete commits.

---

## 5. Cross-references

- `STREAMING_APPEND.md` — chunk-manifest objects, streaming ingest, S3-style
  append (`x-amz-write-offset-bytes`).
- `OBJECT_SIZING.md` — single-needle vs. chunked thresholds, `volume_max`.
- `CONDITIONAL_WRITES.md` — CAS / conditional-PUT contract at the metadata commit.
- `LOCALITY_DESIGN.md` — zero-copy short-circuit local reads (fd-passing).
- `ARCHITECTURE.md` §7 — the three planes (gateway / metadata / storage).
