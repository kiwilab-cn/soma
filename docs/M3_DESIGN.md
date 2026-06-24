# Soma M3 Design — Elastic Scale

> Detailed design for milestone **M3**: online scale-out, background rebalance,
> and node-loss self-heal. Parent: [`ARCHITECTURE.md`](./ARCHITECTURE.md). Builds
> on M2 (replication, consistent-hash placement) and M4 (erasure coding).

## 1. The problem M3 solves

Today placement is a consistent-hash [`Ring`] built from a **static** list of
storage endpoints (`storage_endpoints`), addressed by array **index**. An object's
replicas/shards are `ring.replicas(object_id, n)`. This has two gaps:

1. **Membership is static.** Adding or removing a node means editing config and
   restarting every gateway. Worse, changing the node set silently changes the
   ring, so existing objects' computed placement no longer matches where their
   bytes actually live — they become unreadable. There is no data movement.
2. **Failure is only patched lazily.** Replication self-heals on read (read-repair)
   but never proactively; erasure-coded objects are not reconstructed at all
   (deferred from M4b). A permanently lost node leaves objects under-durable.

M3 makes the cluster **elastic**: nodes join and leave at runtime, data migrates
to match, and lost redundancy is rebuilt — all in the background, reliability over
speed (the project's explicit "slow rebalance is fine" constraint).

## 2. Core model: placement groups + a mutable placement table

The central change is to stop deriving placement *directly* from the live node
ring, and instead route through **placement groups (PGs)** with a **stored,
mutable PG→nodes table** as the authority. This is the Ceph-style decoupling, and
it is what makes bounded, resumable migration possible.

```
object_id ──hash──▶ pg = H(object_id) mod P ──PG table──▶ [node_id, node_id, ...]
```

- **Fixed PG count `P`** (e.g. 256), chosen at cluster init and constant for the
  cluster's life. `pg = stable_hash(object_id) % P` — independent of node count, so
  an object's PG never changes.
- **PG table** (`pg → PgPlacement`), stored in the metadata store, is the
  **authority** for where a PG's objects live. The consistent-hash ring computes
  the *target* mapping from current membership; the *stored* table is what
  gateways use — so a PG can stay on its old nodes until its data has actually
  moved. (This is Ceph's `pg_temp` idea: desired vs acting set.)
- **Stable node identity.** Nodes are referenced by a durable `node_id`, never by
  array index, so adding/removing a node never renumbers the others. The ring is
  built over `node_id`s drawn from live membership.

Why PGs and not per-object placement in `ObjectMeta`? Per-object placement would
bloat metadata (deliberately avoided in M2), force an O(objects) scan to rebalance
or to find a down node's data, and need a secondary index. PGs make the unit of
placement and migration `P` (hundreds), not the object count (billions): "which
objects must move when node X joins/leaves" is answered by scanning `P` PG rows.

### What a gateway does per request

`object_id → pg → PG table lookup → node set`. The PG table is small (`P` rows),
read once and **cached on each gateway** with a generation number; the controller
bumps the generation on any change, and gateways refresh lazily (a stale read just
means trying the old node set, which the migration protocol tolerates — see §5).

## 3. Membership

A **membership table** in the metadata store: `node_id → NodeInfo { endpoint,
state, last_heartbeat, generation }`, where `state ∈ {Joining, Active, Draining,
Down}`.

- **Self-registration.** A storage node, on startup, calls `Register` on the meta
  node with its `node_id` (stable, derived from the StatefulSet pod identity / a
  persisted file) and endpoint, then **heartbeats** periodically.
- **Liveness.** The controller marks a node `Down` after it misses heartbeats for a
  threshold; a returning node re-registers and goes back to `Active`.
- **Scale-out** = a new pod self-registers (`Joining → Active`). **Scale-in** =
  `Draining` (an admin action / preStop hook) so the node's data migrates off
  before the pod is removed; an ungraceful loss is just `Down`.

This keeps operators in the k8s idiom: `kubectl scale` the storage StatefulSet up
or down; pods (de)register themselves; the controller reconciles.

## 4. The controller + mover

A single **cluster controller** runs in the **meta** role (the cluster's singleton
authority — its natural home; the meta binary already links `soma-cluster`). It
reconciles **desired** placement (computed from `Active` membership via the ring)
against the **stored** PG table, and drives migration:

```
loop (slow, background):
  members  = membership.active()
  for pg in 0..P:
    target  = ring_place(pg, members, replication_or_ec_width)
    stored  = pg_table[pg]
    if target != stored.nodes and not stored.migrating:
      begin_migration(pg, from = stored.nodes, to = target)   // dual set + gen bump
  drive in-flight migrations to completion (bounded concurrency), then finalize.
```

The **mover** performs the byte movement for a migrating PG, one object at a time,
at low priority (rate-limited, a few PGs concurrently — reliability over speed):

- **Replication:** for each new node in the target set, copy a surviving replica's
  bytes to it (`StorageClient.get` from an old holder → `put` to the new holder).
- **Erasure coding:** for each target slot that changed, the new node needs *that
  slot's shard*. The mover reconstructs the object from any `k` surviving shards,
  re-encodes, and writes the one shard that slot requires (this is the EC
  reconstruction deferred from M4b, landing here). 
- When every object in the PG is present on the target set, the controller
  **finalizes**: PG table flips to target-only, generation bumps, and the stale
  copies on dropped nodes are deleted (or left to GC).

The controller holds `StorageClient`s to all nodes (same client the gateway uses).
A single mover is a throughput bottleneck but correct; **distributed/parallel
movers are a later refinement** — "slow is fine."

## 5. Correctness during migration (the hard part)

While a PG is migrating it has **both** an old set and a target set. The protocol
that keeps reads correct and writes durable through the transition:

- **Writes dual-target.** A write to an object in a migrating PG goes to **both**
  the old set and the target set (each under its own quorum). Nothing written
  mid-migration is lost when the PG finalizes to target-only.
- **Reads fall back.** A read tries the **target** set first, then the **old** set
  (the same failover the replicated/EC backends already do, widened to the union).
  So an object the mover hasn't copied yet is still served from its old home.
- **Finalize is the commit point.** The PG flips to target-only only after every
  object is confirmed on the target set. A gateway with a stale PG-table generation
  reads/writes the old set — still correct, because the old set is retained until
  finalize and dual-writes keep it current until then.

This mirrors the metadata-as-commit-authority discipline from M2: the PG table's
generation is the linearization point, evaluated in the meta store's transaction.

## 6. Node-loss self-heal

A `Down` node is the same reconcile, triggered by liveness instead of a join:

- PGs that included the down node are **under-durable** (a missing replica, or a
  missing shard). The controller computes a target set excluding the down node
  (picking a replacement), marks those PGs migrating (old = surviving set), and the
  mover **re-replicates** (copy a surviving replica) or **reconstructs** (rebuild
  the missing shard from `k` survivors) onto the replacement.
- This supersedes M2c's read-repair-only healing for the proactive case (read
  repair stays as the cheap online path). It also closes the M4b gap: EC objects
  are now reconstructed after node loss, not just served degraded.

## 7. Phasing

A large milestone; each phase is an independent, reviewed branch.

| Phase | Delivers |
| --- | --- |
| **M3a — membership + PG routing** | Stable `node_id`; membership table + self-registration/heartbeat/liveness; PG table seeded at init; placement resolved via `object_id → pg → PG table` (replication + EC) through a `PlacementClient` the gateway caches. **No migration yet** — foundation only; the cluster behaves exactly as M2 but addressed by PG/identity. |
| **M3b — scale-out rebalance** | The controller + mover for **replication**; membership-change → migrating PGs (dual-write + read-fallback) → finalize. `kubectl scale` up moves data to the new node, imperceptibly. |
| **M3c — node-loss re-replication + drain** | Liveness `Down` → proactive re-replication of under-durable PGs onto replacements; graceful `Draining` for scale-in. |
| **M3d — EC reconstruction** | Extend the mover to reconstruct erasure-coded shards for migrated/lost slots (the deferred M4b reconstruction), closing EC durability under membership change. |

Each phase keeps the test discipline of the durability work: **deterministic fault
injection is mandatory** — kill nodes mid-migration, finalize under concurrent
writes, stale-generation reads, lose a node during another's rebalance.

## 8. What's deferred (recorded)

- **Distributed/parallel movers** — M3 uses one bounded mover in the controller;
  parallel storage-to-storage movement is a throughput refinement.
- **Metadata HA** — the meta role (and now the controller) is still a single
  replica; HA is delegated to a future distributed metadata engine (from M2).
- **Capacity/weight-aware placement** — M3 places by uniform consistent hashing;
  heterogeneous node weights and capacity-aware balancing are later.
- **Automatic PG splitting/merging** — `P` is fixed at init; online PG count change
  (Ceph's pg_num growth) is out of scope.

## 9. Technology choices (additions over M2)

| Concern | Choice |
| --- | --- |
| PG table + membership | new tables in `RedbMetaStore`, mutated in its write transaction (generation = linearization point) |
| Node identity | stable `node_id` (StatefulSet pod ordinal / persisted file) |
| Controller + mover | background task in the **meta** role, holding `StorageClient`s; rate-limited, bounded concurrency |
| Heartbeat / register | new `MetadataStore` (or a sibling membership) RPCs over the existing `tonic` channel |
