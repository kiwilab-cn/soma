# Soma M4 Design — Hardening: Erasure Coding, Encryption at Rest, Multi-Tenant QoS

> Detailed design for milestone **M4**.
> Parent: [`ARCHITECTURE.md`](./ARCHITECTURE.md). Builds on M2 (distributed
> durability). M4 hardens the data plane: storage efficiency, confidentiality,
> and fairness.

## 1. Scope & framing

The architecture (§13) lists M4 as "erasure coding · envelope encryption ·
multi-tenant QoS · AI ingest pipeline GA". M4 ships the **three self-contained
pillars**:

1. **Erasure coding** — Reed-Solomon `k+m` storage, an efficient alternative to
   N-way replication.
2. **Envelope encryption at rest** — per-object data keys wrapped by a master key.
3. **Multi-tenant QoS** — per-tenant quotas and rate limiting.

### AI ingest is deferred

The AI ingest pipeline (auto vectorize / graph-ify ingested objects — the moat)
is **deferred out of M4**. It requires the in-house multi-modal indexing engine as
its index sink, which is not yet available as a standalone dependency for this
repository. It returns as its own milestone once that engine is available, behind
a pluggable `IndexSink` boundary so the blob/S3/ingest framework needs no rework.
(M4 builds none of it; this note records the deferral and the seam.)

### Phasing

| Phase | Delivers |
| --- | --- |
| **M4a — encryption** | `EncryptingBackend` decorator: per-object DEK, AES-256-GCM, master-key envelope. |
| **M4b — erasure coding** | `ErasureCodedBackend`: Reed-Solomon `k+m` shards placed across nodes; degraded reads + reconstruction. |
| **M4c — multi-tenant QoS** | per-tenant quotas (bytes/objects) + request rate limiting at the gateway. |

Each phase is an independent, reviewed branch (encryption and EC are both
`StorageBackend` decorators; QoS is gateway-side), tested — with fault injection
for EC.

---

## 2. Erasure coding (Reed-Solomon)

N-way replication costs `N×` storage for `N−1` fault tolerance. Erasure coding
gives the same tolerance for far less: split an object into `k` **data** shards +
`m` **parity** shards; any `k` of the `k+m` reconstruct the object, surviving up to
`m` node losses, at `(k+m)/k×` storage (e.g. `k=4, m=2` → 1.5× for 2-failure
tolerance vs replication's 3× for 2-failure tolerance).

### Placement reuses the ring and the object_id model

`ErasureCodedBackend` is a `StorageBackend` the gateway uses in place of
`ReplicatedBackend`:

- The object's `k+m` **distinct** storage nodes are the consistent-hash ring's
  `replicas(object_id, k+m)` — exactly as replication, just a wider set.
- The bytes are encoded into `k+m` shards (`reed-solomon-simd`). Shard `i` is
  written to the `i`-th node in the placement list **under the same `object_id`** —
  each node holds exactly one shard per object, so the storage node is unchanged
  (it stores opaque bytes by id; it never knows it holds a shard). The shard index
  is implicit in the node's position.

```
PUT  (gateway):
  shards = rs.encode(pad(data), k, m)         // k data + m parity, each |shard| = ceil(len/k)
  nodes  = ring.replicas(object_id, k+m)
  write shard[i] -> nodes[i]  for all i; require >= k+m-? acks (a write quorum).
  then commit metadata {object_id, size, ...}  (size is the true object length).

GET  (gateway):
  nodes = ring.replicas(object_id, k+m)
  fetch shards from any k reachable nodes (prefer the k data shards);
  if fewer than k data shards, reconstruct from parity (rs.reconstruct);
  reassemble + truncate to `size`; serve (range slices after).
```

- **Degraded read**: missing data shards are recomputed from parity as long as `k`
  total shards survive.
- **Reconstruction / repair**: a lost node's shards are recomputed from `k`
  survivors and written to a replacement — the same machinery as M2c read-repair,
  generalized to shards (a later step alongside the placement-group rebalance).
- **Padding & size**: the object is zero-padded to a multiple of `k` before
  encoding; the metadata's true `size` truncates on read.

### Choosing replication vs EC

A bucket- or cluster-level setting selects the durability backend:
`replicated` (default, low-latency, small objects) or `erasure k+m` (efficient,
larger objects). M4 ships a cluster-wide setting (`durability.mode`); per-bucket
selection is a later refinement. EC and the read cache compose (cache holds the
reassembled object).

### Testing (fault injection, mandatory)

- write then lose `m` nodes → object still reconstructs; lose `m+1` → read fails.
- corrupt a shard → reconstruction routes around it (CRC catches it at the node).
- write quorum semantics under a node down.

---

## 3. Envelope encryption at rest

Confidentiality for stored bytes, without a per-object key-management database.

### Scheme

- Per **object**, a random 256-bit **DEK** (data encryption key).
- The payload is sealed with **AES-256-GCM** under the DEK (authenticated; the
  needle CRC stays as a cheap corruption check, GCM as the cryptographic seal).
- The DEK is **wrapped** (encrypted) by a **master key (KEK)** and stored
  *alongside the ciphertext*, so the stored needle is self-describing:

```
stored bytes = [ version:u8 ][ wrapped_dek (incl. its nonce) ][ gcm_nonce:12 ][ ciphertext+tag ]
```

`EncryptingBackend` is a `StorageBackend` decorator (sits above the replicated/EC
backend):

```
put(object_id, data):
  dek        = random 32 bytes
  ciphertext = AES-256-GCM(dek, nonce, data)
  wrapped    = wrap(KEK, dek)                    // AES-256-GCM under the master key
  inner.put(object_id, frame(wrapped, nonce, ciphertext))

get(object_id, range):
  frame = inner.get(object_id, None)             // full (GCM is not seekable)
  dek   = unwrap(KEK, frame.wrapped)
  plain = AES-256-GCM-open(dek, frame.nonce, frame.ciphertext)
  range.map(|r| slice(plain, r)).unwrap_or(plain)
```

- **Range reads** decrypt the whole object then slice (GCM seals the whole
  payload). For encrypted buckets this trades range efficiency for integrity; a
  seekable mode (AES-CTR + separate MAC, or per-chunk GCM) is a later refinement.
- **Composition order**: encrypt **before** replication/EC, so each replica/shard
  holds ciphertext (a node never sees plaintext). `EncryptingBackend` wraps
  `ReplicatedBackend`/`ErasureCodedBackend`. The gateway read cache then holds
  *plaintext* (already-decrypted) — acceptable (it's in-process memory).

### Master key (KEK)

A `KeyProvider` trait abstracts the KEK source:

- **M4**: a `StaticKeyProvider` — the master key from config / a Kubernetes Secret
  (base64). Self-contained, no external dependency.
- **Later**: an external KMS provider (AWS KMS / Vault) behind the same trait;
  key rotation re-wraps DEKs without re-encrypting objects.

Encryption is opt-in via config (`encryption.enabled` + master key). Disabled →
the decorator is not inserted.

---

## 4. Multi-tenant QoS

Soma's consumers are multi-tenant; one tenant must not exhaust capacity or starve
another.

### Tenant identity

A **tenant** is identified by its access key: the credential set maps each
`access_key → tenant_id`. (Bucket→tenant ownership is a later refinement.) The
gateway resolves the tenant from the SigV4 access key already parsed during auth.

### Quotas

Per-tenant limits enforced at write time:

- **storage bytes** and **object count** — tracked per tenant in the metadata
  store (a small counters table, updated transactionally with `put_object` /
  `delete_object`). A `PUT` that would exceed the quota is rejected with an S3
  `QuotaExceeded` (HTTP 403).
- Quotas are configured per tenant (config / a tenants table); an unset quota
  means unlimited.

### Rate limiting

Per-tenant **token-bucket** rate limiting at the gateway (requests/sec, burst),
returning S3 `SlowDown` (HTTP 503) when exhausted. In-process per gateway pod in
M4 (each pod limits independently); a shared/global limiter is a later refinement.

### QoS isolation

Quota tracking lives in the metadata transaction (consistent); rate limiting is
gateway-local and cheap. Together they cap a tenant's footprint and request rate
so one tenant's burst or scan cannot monopolize the cluster.

---

## 5. What's deferred (recorded)

- **AI ingest pipeline** — the moat; needs the in-house indexing engine as a
  standalone dependency (§1). Returns as its own milestone behind an `IndexSink`.
- **EC reconstruction/rebalance coordinator** and **per-bucket durability/encryption
  policy** — refinements over the M4 cluster-wide settings.
- **Seekable encrypted range reads**, **external KMS + key rotation**, **global
  (cross-gateway) rate limiting**, **bucket→tenant ownership** — later refinements.
- **Metadata HA** — still delegated to a future distributed engine (from M2).

---

## 6. Technology choices (additions over M2)

| Concern | Choice |
| --- | --- |
| Erasure coding | `reed-solomon-simd` |
| Symmetric encryption | `aes-gcm` (AES-256-GCM) |
| Key wrapping | AES-256-GCM under the master key (KEK) |
| Rate limiting | token bucket (in-process) |
| Tenant quotas | counters in `RedbMetaStore`, updated in the put/delete transaction |
