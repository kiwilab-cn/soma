//! Public data types for the metadata store.

use serde::{Deserialize, Serialize};
use soma_core::ObjectId;

/// An S3-style entity tag. Opaque to the metadata store: it only stores and
/// compares it (e.g. for `If-Match`). The S3 layer decides its format (an MD5
/// hex digest for single-part objects, a `hash-N` form for multipart).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ETag(pub String);

/// A per-object version number, incremented on each successful overwrite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Version(pub u64);

/// Options chosen when a bucket is created.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BucketOpts {
    /// Whether object versioning is enabled. Stored for forward compatibility;
    /// version *history retention* is a later milestone (M0 keeps the current
    /// version only).
    pub versioning: bool,
    /// The access key that owns the bucket (set at creation: create→own). Empty
    /// leaves the bucket unowned (open). See [`BucketMeta::owner`].
    #[serde(default)]
    pub owner: String,
}

/// Server-side encryption algorithm for a bucket's default encryption (S3 SSE).
/// Only SSE-S3 (`AES256`, server-managed key) is supported; SSE-KMS/SSE-C are not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SseAlgorithm {
    /// SSE-S3: AES-256 under the cluster's server-managed master key.
    Aes256,
}

/// Stored metadata about a bucket.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BucketMeta {
    /// Bucket name.
    pub name: String,
    /// Whether versioning is enabled.
    pub versioning: bool,
    /// Default server-side encryption applied to objects written without an
    /// explicit SSE header (S3 `PutBucketEncryption`). `None` = not encrypted.
    #[serde(default)]
    pub default_sse: Option<SseAlgorithm>,
    /// Per-bucket storage quota, enforced on writes (zeros = unlimited).
    #[serde(default)]
    pub quota: Quota,
    /// Per-bucket request rate limit, enforced at the gateway (zero = unlimited).
    #[serde(default)]
    pub rate_limit: RateLimit,
    /// Owning access key (the tenant). Empty = unowned, which means **open**: any
    /// authenticated key may read and write (back-compat / single-tenant default).
    /// When set, only the owner may write; reads follow `public_read` / `readers`.
    #[serde(default)]
    pub owner: String,
    /// If true, any authenticated key may read this bucket (e.g. a shared `global`
    /// bucket). Writes are still owner-only.
    #[serde(default)]
    pub public_read: bool,
    /// Additional access keys granted read access (beyond the owner).
    #[serde(default)]
    pub readers: Vec<String>,
}

impl BucketMeta {
    /// Whether `key` may read this bucket. Unowned buckets are open to all.
    pub fn can_read(&self, key: &str) -> bool {
        self.owner.is_empty()
            || self.owner == key
            || self.public_read
            || self.readers.iter().any(|r| r == key)
    }

    /// Whether `key` may write this bucket. Unowned buckets are open; otherwise
    /// writes are owner-only.
    pub fn can_write(&self, key: &str) -> bool {
        self.owner.is_empty() || self.owner == key
    }
}

/// Everything needed to commit an object's current version.
///
/// Distributed model: the metadata is **logical** — it identifies the object by
/// `object_id`; the physical byte location lives only in each storage node's
/// node-local index (placement is computed from `object_id`). See
/// `docs/M2_DESIGN.md` §2.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectPut {
    /// The internal object id (allocated via [`crate::MetadataStore::next_object_id`]).
    pub object_id: ObjectId,
    /// Payload size in bytes.
    pub size: u64,
    /// Content tag.
    pub etag: ETag,
    /// Creation time (unix seconds), supplied by the caller (the store does no
    /// clock access). Surfaced as S3 `LastModified`.
    pub created_at: u64,
    /// Whether the stored bytes are an encryption frame (so reads decrypt them).
    pub encrypted: bool,
}

/// One entry in a batched object commit — the same arguments as
/// [`crate::MetadataStore::put_object`], carrying its own bucket/key/condition so
/// a batch can span buckets and keys. See
/// [`crate::MetadataStore::put_object_batch`].
#[derive(Debug, Clone)]
pub struct ObjectPutItem {
    /// Target bucket.
    pub bucket: String,
    /// Target object key.
    pub key: String,
    /// The version to commit.
    pub put: ObjectPut,
    /// Conditional-write precondition, evaluated independently for this item.
    pub cond: PutCondition,
}

/// One entry in a batched object delete — the same arguments as
/// [`crate::MetadataStore::delete_object`]. See
/// [`crate::MetadataStore::delete_object_batch`].
#[derive(Debug, Clone)]
pub struct ObjectDeleteItem {
    /// Target bucket.
    pub bucket: String,
    /// Target object key.
    pub key: String,
    /// Conditional-delete precondition, evaluated independently for this item.
    pub cond: PutCondition,
}

/// A per-bucket resource quota. Zero in a dimension means unlimited.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quota {
    /// Maximum total live bytes in the bucket (0 = unlimited).
    pub max_bytes: u64,
    /// Maximum live object count in the bucket (0 = unlimited).
    pub max_objects: u64,
}

/// A per-bucket request rate limit (token bucket). Zero `rps` = no limit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct RateLimit {
    /// Sustained requests per second (0 = no limit).
    pub rps: f64,
    /// Token-bucket burst capacity (requests).
    pub burst: f64,
}

/// A bucket's tracked live usage (current object versions only).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketUsage {
    /// Total live bytes.
    pub bytes: u64,
    /// Total live object count.
    pub objects: u64,
}

/// Stored metadata about an object's current version (logical — no byte location).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMeta {
    /// The internal object id of this version. Storage nodes resolve this to
    /// bytes via their node-local index.
    pub object_id: ObjectId,
    /// Payload size in bytes.
    pub size: u64,
    /// Content tag.
    pub etag: ETag,
    /// Version number of this object.
    pub version: Version,
    /// Creation time (unix seconds).
    pub created_at: u64,
    /// Whether the stored bytes are an encryption frame (so reads decrypt them).
    #[serde(default)]
    pub encrypted: bool,
}

/// Condition under which a put/delete is allowed (S3 conditional writes).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum PutCondition {
    /// Unconditional.
    #[default]
    None,
    /// Proceed only if the current ETag matches (`If-Match`).
    IfMatch(ETag),
    /// Proceed only if the object is currently absent (`If-None-Match: *`).
    IfNoneMatch,
}

/// Lifecycle state of a storage node in the cluster (M3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum NodeState {
    /// Registered, not yet serving placement (reserved for M3b rebalance).
    Joining,
    /// Live and serving.
    #[default]
    Active,
    /// Being decommissioned; data migrates off before removal (M3c).
    Draining,
    /// Missed heartbeats past the liveness threshold.
    Down,
}

/// A storage node's physical topology, used for data-locality decisions (which
/// failure domain / host a replica lives on). Populated from the orchestrator
/// (e.g. Kubernetes downward API: `topology.kubernetes.io/zone`,
/// `kubernetes.io/hostname`). Empty fields mean "unknown" — locality is then a
/// no-op, never an error.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeTopology {
    /// Failure domain (availability zone / rack). Empty = unknown.
    pub zone: String,
    /// Physical host the node runs on (the unit of read short-circuiting). Empty =
    /// unknown.
    pub host: String,
}

/// Cluster membership record for one storage node (M3). Keyed by a stable
/// `node_id` (never an array index), so adding/removing a node never renumbers
/// the others.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Stable node identity (e.g. the StatefulSet pod name).
    pub node_id: String,
    /// The address other nodes reach this node at (e.g. `http://soma-storage-0…:9200`).
    pub endpoint: String,
    /// Lifecycle state.
    pub state: NodeState,
    /// Unix seconds of the last heartbeat (or registration).
    pub last_heartbeat: u64,
    /// Bumped each time the node re-registers (e.g. after a restart).
    pub generation: u64,
    /// Physical topology (zone / host) for data-locality scheduling. Defaulted for
    /// records written before topology was tracked.
    #[serde(default)]
    pub zone: String,
    /// Host the node runs on — the unit at which a co-located reader can
    /// short-circuit to local storage. Defaulted for older records.
    #[serde(default)]
    pub host: String,
}

/// The role a node plays for one object's bytes, in a [`NodeLocation`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardRole {
    /// Holds a full copy of the object (replication).
    Replica,
    /// Holds erasure-coded **data** shard `index` (0-based).
    DataShard { index: usize },
    /// Holds erasure-coded **parity** shard `index` (0-based).
    ParityShard { index: usize },
}

/// How an object's bytes are laid out across its nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataLayout {
    /// `width` full replicas; a read needs any one node.
    Replicated { width: usize },
    /// Reed-Solomon `data_shards + parity_shards`; a read needs any `data_shards`
    /// of them (and locality is therefore weaker — see `docs/M4_DESIGN.md`).
    Erasure {
        /// Number of data shards (`k`).
        data_shards: usize,
        /// Number of parity shards (`m`).
        parity_shards: usize,
    },
}

/// One node holding (part of) an object, with the topology a scheduler needs to
/// decide locality. The equivalent of an HDFS block location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeLocation {
    /// Stable node id (matches [`NodeInfo::node_id`]).
    pub node_id: String,
    /// The node's reachable endpoint.
    pub endpoint: String,
    /// Failure domain (empty = unknown).
    pub zone: String,
    /// Host (empty = unknown) — compare against the reader's host to decide
    /// whether a local short-circuit read is possible.
    pub host: String,
    /// What this node holds for the object (replica / data / parity shard).
    pub role: ShardRole,
}

/// Where an object's bytes live, plus how they are laid out — the answer to
/// "which nodes hold object X" (HDFS `getFileBlockLocations` analogue).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectLocations {
    /// The object's internal id (placement is computed from it).
    pub object_id: ObjectId,
    /// Object size in bytes.
    pub size: u64,
    /// Replication or erasure layout.
    pub layout: DataLayout,
    /// Holding nodes in placement order.
    pub nodes: Vec<NodeLocation>,
}

/// The set of nodes a placement group's objects live on (M3). The stored
/// PG→nodes table is the authority for placement; the consistent-hash ring only
/// computes the *target* mapping (see `docs/M3_DESIGN.md` §2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgPlacement {
    /// The **acting** node ids holding this PG's replicas/shards, in placement
    /// order. Reads and writes always reach this set; it is the durable home.
    pub node_ids: Vec<String>,
    /// The **target** node ids when this PG is migrating (empty otherwise). During
    /// migration writes go to `node_ids ∪ target` and reads try `target` then
    /// `node_ids`; on finalize, `node_ids` becomes `target` and this clears.
    #[serde(default)]
    pub target: Vec<String>,
    /// Bumped on every change to this PG's placement — the linearization point so
    /// gateways can detect migrations/finalizes when they refresh.
    pub generation: u64,
}

impl PgPlacement {
    /// Whether this PG is currently migrating.
    pub fn is_migrating(&self) -> bool {
        !self.target.is_empty()
    }
}

/// A `ListObjectsV2`-style request.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ListRequest {
    /// Only keys beginning with this prefix.
    pub prefix: String,
    /// If set, roll keys sharing a substring up to the delimiter into common
    /// prefixes (S3 "directory" emulation).
    pub delimiter: Option<String>,
    /// Opaque resume token from a previous truncated response.
    pub continuation_token: Option<Vec<u8>>,
    /// Maximum keys (objects + common prefixes) to return. 0 means the default
    /// (1000); values above 1000 are clamped.
    pub max_keys: usize,
}

/// One object in a listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectEntry {
    /// Full object key.
    pub key: String,
    /// Payload size.
    pub size: u64,
    /// Content tag.
    pub etag: ETag,
    /// Version number.
    pub version: Version,
    /// Creation time (unix seconds).
    pub created_at: u64,
}

/// A `ListObjectsV2`-style response.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ListResult {
    /// Matching objects (sorted by key).
    pub objects: Vec<ObjectEntry>,
    /// Rolled-up common prefixes (sorted), when a delimiter was given.
    pub common_prefixes: Vec<String>,
    /// Opaque token to pass back to fetch the next page; `None` when complete.
    pub next_continuation_token: Option<Vec<u8>>,
    /// Whether the listing was truncated.
    pub is_truncated: bool,
}
