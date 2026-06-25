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
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct BucketOpts {
    /// Whether object versioning is enabled. Stored for forward compatibility;
    /// version *history retention* is a later milestone (M0 keeps the current
    /// version only).
    pub versioning: bool,
}

/// Server-side encryption algorithm for a bucket's default encryption (S3 SSE).
/// Only SSE-S3 (`AES256`, server-managed key) is supported; SSE-KMS/SSE-C are not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SseAlgorithm {
    /// SSE-S3: AES-256 under the cluster's server-managed master key.
    Aes256,
}

/// Stored metadata about a bucket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketMeta {
    /// Bucket name.
    pub name: String,
    /// Whether versioning is enabled.
    pub versioning: bool,
    /// Default server-side encryption applied to objects written without an
    /// explicit SSE header (S3 `PutBucketEncryption`). `None` = not encrypted.
    #[serde(default)]
    pub default_sse: Option<SseAlgorithm>,
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
    /// Owning tenant (the access key). Empty disables quota accounting for this
    /// put. When set, the store charges the tenant's usage and enforces `quota`
    /// atomically inside the commit, refunding any overwritten version's owner.
    pub tenant: String,
    /// The owning tenant's quota, enforced when `tenant` is non-empty.
    pub quota: Quota,
    /// Whether the stored bytes are an encryption frame (so reads decrypt them).
    pub encrypted: bool,
}

/// A per-tenant resource quota. Zero in a dimension means unlimited.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quota {
    /// Maximum total live bytes for the tenant (0 = unlimited).
    pub max_bytes: u64,
    /// Maximum live object count for the tenant (0 = unlimited).
    pub max_objects: u64,
}

/// A tenant's tracked live usage (current object versions only).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantUsage {
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
    /// Owning tenant (the access key), used to refund quota on overwrite/delete.
    /// Empty when the object was written without QoS.
    pub tenant: String,
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
