//! Metadata store for Soma — the single authority mapping `(bucket, key)` to a
//! needle location and version, with conditional-write (CAS) semantics.
//!
//! In M0 this is backed by [`RedbMetaStore`] (a pure-Rust embedded ACID B-tree).
//! Later milestones wrap the same [`MetadataStore`] trait in a Raft-replicated
//! implementation; the conditional-write evaluation that happens inside one redb
//! write transaction here moves into the Raft state-machine `apply()` then,
//! unchanged in contract (see `docs/ARCHITECTURE.md` §5.3).
//!
//! The trait is **synchronous**, matching the storage backend: the async S3 edge
//! bridges to it via `spawn_blocking`.

mod error;
mod redb_store;
mod types;

pub use error::{Error, Result};
pub use redb_store::RedbMetaStore;
pub use types::{
    BucketMeta, BucketOpts, BucketUsage, DataLayout, ETag, ListRequest, ListResult, NodeInfo,
    NodeLocation, NodeState, NodeTopology, ObjectEntry, ObjectLocations, ObjectMeta, ObjectPut,
    PgPlacement, PutCondition, Quota, RateLimit, ShardRole, SseAlgorithm, Version,
};

use soma_core::ObjectId;

/// The authority for "object name → bytes location + version".
///
/// Implementations own atomicity and conditional-write linearization; they do
/// **not** own the bytes (that is the storage backend's job). All methods are
/// synchronous.
pub trait MetadataStore: Send + Sync {
    /// Create a bucket. Errors if it already exists.
    fn create_bucket(&self, name: &str, opts: BucketOpts) -> Result<()>;

    /// Delete an empty bucket. Errors if it is missing or non-empty.
    fn delete_bucket(&self, name: &str) -> Result<()>;

    /// Fetch a bucket's metadata, if it exists.
    fn get_bucket(&self, name: &str) -> Result<Option<BucketMeta>>;

    /// Set (or clear, with `None`) a bucket's default server-side encryption
    /// (S3 `PutBucketEncryption` / `DeleteBucketEncryption`). Errors if the bucket
    /// does not exist.
    fn set_bucket_encryption(&self, name: &str, algo: Option<SseAlgorithm>) -> Result<()>;

    /// Set a bucket's storage quota (zeros = unlimited). Errors if absent.
    fn set_bucket_quota(&self, name: &str, quota: Quota) -> Result<()>;

    /// Set a bucket's request rate limit (zero rps = unlimited). Errors if absent.
    fn set_bucket_rate_limit(&self, name: &str, limit: RateLimit) -> Result<()>;

    /// List all buckets (sorted by name).
    fn list_buckets(&self) -> Result<Vec<BucketMeta>>;

    /// Commit an object's current version, subject to `cond`. The CAS is
    /// evaluated atomically; returns the new version on success.
    fn put_object(
        &self,
        bucket: &str,
        key: &str,
        put: ObjectPut,
        cond: PutCondition,
    ) -> Result<Version>;

    /// Fetch an object's current metadata, if it exists.
    fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>>;

    /// Delete an object, subject to `cond`. Idempotent: deleting an absent object
    /// succeeds unless a condition forbids it. Refunds the bucket's usage.
    fn delete_object(&self, bucket: &str, key: &str, cond: PutCondition) -> Result<()>;

    /// The tracked live usage for a bucket (zero if unknown).
    fn bucket_usage(&self, bucket: &str) -> Result<BucketUsage>;

    /// Record object ids whose bytes are now orphaned on storage nodes (e.g. the
    /// parts of a completed/aborted multipart upload), for the GC to reclaim.
    /// Overwrite/delete orphans are recorded automatically inside `put_object` /
    /// `delete_object`; this is for orphans the metadata layer can't infer.
    fn mark_garbage(&self, object_ids: &[ObjectId]) -> Result<()>;

    // --- cluster membership + placement (M3) -------------------------------

    /// Register (or re-register) a storage node, marking it `Active` and bumping
    /// its generation. `topology` records the node's failure domain / host for
    /// data-locality decisions (empty fields = unknown). `now` is the
    /// caller-supplied unix-seconds clock.
    fn register_node(
        &self,
        node_id: &str,
        endpoint: &str,
        topology: NodeTopology,
        now: u64,
    ) -> Result<()>;

    /// Record a heartbeat for a registered node. Errors with [`Error::UnknownNode`]
    /// if the node is not registered (so it re-registers).
    fn heartbeat(&self, node_id: &str, now: u64) -> Result<()>;

    /// Set a node's lifecycle state (e.g. mark it `Down` after missed heartbeats,
    /// or `Draining` for a graceful decommission). Errors with
    /// [`Error::UnknownNode`] if the node is not registered.
    fn set_node_state(&self, node_id: &str, state: NodeState) -> Result<()>;

    /// List all known members (sorted by node id).
    fn list_members(&self) -> Result<Vec<NodeInfo>>;

    /// Seed the placement-group table, but only if it is currently empty. Returns
    /// `true` if this call wrote the table, `false` if it was already populated.
    /// Idempotent under concurrent gateways (first writer wins, atomically).
    fn seed_pg_table(&self, entries: &[(u32, PgPlacement)]) -> Result<bool>;

    /// Read the entire placement-group table (sorted by pg).
    fn list_pg_table(&self) -> Result<Vec<(u32, PgPlacement)>>;

    /// List objects in a bucket (prefix + delimiter + pagination).
    fn list_objects(&self, bucket: &str, req: &ListRequest) -> Result<ListResult>;

    /// Allocate the next monotonic object id.
    fn next_object_id(&self) -> Result<ObjectId>;
}

/// Resolves an object id to the nodes that physically hold its bytes, with the
/// topology a scheduler needs for data-locality (HDFS `getFileBlockLocations`
/// analogue). Implemented by the gateway's placement view; absent in single-node
/// deployments (where there is nothing to schedule across).
pub trait LocationOracle: Send + Sync {
    /// The nodes holding `object_id` and how its bytes are laid out, or `None` if
    /// the object's placement group is currently unresolvable (no live nodes).
    /// `size` is supplied by the caller (the object's metadata).
    fn locate(&self, object_id: ObjectId, size: u64) -> Option<ObjectLocations>;
}
