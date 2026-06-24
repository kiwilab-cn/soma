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
    BucketMeta, BucketOpts, ETag, ListRequest, ListResult, ObjectEntry, ObjectMeta, ObjectPut,
    PutCondition, Quota, TenantUsage, Version,
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
    /// succeeds unless a condition forbids it. Refunds the object's owning tenant.
    fn delete_object(&self, bucket: &str, key: &str, cond: PutCondition) -> Result<()>;

    /// The tracked live usage for a tenant (zero if the tenant is unknown).
    fn tenant_usage(&self, tenant: &str) -> Result<TenantUsage>;

    /// List objects in a bucket (prefix + delimiter + pagination).
    fn list_objects(&self, bucket: &str, req: &ListRequest) -> Result<ListResult>;

    /// Allocate the next monotonic object id.
    fn next_object_id(&self) -> Result<ObjectId>;
}
