//! Public data types for the metadata store.

use serde::{Deserialize, Serialize};
use soma_core::{ObjectId, ObjectLocation};

/// An S3-style entity tag. Opaque to the metadata store: it only stores and
/// compares it (e.g. for `If-Match`). The S3 layer decides its format (an MD5
/// hex digest for single-part objects, a `hash-N` form for multipart).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ETag(pub String);

/// A per-object version number, incremented on each successful overwrite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Version(pub u64);

/// Options chosen when a bucket is created.
#[derive(Debug, Clone, Copy, Default)]
pub struct BucketOpts {
    /// Whether object versioning is enabled. Stored for forward compatibility;
    /// version *history retention* is a later milestone (M0 keeps the current
    /// version only).
    pub versioning: bool,
}

/// Stored metadata about a bucket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketMeta {
    /// Bucket name.
    pub name: String,
    /// Whether versioning is enabled.
    pub versioning: bool,
}

/// Everything needed to commit an object's current version.
#[derive(Debug, Clone)]
pub struct ObjectPut {
    /// The internal object id (allocated via [`crate::MetadataStore::next_object_id`]).
    pub object_id: ObjectId,
    /// Where the bytes live.
    pub location: ObjectLocation,
    /// Payload size in bytes.
    pub size: u64,
    /// Content tag.
    pub etag: ETag,
}

/// Stored metadata about an object's current version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMeta {
    /// The internal object id of this version.
    pub object_id: ObjectId,
    /// Where the bytes live.
    pub location: ObjectLocation,
    /// Payload size in bytes.
    pub size: u64,
    /// Content tag.
    pub etag: ETag,
    /// Version number of this object.
    pub version: Version,
}

/// Condition under which a put/delete is allowed (S3 conditional writes).
#[derive(Debug, Clone, Default)]
pub enum PutCondition {
    /// Unconditional.
    #[default]
    None,
    /// Proceed only if the current ETag matches (`If-Match`).
    IfMatch(ETag),
    /// Proceed only if the object is currently absent (`If-None-Match: *`).
    IfNoneMatch,
}

/// A `ListObjectsV2`-style request.
#[derive(Debug, Clone, Default)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectEntry {
    /// Full object key.
    pub key: String,
    /// Payload size.
    pub size: u64,
    /// Content tag.
    pub etag: ETag,
    /// Version number.
    pub version: Version,
}

/// A `ListObjectsV2`-style response.
#[derive(Debug, Clone, Default)]
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
