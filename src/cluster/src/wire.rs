//! Postcard-encoded request/reply payloads carried over the gRPC `Frame`.

use serde::{Deserialize, Serialize};
use soma_meta::{
    BucketMeta, BucketOpts, ListRequest, ListResult, ObjectMeta, ObjectPut, PutCondition,
    TenantUsage, Version,
};

/// A metadata operation (mirrors the `MetadataStore` trait).
#[derive(Serialize, Deserialize)]
pub(crate) enum MetaRequest {
    CreateBucket {
        name: String,
        opts: BucketOpts,
    },
    DeleteBucket {
        name: String,
    },
    GetBucket {
        name: String,
    },
    ListBuckets,
    PutObject {
        bucket: String,
        key: String,
        put: ObjectPut,
        cond: PutCondition,
    },
    GetObject {
        bucket: String,
        key: String,
    },
    DeleteObject {
        bucket: String,
        key: String,
        cond: PutCondition,
    },
    ListObjects {
        bucket: String,
        req: ListRequest,
    },
    NextObjectId,
    TenantUsage {
        tenant: String,
    },
}

/// A metadata reply.
#[derive(Serialize, Deserialize)]
pub(crate) enum MetaReply {
    Unit,
    Bucket(Option<BucketMeta>),
    Buckets(Vec<BucketMeta>),
    Version(Version),
    Object(Option<ObjectMeta>),
    List(ListResult),
    ObjectId(u64),
    Usage(TenantUsage),
}

/// A storage operation (mirrors the `StorageBackend` trait). Ranges travel as a
/// plain `(offset, length)` so `ByteRange` needs no serde.
#[derive(Serialize, Deserialize)]
pub(crate) enum StorageRequest {
    Put {
        object_id: u64,
        data: Vec<u8>,
    },
    Get {
        object_id: u64,
        range: Option<(u64, u64)>,
    },
    Delete {
        object_id: u64,
    },
    Sync,
    Checkpoint,
}

/// A storage reply.
#[derive(Serialize, Deserialize)]
pub(crate) enum StorageReply {
    Data(Vec<u8>),
    Unit,
}

/// A transport-stable error: a kind tag plus a message, so the caller can
/// reconstruct the semantic error (see `Error::from_remote`).
#[derive(Serialize, Deserialize)]
pub(crate) struct WireError {
    pub kind: String,
    pub message: String,
}
