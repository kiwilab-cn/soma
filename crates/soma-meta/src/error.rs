//! Metadata store error type.

/// Errors produced by the metadata store.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error from the underlying redb engine. Boxed because redb's error
    /// types are large and would otherwise bloat every `Result` (clippy
    /// `result_large_err`).
    #[error(transparent)]
    Redb(Box<redb::Error>),

    /// (De)serialization of a stored record failed.
    #[error("serialization error: {0}")]
    Serde(#[from] postcard::Error),

    /// A conditional write (`If-Match` / `If-None-Match`) was not satisfied.
    #[error("precondition failed")]
    PreconditionFailed,

    /// The named bucket does not exist.
    #[error("no such bucket: {0}")]
    NoSuchBucket(String),

    /// The bucket already exists.
    #[error("bucket already exists: {0}")]
    BucketAlreadyExists(String),

    /// The bucket still holds objects and cannot be deleted.
    #[error("bucket not empty: {0}")]
    BucketNotEmpty(String),

    /// A bucket name failed validation.
    #[error("invalid bucket name: {0}")]
    InvalidBucketName(String),

    /// A stored object key was not valid UTF-8 (should never happen).
    #[error("stored object key is not valid utf-8")]
    NonUtf8Key,
}

// redb surfaces several distinct error types from its various operations; funnel
// them all through redb's own unified `Error`, boxed.
macro_rules! from_redb {
    ($($t:ty),* $(,)?) => {
        $(impl From<$t> for Error {
            fn from(e: $t) -> Self {
                Error::Redb(Box::new(e.into()))
            }
        })*
    };
}
from_redb!(
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError,
);

/// Metadata result alias.
pub type Result<T> = std::result::Result<T, Error>;
