//! Backend error type.

/// Errors produced by the storage backend.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A core encoding/decoding/scan error.
    #[error(transparent)]
    Core(#[from] soma_core::Error),

    /// Underlying filesystem IO failure.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// A location referenced a volume the backend does not have open.
    #[error("volume {0} not found")]
    VolumeNotFound(u32),

    /// The on-disk header at a location disagreed with the location metadata.
    #[error("location mismatch at volume {volume} offset {offset}: {detail}")]
    LocationMismatch {
        /// Volume id.
        volume: u32,
        /// Byte offset.
        offset: u64,
        /// What disagreed.
        detail: &'static str,
    },

    /// A requested byte range fell outside the object.
    #[error("invalid range: offset {offset} len {len} exceed object size {size}")]
    BadRange {
        /// Range start within the object.
        offset: u64,
        /// Range length.
        len: u64,
        /// Object payload size.
        size: u32,
    },

    /// A volume file name could not be parsed into a volume id.
    #[error("malformed volume file name: {0}")]
    BadVolumeName(String),

    /// No live object with this id is present on this node.
    #[error("object {0} not found")]
    ObjectNotFound(u64),

    /// An error surfaced from a remote storage node over RPC.
    #[error("remote storage error: {0}")]
    Remote(String),

    /// Encryption, decryption, or key (un)wrapping failed — a bad master key, a
    /// corrupt ciphertext frame, or an authentication-tag mismatch.
    #[error("crypto error: {0}")]
    Crypto(&'static str),

    /// Erasure coding failed — too few shards survive to reconstruct the object,
    /// or the Reed-Solomon encode/decode itself failed.
    #[error("erasure coding error: {0}")]
    Erasure(&'static str),
}

impl Error {
    /// A short, stable kind tag used to reconstruct the error across RPC.
    pub fn kind(&self) -> &'static str {
        match self {
            Error::BadRange { .. } => "bad_range",
            Error::ObjectNotFound(_) => "object_not_found",
            _ => "internal",
        }
    }

    /// Reconstruct an error from a `(kind, message)` pair received over RPC.
    pub fn from_remote(kind: &str, message: String) -> Self {
        match kind {
            // The exact numbers/ids are not needed by the caller: the S3 layer
            // only distinguishes BadRange (-> 416), and the replicated backend
            // only matches the ObjectNotFound variant (to drive read-repair).
            "bad_range" => Error::BadRange {
                offset: 0,
                len: 0,
                size: 0,
            },
            "object_not_found" => Error::ObjectNotFound(0),
            _ => Error::Remote(message),
        }
    }
}

/// Backend result alias.
pub type Result<T> = std::result::Result<T, Error>;
