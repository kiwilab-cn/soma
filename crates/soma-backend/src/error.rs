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
}

/// Backend result alias.
pub type Result<T> = std::result::Result<T, Error>;
