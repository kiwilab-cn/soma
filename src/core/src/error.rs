//! Shared error type for the core layer.

/// Errors produced while encoding, decoding, or scanning the on-disk format.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The needle magic sentinel did not match — not a needle boundary.
    #[error("needle magic mismatch")]
    BadMagic,

    /// The needle was written by an unsupported format version.
    #[error("unsupported needle format version {0}")]
    BadVersion(u8),

    /// The header CRC did not verify — the header is corrupt or torn.
    #[error("needle header crc mismatch")]
    HeaderCrc,

    /// The data CRC did not verify — the payload is corrupt (bitrot).
    #[error("needle data crc mismatch")]
    DataCrc,

    /// A buffer was shorter than required to decode a structure.
    #[error("buffer too short: need {need} bytes, have {have}")]
    Truncated {
        /// Bytes required.
        need: usize,
        /// Bytes available.
        have: usize,
    },

    /// The payload exceeds the maximum a single needle can hold.
    #[error("data length {0} exceeds u32 maximum")]
    DataTooLarge(usize),

    /// Underlying IO failure (surfaced by higher layers that do real IO).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;
