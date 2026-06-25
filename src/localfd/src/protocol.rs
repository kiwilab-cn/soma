//! Wire protocol for the local short-circuit read socket.
//!
//! A request names an object; the reply carries the framing of its payload, and —
//! on success — the volume file descriptor rides alongside the reply bytes as
//! ancillary data (`SCM_RIGHTS`). Each message is length-prefixed: a little-endian
//! `u32` byte count followed by the postcard-encoded body.

use serde::{Deserialize, Serialize};

/// Ask for an object's bytes as a descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalRequest {
    /// The internal object id to read.
    pub object_id: u64,
}

/// The framing reply. On [`LocalReply::Ok`] a single file descriptor for the
/// volume file is attached out-of-band via `SCM_RIGHTS`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LocalReply {
    /// The object's payload occupies `[payload_offset, payload_offset + len)` in the
    /// attached descriptor; the reader must verify it against `crc`.
    Ok {
        /// Byte offset of the payload within the attached descriptor.
        payload_offset: u64,
        /// Payload length in bytes.
        len: u32,
        /// CRC32C the reader must check the payload against.
        crc: u32,
    },
    /// No such object on this node.
    NotFound,
    /// The node could not serve the read (message is for diagnostics).
    Error(String),
}

/// Encode a message as `[u32 len][postcard body]`.
pub(crate) fn frame<T: Serialize>(msg: &T) -> Result<Vec<u8>, postcard::Error> {
    let body = postcard::to_allocvec(msg)?;
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}
