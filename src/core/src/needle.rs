//! The needle: how a single object is framed inside a volume file.
//!
//! On-disk layout (little-endian), per `docs/MVP_DESIGN.md` §4.2:
//!
//! ```text
//! header (fixed 32 bytes):
//!   0..4   magic        u32   needle sentinel
//!   4      version      u8    format version
//!   5      flags        u8    bit0 = tombstone
//!   6..8   reserved     u16   0
//!   8..16  object_id    u64
//!   16..20 data_len     u32
//!   20..24 data_crc     u32   CRC32C of the payload
//!   24..28 reserved     u32   0
//!   28..32 header_crc   u32   CRC32C of bytes[0..28]
//! data:      data_len bytes
//! padding:   zeros up to an 8-byte boundary
//! ```
//!
//! Needles are 8-byte aligned so a scan can step deterministically from one to
//! the next, and the header is self-validating (magic + `header_crc`) so a scan
//! can detect a torn tail after a crash.

use crate::error::{Error, Result};
use crate::id::ObjectId;

/// Needle magic sentinel: ASCII "SOMA".
pub const MAGIC: u32 = 0x534F_4D41;

/// Current on-disk needle format version.
pub const FORMAT_VERSION: u8 = 1;

/// Fixed needle header length in bytes.
pub const HEADER_LEN: usize = 32;

/// Needle alignment in bytes; every needle starts on a multiple of this.
pub const NEEDLE_ALIGN: usize = 8;

/// `flags` bit 0: this needle is a delete marker (tombstone).
pub const FLAG_TOMBSTONE: u8 = 0b0000_0001;

// Field offsets within the header.
const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 4;
const OFF_FLAGS: usize = 5;
const OFF_OBJECT_ID: usize = 8;
const OFF_DATA_LEN: usize = 16;
const OFF_DATA_CRC: usize = 20;
const OFF_HEADER_CRC: usize = 28;

/// Round `n` up to the next multiple of `align` (a power of two).
#[inline]
pub fn align_up(n: usize, align: usize) -> usize {
    debug_assert!(align.is_power_of_two());
    (n + align - 1) & !(align - 1)
}

/// Total on-disk size of a needle carrying `data_len` payload bytes, including
/// the header and trailing alignment padding.
#[inline]
pub fn padded_needle_len(data_len: usize) -> usize {
    HEADER_LEN + align_up(data_len, NEEDLE_ALIGN)
}

/// Decoded needle header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NeedleHeader {
    /// Object id this needle stores.
    pub object_id: ObjectId,
    /// Flag bits (see [`FLAG_TOMBSTONE`]).
    pub flags: u8,
    /// Payload length in bytes.
    pub data_len: u32,
    /// CRC32C of the payload.
    pub data_crc: u32,
}

impl NeedleHeader {
    /// Whether this needle is a delete marker.
    #[inline]
    pub fn is_tombstone(&self) -> bool {
        self.flags & FLAG_TOMBSTONE != 0
    }

    /// Total on-disk size of this needle (header + payload + padding).
    #[inline]
    pub fn on_disk_len(&self) -> usize {
        padded_needle_len(self.data_len as usize)
    }

    /// Serialize this header into a fixed 32-byte array, computing `header_crc`.
    fn encode(&self) -> [u8; HEADER_LEN] {
        let mut h = [0u8; HEADER_LEN];
        h[OFF_MAGIC..OFF_MAGIC + 4].copy_from_slice(&MAGIC.to_le_bytes());
        h[OFF_VERSION] = FORMAT_VERSION;
        h[OFF_FLAGS] = self.flags;
        h[OFF_OBJECT_ID..OFF_OBJECT_ID + 8].copy_from_slice(&self.object_id.to_le_bytes());
        h[OFF_DATA_LEN..OFF_DATA_LEN + 4].copy_from_slice(&self.data_len.to_le_bytes());
        h[OFF_DATA_CRC..OFF_DATA_CRC + 4].copy_from_slice(&self.data_crc.to_le_bytes());
        let crc = crc32c::crc32c(&h[..OFF_HEADER_CRC]);
        h[OFF_HEADER_CRC..OFF_HEADER_CRC + 4].copy_from_slice(&crc.to_le_bytes());
        h
    }

    /// Decode and validate a header from the first [`HEADER_LEN`] bytes of `src`.
    ///
    /// Returns [`Error::BadMagic`] / [`Error::BadVersion`] / [`Error::HeaderCrc`]
    /// when the bytes are not a valid header — which a scanner treats as the end
    /// of valid data (a torn tail).
    pub fn decode(src: &[u8]) -> Result<NeedleHeader> {
        if src.len() < HEADER_LEN {
            return Err(Error::Truncated {
                need: HEADER_LEN,
                have: src.len(),
            });
        }
        let magic = u32::from_le_bytes(le4(src, OFF_MAGIC));
        if magic != MAGIC {
            return Err(Error::BadMagic);
        }
        let version = src[OFF_VERSION];
        if version != FORMAT_VERSION {
            return Err(Error::BadVersion(version));
        }
        let stored_crc = u32::from_le_bytes(le4(src, OFF_HEADER_CRC));
        let computed_crc = crc32c::crc32c(&src[..OFF_HEADER_CRC]);
        if stored_crc != computed_crc {
            return Err(Error::HeaderCrc);
        }
        Ok(NeedleHeader {
            object_id: u64::from_le_bytes(le8(src, OFF_OBJECT_ID)),
            flags: src[OFF_FLAGS],
            data_len: u32::from_le_bytes(le4(src, OFF_DATA_LEN)),
            data_crc: u32::from_le_bytes(le4(src, OFF_DATA_CRC)),
        })
    }
}

/// Encode a complete needle (header + payload + padding) into a fresh buffer.
///
/// `data` may be empty (a zero-length object) and may be a tombstone marker when
/// `flags` carries [`FLAG_TOMBSTONE`].
pub fn encode_needle(object_id: ObjectId, flags: u8, data: &[u8]) -> Result<Vec<u8>> {
    if data.len() > u32::MAX as usize {
        return Err(Error::DataTooLarge(data.len()));
    }
    let header = NeedleHeader {
        object_id,
        flags,
        data_len: data.len() as u32,
        data_crc: crc32c::crc32c(data),
    };
    let total = padded_needle_len(data.len());
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&header.encode());
    buf.extend_from_slice(data);
    buf.resize(total, 0); // trailing alignment padding
    Ok(buf)
}

/// Verify that `data` matches `header`'s length and CRC. Called on the read path
/// to catch silent corruption (bitrot).
pub fn verify_data(header: &NeedleHeader, data: &[u8]) -> Result<()> {
    if data.len() != header.data_len as usize {
        return Err(Error::Truncated {
            need: header.data_len as usize,
            have: data.len(),
        });
    }
    if crc32c::crc32c(data) != header.data_crc {
        return Err(Error::DataCrc);
    }
    Ok(())
}

/// One needle located by a scan: its byte offset and decoded header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScannedNeedle {
    /// Byte offset of the needle header within the volume.
    pub offset: u64,
    /// Decoded header.
    pub header: NeedleHeader,
}

impl ScannedNeedle {
    /// Byte offset of this needle's payload (immediately after the header).
    #[inline]
    pub fn data_offset(&self) -> u64 {
        self.offset + HEADER_LEN as u64
    }
}

/// Result of scanning a volume buffer for needles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanOutcome {
    /// Every intact needle found, in file order.
    pub needles: Vec<ScannedNeedle>,
    /// Offset just past the last intact needle. A volume should be truncated to
    /// this length on recovery to discard a torn tail.
    pub valid_end: u64,
}

/// Scan `buf` for needles starting at byte offset `start`.
///
/// Stops at the first position that is not a complete, header-valid needle
/// (insufficient bytes, bad magic, bad version, or bad header CRC) — that
/// position becomes [`ScanOutcome::valid_end`]. Payload CRCs are *not* checked
/// here (that is a read-path concern via [`verify_data`]); a scan only needs the
/// boundaries to rebuild the index.
pub fn scan(buf: &[u8], start: u64) -> ScanOutcome {
    let mut pos = start as usize;
    let mut needles = Vec::new();
    while pos + HEADER_LEN <= buf.len() {
        let header = match NeedleHeader::decode(&buf[pos..pos + HEADER_LEN]) {
            Ok(h) => h,
            Err(_) => break,
        };
        let total = padded_needle_len(header.data_len as usize);
        if pos + total > buf.len() {
            break; // payload is truncated — torn tail
        }
        needles.push(ScannedNeedle {
            offset: pos as u64,
            header,
        });
        pos += total;
    }
    ScanOutcome {
        needles,
        valid_end: pos as u64,
    }
}

#[inline]
fn le4(src: &[u8], off: usize) -> [u8; 4] {
    [src[off], src[off + 1], src[off + 2], src[off + 3]]
}

#[inline]
fn le8(src: &[u8], off: usize) -> [u8; 8] {
    let mut a = [0u8; 8];
    a.copy_from_slice(&src[off..off + 8]);
    a
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn align_up_basic() {
        assert_eq!(align_up(0, 8), 0);
        assert_eq!(align_up(1, 8), 8);
        assert_eq!(align_up(8, 8), 8);
        assert_eq!(align_up(9, 8), 16);
    }

    #[test]
    fn header_is_eight_byte_aligned() {
        assert_eq!(HEADER_LEN % NEEDLE_ALIGN, 0);
    }

    #[test]
    fn roundtrip_empty_payload() {
        let buf = encode_needle(7, 0, &[]).unwrap();
        assert_eq!(buf.len(), HEADER_LEN);
        let h = NeedleHeader::decode(&buf).unwrap();
        assert_eq!(h.object_id, 7);
        assert_eq!(h.data_len, 0);
        assert!(!h.is_tombstone());
        verify_data(&h, &[]).unwrap();
    }

    #[test]
    fn tombstone_flag_roundtrips() {
        let buf = encode_needle(42, FLAG_TOMBSTONE, &[]).unwrap();
        let h = NeedleHeader::decode(&buf).unwrap();
        assert!(h.is_tombstone());
    }

    #[test]
    fn corrupt_header_byte_fails_decode() {
        let mut buf = encode_needle(1, 0, b"hello").unwrap();
        buf[OFF_OBJECT_ID] ^= 0xFF; // flip an id byte; header_crc no longer matches
        assert!(matches!(NeedleHeader::decode(&buf), Err(Error::HeaderCrc)));
    }

    #[test]
    fn corrupt_data_byte_fails_verify() {
        let mut buf = encode_needle(1, 0, b"hello world").unwrap();
        let h = NeedleHeader::decode(&buf).unwrap();
        buf[HEADER_LEN] ^= 0xFF; // flip first payload byte
        let data = &buf[HEADER_LEN..HEADER_LEN + h.data_len as usize];
        assert!(matches!(verify_data(&h, data), Err(Error::DataCrc)));
    }

    #[test]
    fn bad_magic_is_detected() {
        let mut buf = encode_needle(1, 0, b"x").unwrap();
        buf[0] ^= 0xFF;
        assert!(matches!(NeedleHeader::decode(&buf), Err(Error::BadMagic)));
    }

    #[test]
    fn scan_finds_all_needles_in_order() {
        let mut vol = Vec::new();
        let payloads: [&[u8]; 4] = [b"", b"a", b"abcdefgh", b"some longer payload here"];
        for (i, p) in payloads.iter().enumerate() {
            vol.extend_from_slice(&encode_needle(i as u64, 0, p).unwrap());
        }
        let out = scan(&vol, 0);
        assert_eq!(out.needles.len(), 4);
        assert_eq!(out.valid_end, vol.len() as u64);
        for (i, n) in out.needles.iter().enumerate() {
            assert_eq!(n.header.object_id, i as u64);
            assert_eq!(n.header.data_len as usize, payloads[i].len());
        }
    }

    #[test]
    fn scan_stops_at_torn_tail() {
        let mut vol = encode_needle(0, 0, b"complete").unwrap();
        let clean_end = vol.len() as u64;
        // Append a second needle then truncate it mid-payload.
        let second = encode_needle(1, 0, b"this one is torn off").unwrap();
        vol.extend_from_slice(&second[..second.len() - 5]);
        let out = scan(&vol, 0);
        assert_eq!(out.needles.len(), 1);
        assert_eq!(out.valid_end, clean_end);
    }

    #[test]
    fn scan_stops_on_garbage_tail() {
        let mut vol = encode_needle(0, 0, b"ok").unwrap();
        let clean_end = vol.len() as u64;
        vol.extend_from_slice(&[0xAB; 10]); // not a needle
        let out = scan(&vol, 0);
        assert_eq!(out.needles.len(), 1);
        assert_eq!(out.valid_end, clean_end);
    }

    proptest! {
        #[test]
        fn prop_roundtrip(object_id in any::<u64>(), tomb in any::<bool>(), data in proptest::collection::vec(any::<u8>(), 0..2048)) {
            let flags = if tomb { FLAG_TOMBSTONE } else { 0 };
            let buf = encode_needle(object_id, flags, &data).unwrap();
            prop_assert_eq!(buf.len(), padded_needle_len(data.len()));
            prop_assert_eq!(buf.len() % NEEDLE_ALIGN, 0);
            let h = NeedleHeader::decode(&buf).unwrap();
            prop_assert_eq!(h.object_id, object_id);
            prop_assert_eq!(h.is_tombstone(), tomb);
            prop_assert_eq!(h.data_len as usize, data.len());
            let payload = &buf[HEADER_LEN..HEADER_LEN + data.len()];
            verify_data(&h, payload).unwrap();
        }

        #[test]
        fn prop_scan_reconstructs(items in proptest::collection::vec((any::<u64>(), proptest::collection::vec(any::<u8>(), 0..256)), 0..50)) {
            let mut vol = Vec::new();
            for (id, data) in &items {
                vol.extend_from_slice(&encode_needle(*id, 0, data).unwrap());
            }
            let out = scan(&vol, 0);
            prop_assert_eq!(out.valid_end, vol.len() as u64);
            prop_assert_eq!(out.needles.len(), items.len());
            for (n, (id, data)) in out.needles.iter().zip(items.iter()) {
                prop_assert_eq!(n.header.object_id, *id);
                prop_assert_eq!(n.header.data_len as usize, data.len());
            }
        }
    }
}
