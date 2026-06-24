//! `.idx` checkpoint file framing.
//!
//! An `.idx` file is a crash-recovery accelerator: it snapshots a volume's hot
//! index as of a `checkpoint_offset`, so on restart the backend only has to scan
//! the volume from that offset forward instead of from the beginning. It is
//! purely derived state — a corrupt or missing `.idx` is never fatal; recovery
//! falls back to a full scan from offset 0.
//!
//! Layout (little-endian):
//!
//! ```text
//! header (24 bytes):
//!   0..4   magic              u32   "SIDX"
//!   4      version            u8
//!   5..8   reserved           [u8;3]
//!   8..16  checkpoint_offset  u64   volume bytes covered by this snapshot
//!   16..20 entry_count        u32
//!   20..24 reserved           u32
//! entries:  entry_count * IDX_ENTRY_LEN bytes (see soma_core::IdxEntry)
//! trailer:  crc32c u32 over [header .. end of entries]
//! ```

use soma_core::{HotIndex, IDX_ENTRY_LEN};

const MAGIC: u32 = 0x5849_4453; // "SIDX" little-endian
const VERSION: u8 = 1;
const HEADER_LEN: usize = 24;

/// A decoded `.idx` snapshot: the index plus the volume offset it covers.
pub struct IdxSnapshot {
    /// Volume bytes covered; recovery scans the volume from here forward.
    pub checkpoint_offset: u64,
    /// The reconstructed hot index.
    pub index: HotIndex,
}

/// Serialize a hot index and its checkpoint offset into `.idx` file bytes.
pub fn encode(checkpoint_offset: u64, index: &HotIndex) -> Vec<u8> {
    let entries = index.to_idx_bytes();
    debug_assert_eq!(entries.len() % IDX_ENTRY_LEN, 0);
    let entry_count = (entries.len() / IDX_ENTRY_LEN) as u32;

    let mut buf = Vec::with_capacity(HEADER_LEN + entries.len() + 4);
    buf.extend_from_slice(&MAGIC.to_le_bytes());
    buf.push(VERSION);
    buf.extend_from_slice(&[0u8; 3]); // reserved
    buf.extend_from_slice(&checkpoint_offset.to_le_bytes());
    buf.extend_from_slice(&entry_count.to_le_bytes());
    buf.extend_from_slice(&[0u8; 4]); // reserved
    buf.extend_from_slice(&entries);
    let crc = crc32c::crc32c(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

/// Decode `.idx` file bytes. Returns `None` (rather than an error) when the file
/// is too short, has a bad magic/version, or fails its CRC — callers treat a
/// bad `.idx` as "no checkpoint" and fall back to a full scan.
pub fn decode(buf: &[u8]) -> Option<IdxSnapshot> {
    if buf.len() < HEADER_LEN + 4 {
        return None;
    }
    if u32::from_le_bytes(arr4(buf, 0)) != MAGIC {
        return None;
    }
    if buf[4] != VERSION {
        return None;
    }
    let checkpoint_offset = u64::from_le_bytes(arr8(buf, 8));
    let entry_count = u32::from_le_bytes(arr4(buf, 16)) as usize;

    let entries_len = entry_count.checked_mul(IDX_ENTRY_LEN)?;
    let total = HEADER_LEN.checked_add(entries_len)?.checked_add(4)?;
    if buf.len() != total {
        return None;
    }

    let stored_crc = u32::from_le_bytes(arr4(buf, total - 4));
    if crc32c::crc32c(&buf[..total - 4]) != stored_crc {
        return None;
    }

    let entries = &buf[HEADER_LEN..HEADER_LEN + entries_len];
    let index = HotIndex::from_idx_bytes(entries).ok()?;
    Some(IdxSnapshot {
        checkpoint_offset,
        index,
    })
}

#[inline]
fn arr4(src: &[u8], off: usize) -> [u8; 4] {
    [src[off], src[off + 1], src[off + 2], src[off + 3]]
}

#[inline]
fn arr8(src: &[u8], off: usize) -> [u8; 8] {
    let mut a = [0u8; 8];
    a.copy_from_slice(&src[off..off + 8]);
    a
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use soma_core::NeedleLoc;

    fn sample_index() -> HotIndex {
        let mut idx = HotIndex::new();
        idx.insert(
            1,
            NeedleLoc {
                offset: 0,
                size: 10,
                flags: 0,
            },
        );
        idx.insert(
            2,
            NeedleLoc {
                offset: 64,
                size: 20,
                flags: 1,
            },
        );
        idx
    }

    #[test]
    fn roundtrip() {
        let idx = sample_index();
        let bytes = encode(4096, &idx);
        let snap = decode(&bytes).expect("decodes");
        assert_eq!(snap.checkpoint_offset, 4096);
        assert_eq!(snap.index.len(), 2);
        assert_eq!(snap.index.get(1), idx.get(1));
        assert_eq!(snap.index.get(2), idx.get(2));
    }

    #[test]
    fn empty_index_roundtrips() {
        let bytes = encode(0, &HotIndex::new());
        let snap = decode(&bytes).expect("decodes");
        assert_eq!(snap.checkpoint_offset, 0);
        assert!(snap.index.is_empty());
    }

    #[test]
    fn corruption_returns_none() {
        let mut bytes = encode(4096, &sample_index());
        let n = bytes.len();
        bytes[n - 1] ^= 0xFF; // break the trailing CRC
        assert!(decode(&bytes).is_none());
    }

    #[test]
    fn truncation_returns_none() {
        let bytes = encode(4096, &sample_index());
        assert!(decode(&bytes[..bytes.len() - 10]).is_none());
        assert!(decode(&[]).is_none());
    }

    #[test]
    fn bad_magic_returns_none() {
        let mut bytes = encode(0, &HotIndex::new());
        bytes[0] ^= 0xFF;
        assert!(decode(&bytes).is_none());
    }
}
