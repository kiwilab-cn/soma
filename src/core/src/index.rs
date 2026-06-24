//! The in-RAM hot index and its `.idx` checkpoint entry codec.
//!
//! The hot index maps an [`ObjectId`] to the byte location of its needle within
//! a volume. It is a **derived cache, never an authority** (see
//! `docs/ARCHITECTURE.md` §4.4): it can always be rebuilt by scanning the volume
//! and replaying the `.idx` checkpoint, so losing it is never data loss.

use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::id::{ObjectId, VolumeId};
use crate::needle::{ScanOutcome, FLAG_TOMBSTONE};

/// Location of a needle's payload within its volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NeedleLoc {
    /// Byte offset of the needle *header* within the volume file.
    pub offset: u64,
    /// Payload length in bytes.
    pub size: u32,
    /// Needle flag bits (see `FLAG_TOMBSTONE`).
    pub flags: u8,
}

impl NeedleLoc {
    /// Whether this location refers to a tombstone (deleted) needle.
    #[inline]
    pub fn is_tombstone(&self) -> bool {
        self.flags & FLAG_TOMBSTONE != 0
    }
}

/// A fully-qualified object location: which volume, and where within it.
///
/// This is what the metadata store persists as the authority for "object name →
/// bytes", and what the storage backend consumes to read/delete. `NeedleLoc`
/// alone is volume-relative; `ObjectLocation` adds the [`VolumeId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ObjectLocation {
    /// Volume holding the needle.
    pub volume: VolumeId,
    /// Location of the needle within that volume.
    pub needle: NeedleLoc,
}

impl ObjectLocation {
    /// Construct from a volume id and a within-volume location.
    #[inline]
    pub fn new(volume: VolumeId, needle: NeedleLoc) -> Self {
        Self { volume, needle }
    }

    /// Whether this location refers to a tombstone (deleted) needle.
    #[inline]
    pub fn is_tombstone(&self) -> bool {
        self.needle.is_tombstone()
    }
}

/// Serialized size of one `.idx` checkpoint entry.
pub const IDX_ENTRY_LEN: usize = 24;

/// One `.idx` checkpoint entry: an object id paired with its location.
///
/// On-disk layout (little-endian, [`IDX_ENTRY_LEN`] bytes):
/// `object_id u64 | offset u64 | size u32 | flags u8 | reserved [u8;3]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdxEntry {
    /// Object id.
    pub object_id: ObjectId,
    /// Its location.
    pub loc: NeedleLoc,
}

impl IdxEntry {
    /// Serialize into a fixed [`IDX_ENTRY_LEN`]-byte array.
    pub fn encode(&self) -> [u8; IDX_ENTRY_LEN] {
        let mut b = [0u8; IDX_ENTRY_LEN];
        b[0..8].copy_from_slice(&self.object_id.to_le_bytes());
        b[8..16].copy_from_slice(&self.loc.offset.to_le_bytes());
        b[16..20].copy_from_slice(&self.loc.size.to_le_bytes());
        b[20] = self.loc.flags;
        b
    }

    /// Decode from the first [`IDX_ENTRY_LEN`] bytes of `src`.
    pub fn decode(src: &[u8]) -> Result<IdxEntry> {
        if src.len() < IDX_ENTRY_LEN {
            return Err(Error::Truncated {
                need: IDX_ENTRY_LEN,
                have: src.len(),
            });
        }
        let mut id = [0u8; 8];
        id.copy_from_slice(&src[0..8]);
        let mut off = [0u8; 8];
        off.copy_from_slice(&src[8..16]);
        let mut sz = [0u8; 4];
        sz.copy_from_slice(&src[16..20]);
        Ok(IdxEntry {
            object_id: u64::from_le_bytes(id),
            loc: NeedleLoc {
                offset: u64::from_le_bytes(off),
                size: u32::from_le_bytes(sz),
                flags: src[20],
            },
        })
    }
}

/// In-RAM map from object id to needle location for a single volume.
#[derive(Debug, Default, Clone)]
pub struct HotIndex {
    map: HashMap<ObjectId, NeedleLoc>,
}

impl HotIndex {
    /// An empty index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or overwrite a location. Callers apply needles in file order, so a
    /// later write for the same id correctly replaces an earlier one
    /// (newest-wins).
    pub fn insert(&mut self, object_id: ObjectId, loc: NeedleLoc) {
        self.map.insert(object_id, loc);
    }

    /// Look up a location.
    pub fn get(&self, object_id: ObjectId) -> Option<NeedleLoc> {
        self.map.get(&object_id).copied()
    }

    /// Remove a location, returning it if present.
    pub fn remove(&mut self, object_id: ObjectId) -> Option<NeedleLoc> {
        self.map.remove(&object_id)
    }

    /// Number of live entries (tombstones included).
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Iterate entries (order unspecified).
    pub fn iter(&self) -> impl Iterator<Item = (ObjectId, NeedleLoc)> + '_ {
        self.map.iter().map(|(&id, &loc)| (id, loc))
    }

    /// Apply a [`ScanOutcome`] in file order, replaying needles into the index.
    /// Used to rebuild from a volume scan during recovery.
    pub fn apply_scan(&mut self, scan: &ScanOutcome) {
        for n in &scan.needles {
            self.insert(
                n.header.object_id,
                NeedleLoc {
                    offset: n.offset,
                    size: n.header.data_len,
                    flags: n.header.flags,
                },
            );
        }
    }

    /// Serialize the whole index as a packed `.idx` blob (no header — the backend
    /// frames it with its own checkpoint metadata).
    pub fn to_idx_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.map.len() * IDX_ENTRY_LEN);
        for (object_id, loc) in self.iter() {
            out.extend_from_slice(&IdxEntry { object_id, loc }.encode());
        }
        out
    }

    /// Rebuild an index from a packed `.idx` blob produced by [`to_idx_bytes`].
    pub fn from_idx_bytes(buf: &[u8]) -> Result<HotIndex> {
        if buf.len() % IDX_ENTRY_LEN != 0 {
            return Err(Error::Truncated {
                need: IDX_ENTRY_LEN,
                have: buf.len() % IDX_ENTRY_LEN,
            });
        }
        let mut idx = HotIndex::new();
        for chunk in buf.chunks_exact(IDX_ENTRY_LEN) {
            let e = IdxEntry::decode(chunk)?;
            idx.insert(e.object_id, e.loc);
        }
        Ok(idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::needle::{encode_needle, scan, FLAG_TOMBSTONE};
    use proptest::prelude::*;

    #[test]
    fn insert_get_remove() {
        let mut idx = HotIndex::new();
        let loc = NeedleLoc {
            offset: 32,
            size: 10,
            flags: 0,
        };
        idx.insert(1, loc);
        assert_eq!(idx.get(1), Some(loc));
        assert_eq!(idx.len(), 1);
        assert_eq!(idx.remove(1), Some(loc));
        assert!(idx.is_empty());
    }

    #[test]
    fn newest_write_wins() {
        let mut idx = HotIndex::new();
        idx.insert(
            1,
            NeedleLoc {
                offset: 0,
                size: 1,
                flags: 0,
            },
        );
        idx.insert(
            1,
            NeedleLoc {
                offset: 64,
                size: 2,
                flags: 0,
            },
        );
        assert_eq!(idx.get(1).unwrap().offset, 64);
    }

    #[test]
    fn idx_entry_roundtrip() {
        let e = IdxEntry {
            object_id: 0xDEAD_BEEF,
            loc: NeedleLoc {
                offset: 4096,
                size: 1234,
                flags: FLAG_TOMBSTONE,
            },
        };
        let bytes = e.encode();
        assert_eq!(bytes.len(), IDX_ENTRY_LEN);
        assert_eq!(IdxEntry::decode(&bytes).unwrap(), e);
    }

    #[test]
    fn apply_scan_matches_direct_locations() {
        // Build a volume of three needles, scan it, and confirm the rebuilt index
        // points each id at the right offset/size.
        let mut vol = Vec::new();
        let mut expected = Vec::new();
        for (id, data) in [(10u64, &b""[..]), (20, &b"hi"[..]), (30, &b"payload!!"[..])] {
            expected.push((id, vol.len() as u64, data.len() as u32));
            vol.extend_from_slice(&encode_needle(id, 0, data).unwrap());
        }
        let mut idx = HotIndex::new();
        idx.apply_scan(&scan(&vol, 0));
        for (id, off, size) in expected {
            let loc = idx.get(id).unwrap();
            assert_eq!(loc.offset, off);
            assert_eq!(loc.size, size);
        }
    }

    #[test]
    fn idx_blob_roundtrip_equivalent() {
        let mut idx = HotIndex::new();
        idx.insert(
            1,
            NeedleLoc {
                offset: 0,
                size: 5,
                flags: 0,
            },
        );
        idx.insert(
            2,
            NeedleLoc {
                offset: 64,
                size: 9,
                flags: FLAG_TOMBSTONE,
            },
        );
        let blob = idx.to_idx_bytes();
        let rebuilt = HotIndex::from_idx_bytes(&blob).unwrap();
        assert_eq!(rebuilt.len(), 2);
        assert_eq!(rebuilt.get(1), idx.get(1));
        assert_eq!(rebuilt.get(2), idx.get(2));
    }

    #[test]
    fn from_idx_bytes_rejects_ragged_blob() {
        assert!(HotIndex::from_idx_bytes(&[0u8; IDX_ENTRY_LEN + 3]).is_err());
    }

    proptest! {
        #[test]
        fn prop_idx_blob_roundtrip(entries in proptest::collection::hash_map(any::<u64>(), (any::<u64>(), any::<u32>(), any::<u8>()), 0..100)) {
            let mut idx = HotIndex::new();
            for (id, (offset, size, flags)) in &entries {
                idx.insert(*id, NeedleLoc { offset: *offset, size: *size, flags: *flags });
            }
            let rebuilt = HotIndex::from_idx_bytes(&idx.to_idx_bytes()).unwrap();
            prop_assert_eq!(rebuilt.len(), entries.len());
            for (id, (offset, size, flags)) in &entries {
                let loc = rebuilt.get(*id).unwrap();
                prop_assert_eq!(loc.offset, *offset);
                prop_assert_eq!(loc.size, *size);
                prop_assert_eq!(loc.flags, *flags);
            }
        }
    }
}
