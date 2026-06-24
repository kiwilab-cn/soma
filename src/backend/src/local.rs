//! `LocalFsBackend`: the M0 single-node storage backend.
//!
//! Bytes live in append-only `<data_dir>/volumes/<id>.vol` files, one needle per
//! object; a sibling `<id>.idx` checkpoint accelerates recovery. All access is
//! serialized by a single lock in M0 (correctness over concurrency); later
//! milestones relax this.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use soma_core::{
    encode_needle, scan, verify_data, NeedleHeader, NeedleLoc, ObjectId, ObjectLocation, VolumeId,
    FLAG_TOMBSTONE, HEADER_LEN,
};

use crate::error::{Error, Result};
use crate::{idxfile, ByteRange, StorageBackend};

/// Backend configuration.
#[derive(Debug, Clone, Copy)]
pub struct BackendConfig {
    /// Rotate to a new volume once the active one would exceed this many bytes.
    pub volume_max: u64,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            volume_max: 4 * 1024 * 1024 * 1024, // 4 GiB
        }
    }
}

/// One open volume: its file, append cursor, and hot index.
struct Volume {
    id: VolumeId,
    file: File,
    write_offset: u64,
    index: soma_core::HotIndex,
}

impl Volume {
    /// Create a fresh, empty volume file.
    fn create(dir: &Path, id: VolumeId) -> Result<Volume> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(vol_path(dir, id))?;
        Ok(Volume {
            id,
            file,
            write_offset: 0,
            index: soma_core::HotIndex::new(),
        })
    }

    /// Open an existing volume and recover its index + clean tail.
    ///
    /// Loads the `.idx` checkpoint (if any), scans the volume forward from the
    /// checkpoint offset to rebuild the index increment, and truncates any torn
    /// tail left by a crash mid-append.
    fn recover(dir: &Path, id: VolumeId) -> Result<Volume> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(vol_path(dir, id))?;
        let file_len = file.metadata()?.len();

        // Load checkpoint; a missing/corrupt .idx just means "scan from 0".
        let (mut index, scan_from) = match std::fs::read(idx_path(dir, id)) {
            Ok(bytes) => match idxfile::decode(&bytes) {
                Some(snap) if snap.checkpoint_offset <= file_len => {
                    (snap.index, snap.checkpoint_offset)
                }
                _ => (soma_core::HotIndex::new(), 0),
            },
            Err(_) => (soma_core::HotIndex::new(), 0),
        };

        // Scan the delta region and fold needles into the index with absolute
        // offsets (scan reports offsets relative to the region start).
        let region_len = (file_len - scan_from) as usize;
        let mut buf = vec![0u8; region_len];
        file.read_exact_at(&mut buf, scan_from)?;
        let out = scan(&buf, 0);
        for n in &out.needles {
            index.insert(
                n.header.object_id,
                NeedleLoc {
                    offset: scan_from + n.offset,
                    size: n.header.data_len,
                    flags: n.header.flags,
                },
            );
        }
        let valid_end = scan_from + out.valid_end;

        // Discard a torn tail so future appends start at a clean boundary.
        if valid_end < file_len {
            file.set_len(valid_end)?;
            file.sync_all()?;
        }

        Ok(Volume {
            id,
            file,
            write_offset: valid_end,
            index,
        })
    }
}

/// Mutable, lock-guarded backend state.
struct Inner {
    dir: PathBuf,
    volume_max: u64,
    volumes: BTreeMap<u32, Volume>,
    active: u32,
    next_id: u32,
}

impl Inner {
    /// Append a pre-encoded needle to the active volume, rotating first if it
    /// would overflow `volume_max`. fsyncs before returning (durability).
    fn append(
        &mut self,
        object_id: ObjectId,
        needle_bytes: &[u8],
        size: u32,
        flags: u8,
    ) -> Result<ObjectLocation> {
        let needle_len = needle_bytes.len() as u64;

        // Rotate when the active volume is non-empty and would overflow. (An
        // empty volume always accepts at least one needle, even an oversized one.)
        let active = self.active;
        let would_overflow = {
            let v = self
                .volumes
                .get(&active)
                .ok_or(Error::VolumeNotFound(active))?;
            v.write_offset > 0 && v.write_offset + needle_len > self.volume_max
        };
        if would_overflow {
            self.rotate()?;
        }

        let active = self.active;
        let v = self
            .volumes
            .get_mut(&active)
            .ok_or(Error::VolumeNotFound(active))?;
        let offset = v.write_offset;
        v.file.write_all_at(needle_bytes, offset)?;
        v.file.sync_data()?;
        v.write_offset += needle_len;

        let loc = NeedleLoc {
            offset,
            size,
            flags,
        };
        v.index.insert(object_id, loc);
        Ok(ObjectLocation::new(VolumeId(active), loc))
    }

    /// Start a new active volume.
    fn rotate(&mut self) -> Result<()> {
        let id = self.next_id;
        self.next_id += 1;
        let v = Volume::create(&self.dir, VolumeId(id))?;
        self.volumes.insert(id, v);
        self.active = id;
        Ok(())
    }
}

/// Single-node, local-filesystem storage backend.
pub struct LocalFsBackend {
    inner: Mutex<Inner>,
}

impl LocalFsBackend {
    /// Open (creating if absent) a backend rooted at `dir`.
    ///
    /// Discovers existing volume files, recovers each, and selects the
    /// highest-numbered volume as the active append target. With no volumes yet,
    /// creates volume 1.
    pub fn open(dir: impl Into<PathBuf>, config: BackendConfig) -> Result<Self> {
        let dir = dir.into();
        let vols_dir = dir.join("volumes");
        std::fs::create_dir_all(&vols_dir)?;

        let mut ids = discover_volume_ids(&vols_dir)?;
        ids.sort_unstable();

        let mut volumes = BTreeMap::new();
        for id in &ids {
            volumes.insert(*id, Volume::recover(&dir, VolumeId(*id))?);
        }

        let (active, next_id) = match ids.last() {
            Some(&max) => (max, max + 1),
            None => {
                volumes.insert(1, Volume::create(&dir, VolumeId(1))?);
                (1, 2)
            }
        };

        Ok(Self {
            inner: Mutex::new(Inner {
                dir,
                volume_max: config.volume_max,
                volumes,
                active,
                next_id,
            }),
        })
    }
}

impl StorageBackend for LocalFsBackend {
    fn put(&self, object_id: ObjectId, data: &[u8]) -> Result<ObjectLocation> {
        let needle = encode_needle(object_id, 0, data)?;
        let size = data.len() as u32;
        self.inner.lock().append(object_id, &needle, size, 0)
    }

    fn get(&self, loc: ObjectLocation, range: Option<ByteRange>) -> Result<Vec<u8>> {
        let inner = self.inner.lock();
        let vol_id = loc.volume.get();
        let v = inner
            .volumes
            .get(&vol_id)
            .ok_or(Error::VolumeNotFound(vol_id))?;

        let mut header_buf = [0u8; HEADER_LEN];
        v.file.read_exact_at(&mut header_buf, loc.needle.offset)?;
        let header = NeedleHeader::decode(&header_buf)?;
        if header.data_len != loc.needle.size {
            return Err(Error::LocationMismatch {
                volume: vol_id,
                offset: loc.needle.offset,
                detail: "header data_len disagrees with indexed size",
            });
        }

        let mut data = vec![0u8; header.data_len as usize];
        v.file
            .read_exact_at(&mut data, loc.needle.offset + HEADER_LEN as u64)?;
        verify_data(&header, &data)?;

        match range {
            None => Ok(data),
            Some(r) => {
                let end = r
                    .offset
                    .checked_add(r.length)
                    .filter(|&end| end <= data.len() as u64)
                    .ok_or(Error::BadRange {
                        offset: r.offset,
                        len: r.length,
                        size: header.data_len,
                    })?;
                Ok(data[r.offset as usize..end as usize].to_vec())
            }
        }
    }

    fn delete(&self, object_id: ObjectId) -> Result<ObjectLocation> {
        let needle = encode_needle(object_id, FLAG_TOMBSTONE, &[])?;
        self.inner
            .lock()
            .append(object_id, &needle, 0, FLAG_TOMBSTONE)
    }

    fn sync(&self) -> Result<()> {
        let inner = self.inner.lock();
        for v in inner.volumes.values() {
            v.file.sync_all()?;
        }
        Ok(())
    }

    fn checkpoint(&self) -> Result<()> {
        let inner = self.inner.lock();
        for v in inner.volumes.values() {
            let bytes = idxfile::encode(v.write_offset, &v.index);
            let final_path = idx_path(&inner.dir, v.id);
            let tmp_path = idx_tmp_path(&inner.dir, v.id);
            {
                let tmp = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&tmp_path)?;
                tmp.write_all_at(&bytes, 0)?;
                tmp.sync_all()?;
            }
            std::fs::rename(&tmp_path, &final_path)?; // atomic on the same dir
        }
        Ok(())
    }
}

fn vol_path(dir: &Path, id: VolumeId) -> PathBuf {
    dir.join("volumes").join(format!("{id}.vol"))
}

fn idx_path(dir: &Path, id: VolumeId) -> PathBuf {
    dir.join("volumes").join(format!("{id}.idx"))
}

fn idx_tmp_path(dir: &Path, id: VolumeId) -> PathBuf {
    dir.join("volumes").join(format!("{id}.idx.tmp"))
}

/// List the volume ids present under a `volumes/` directory.
fn discover_volume_ids(vols_dir: &Path) -> Result<Vec<u32>> {
    let mut ids = Vec::new();
    for entry in std::fs::read_dir(vols_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("vol") {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| Error::BadVolumeName(path.display().to_string()))?;
        let id = stem
            .parse::<u32>()
            .map_err(|_| Error::BadVolumeName(stem.to_string()))?;
        ids.push(id);
    }
    Ok(ids)
}
