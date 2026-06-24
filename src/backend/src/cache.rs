//! In-memory read cache layered over a [`StorageBackend`] (M1).
//!
//! [`CachingBackend`] is a transparent decorator: writes pass straight through;
//! only `get` consults the cache. It is keyed by the **immutable**
//! [`ObjectLocation`] (a new object version is a new needle at a new location), so
//! there is no invalidation on overwrite or delete — stale entries simply age out
//! (see `docs/M1_DESIGN.md` §3). Admission is biased to small objects: anything
//! larger than `max_object_bytes` bypasses the cache.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use foyer::{Cache, CacheBuilder};
use soma_core::{ObjectId, ObjectLocation};

use crate::error::{Error, Result};
use crate::{ByteRange, StorageBackend};

/// A snapshot of cache counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// Reads served from the cache.
    pub hits: u64,
    /// Reads that missed and went to the inner backend.
    pub misses: u64,
}

/// A read-through, in-memory cache in front of an inner [`StorageBackend`].
pub struct CachingBackend {
    inner: Arc<dyn StorageBackend>,
    cache: Cache<ObjectLocation, Bytes>,
    max_object_bytes: u64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl CachingBackend {
    /// Wrap `inner` with an in-memory cache of `capacity_bytes` total, caching
    /// only objects up to `max_object_bytes`.
    pub fn new(
        inner: Arc<dyn StorageBackend>,
        capacity_bytes: usize,
        max_object_bytes: u64,
    ) -> Self {
        let cache = CacheBuilder::new(capacity_bytes)
            .with_weighter(|_k: &ObjectLocation, v: &Bytes| v.len())
            .build();
        Self {
            inner,
            cache,
            max_object_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Current hit/miss counters.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }

    fn cacheable(&self, loc: &ObjectLocation) -> bool {
        u64::from(loc.needle.size) <= self.max_object_bytes
    }
}

impl StorageBackend for CachingBackend {
    fn put(&self, object_id: ObjectId, data: &[u8]) -> Result<ObjectLocation> {
        self.inner.put(object_id, data)
    }

    fn get(&self, loc: ObjectLocation, range: Option<ByteRange>) -> Result<Vec<u8>> {
        // Large objects bypass the cache entirely.
        if !self.cacheable(&loc) {
            return self.inner.get(loc, range);
        }

        // Small object: get (or warm) the full bytes, then slice any range from
        // them. A ranged read of a small object thus caches the whole object.
        let full = match self.cache.get(&loc) {
            Some(entry) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                entry.value().clone()
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                let bytes = Bytes::from(self.inner.get(loc, None)?);
                self.cache.insert(loc, bytes.clone());
                bytes
            }
        };

        match range {
            None => Ok(full.to_vec()),
            Some(r) => slice_range(&full, r),
        }
    }

    fn delete(&self, object_id: ObjectId) -> Result<ObjectLocation> {
        self.inner.delete(object_id)
    }

    fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    fn checkpoint(&self) -> Result<()> {
        self.inner.checkpoint()
    }
}

/// Slice `[offset, offset+length)` out of `full`, bounds-checked.
fn slice_range(full: &Bytes, r: ByteRange) -> Result<Vec<u8>> {
    let end = r
        .offset
        .checked_add(r.length)
        .filter(|&e| e <= full.len() as u64)
        .ok_or(Error::BadRange {
            offset: r.offset,
            len: r.length,
            size: full.len() as u32,
        })?;
    Ok(full[r.offset as usize..end as usize].to_vec())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use parking_lot::Mutex;
    use soma_core::{NeedleLoc, VolumeId};
    use std::collections::HashMap;

    /// An in-memory backend that counts `get` calls, for asserting cache behavior.
    struct CountingBackend {
        store: Mutex<HashMap<ObjectLocation, Vec<u8>>>,
        next_offset: AtomicU64,
        gets: AtomicU64,
    }

    impl CountingBackend {
        fn new() -> Self {
            Self {
                store: Mutex::new(HashMap::new()),
                next_offset: AtomicU64::new(0),
                gets: AtomicU64::new(0),
            }
        }
        fn get_count(&self) -> u64 {
            self.gets.load(Ordering::Relaxed)
        }
    }

    impl StorageBackend for CountingBackend {
        fn put(&self, _object_id: ObjectId, data: &[u8]) -> Result<ObjectLocation> {
            let offset = self.next_offset.fetch_add(1, Ordering::Relaxed);
            let loc = ObjectLocation::new(
                VolumeId(1),
                NeedleLoc {
                    offset,
                    size: data.len() as u32,
                    flags: 0,
                },
            );
            self.store.lock().insert(loc, data.to_vec());
            Ok(loc)
        }
        fn get(&self, loc: ObjectLocation, range: Option<ByteRange>) -> Result<Vec<u8>> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            let data = self
                .store
                .lock()
                .get(&loc)
                .cloned()
                .ok_or(Error::VolumeNotFound(loc.volume.get()))?;
            match range {
                None => Ok(data),
                Some(r) => slice_range(&Bytes::from(data), r),
            }
        }
        fn delete(&self, _object_id: ObjectId) -> Result<ObjectLocation> {
            unimplemented!()
        }
        fn sync(&self) -> Result<()> {
            Ok(())
        }
        fn checkpoint(&self) -> Result<()> {
            Ok(())
        }
    }

    fn setup(max_object_bytes: u64) -> (Arc<CountingBackend>, CachingBackend) {
        let inner = Arc::new(CountingBackend::new());
        let caching = CachingBackend::new(inner.clone(), 16 * 1024 * 1024, max_object_bytes);
        (inner, caching)
    }

    #[test]
    fn second_read_is_a_hit() {
        let (inner, cache) = setup(1024);
        let loc = cache.put(1, b"hello").unwrap();

        assert_eq!(cache.get(loc, None).unwrap(), b"hello"); // miss
        assert_eq!(cache.get(loc, None).unwrap(), b"hello"); // hit
        assert_eq!(cache.get(loc, None).unwrap(), b"hello"); // hit

        assert_eq!(inner.get_count(), 1); // inner hit only once
        assert_eq!(cache.stats(), CacheStats { hits: 2, misses: 1 });
    }

    #[test]
    fn large_object_bypasses_cache() {
        let (inner, cache) = setup(4); // tiny threshold
        let loc = cache.put(1, b"too large to cache").unwrap();

        cache.get(loc, None).unwrap();
        cache.get(loc, None).unwrap();

        assert_eq!(inner.get_count(), 2); // every read hits the inner backend
        assert_eq!(cache.stats(), CacheStats { hits: 0, misses: 0 });
    }

    #[test]
    fn range_served_from_cached_small_object() {
        let (inner, cache) = setup(1024);
        let loc = cache.put(1, b"0123456789").unwrap();

        // Full read warms the cache.
        cache.get(loc, None).unwrap();
        // Range read is served from the cached bytes (no extra inner get).
        let part = cache
            .get(
                loc,
                Some(ByteRange {
                    offset: 2,
                    length: 4,
                }),
            )
            .unwrap();
        assert_eq!(part, b"2345");
        assert_eq!(inner.get_count(), 1);
    }

    #[test]
    fn range_miss_warms_full_object() {
        let (inner, cache) = setup(1024);
        let loc = cache.put(1, b"0123456789").unwrap();

        // First access is a ranged miss: fetches the full small object + caches.
        let part = cache
            .get(
                loc,
                Some(ByteRange {
                    offset: 0,
                    length: 3,
                }),
            )
            .unwrap();
        assert_eq!(part, b"012");
        // Subsequent full read is a hit.
        assert_eq!(cache.get(loc, None).unwrap(), b"0123456789");
        assert_eq!(inner.get_count(), 1);
    }
}
