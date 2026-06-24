//! In-memory read cache layered over a [`StorageBackend`] (M1).
//!
//! [`CachingBackend`] is a transparent decorator: writes pass straight through;
//! only `get` consults the cache. It is keyed by the **immutable** object id (a
//! new object version gets a new id), so there is no invalidation on overwrite —
//! stale entries simply age out (delete also drops the entry). Admission is
//! biased to small objects: full objects larger than `max_object_bytes` are not
//! cached.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use foyer::{Cache, CacheBuilder};
use soma_core::ObjectId;

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
    cache: Cache<ObjectId, Bytes>,
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
            .with_weighter(|_k: &ObjectId, v: &Bytes| v.len())
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
}

impl StorageBackend for CachingBackend {
    fn put(&self, object_id: ObjectId, data: &[u8]) -> Result<()> {
        self.inner.put(object_id, data)
    }

    fn get(&self, object_id: ObjectId, range: Option<ByteRange>) -> Result<Vec<u8>> {
        // Hit: serve full bytes (slicing any range from them).
        if let Some(entry) = self.cache.get(&object_id) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            metrics::counter!("soma_cache_hits_total").increment(1);
            let full = entry.value().clone();
            return match range {
                None => Ok(full.to_vec()),
                Some(r) => slice_range(&full, r),
            };
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("soma_cache_misses_total").increment(1);
        match range {
            // Full miss: fetch the whole object and cache it if it is small.
            None => {
                let data = self.inner.get(object_id, None)?;
                if data.len() as u64 <= self.max_object_bytes {
                    self.cache.insert(object_id, Bytes::from(data.clone()));
                }
                Ok(data)
            }
            // Ranged miss: fetch only the range; don't cache partials.
            Some(r) => self.inner.get(object_id, Some(r)),
        }
    }

    fn delete(&self, object_id: ObjectId) -> Result<()> {
        self.cache.remove(&object_id);
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
    use std::collections::HashMap;

    /// An in-memory backend that counts `get` calls, for asserting cache behavior.
    struct CountingBackend {
        store: Mutex<HashMap<ObjectId, Vec<u8>>>,
        gets: AtomicU64,
    }

    impl CountingBackend {
        fn new() -> Self {
            Self {
                store: Mutex::new(HashMap::new()),
                gets: AtomicU64::new(0),
            }
        }
        fn get_count(&self) -> u64 {
            self.gets.load(Ordering::Relaxed)
        }
    }

    impl StorageBackend for CountingBackend {
        fn put(&self, object_id: ObjectId, data: &[u8]) -> Result<()> {
            self.store.lock().insert(object_id, data.to_vec());
            Ok(())
        }
        fn get(&self, object_id: ObjectId, range: Option<ByteRange>) -> Result<Vec<u8>> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            let data = self
                .store
                .lock()
                .get(&object_id)
                .cloned()
                .ok_or(Error::ObjectNotFound(object_id))?;
            match range {
                None => Ok(data),
                Some(r) => slice_range(&Bytes::from(data), r),
            }
        }
        fn delete(&self, object_id: ObjectId) -> Result<()> {
            self.store.lock().remove(&object_id);
            Ok(())
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
        cache.put(1, b"hello").unwrap();

        assert_eq!(cache.get(1, None).unwrap(), b"hello"); // miss
        assert_eq!(cache.get(1, None).unwrap(), b"hello"); // hit
        assert_eq!(cache.get(1, None).unwrap(), b"hello"); // hit

        assert_eq!(inner.get_count(), 1); // inner hit only once
        assert_eq!(cache.stats(), CacheStats { hits: 2, misses: 1 });
    }

    #[test]
    fn large_object_is_not_cached() {
        let (inner, cache) = setup(4); // tiny threshold
        cache.put(1, b"too large to cache").unwrap();

        cache.get(1, None).unwrap();
        cache.get(1, None).unwrap();

        // Every read misses and hits the inner backend (never cached).
        assert_eq!(inner.get_count(), 2);
        assert_eq!(cache.stats(), CacheStats { hits: 0, misses: 2 });
    }

    #[test]
    fn range_served_from_cached_small_object() {
        let (inner, cache) = setup(1024);
        cache.put(1, b"0123456789").unwrap();

        cache.get(1, None).unwrap(); // full read warms the cache
        let part = cache
            .get(
                1,
                Some(ByteRange {
                    offset: 2,
                    length: 4,
                }),
            )
            .unwrap();
        assert_eq!(part, b"2345");
        assert_eq!(inner.get_count(), 1); // range served from cache
    }

    #[test]
    fn ranged_miss_is_not_cached() {
        let (inner, cache) = setup(1024);
        cache.put(1, b"0123456789").unwrap();

        // Ranged read on a cold object fetches just the range, no caching.
        let part = cache
            .get(
                1,
                Some(ByteRange {
                    offset: 0,
                    length: 3,
                }),
            )
            .unwrap();
        assert_eq!(part, b"012");
        // A following full read still misses (and now caches).
        assert_eq!(cache.get(1, None).unwrap(), b"0123456789");
        assert_eq!(cache.get(1, None).unwrap(), b"0123456789"); // hit
        assert_eq!(inner.get_count(), 2); // ranged miss + full miss
    }

    #[test]
    fn delete_evicts_cache() {
        let (inner, cache) = setup(1024);
        cache.put(1, b"data").unwrap();
        cache.get(1, None).unwrap(); // cache it
        cache.delete(1).unwrap(); // evict + delete in inner
        assert!(cache.get(1, None).is_err()); // gone
        assert_eq!(inner.get_count(), 2);
    }
}
