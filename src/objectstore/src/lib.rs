//! `SomaStore`: an [`object_store::ObjectStore`] that transparently adds soma's
//! **local short-circuit reads** on top of a standard S3 backend.
//!
//! It wraps an `inner` object store (an `AmazonS3` pointed at the soma gateway) and
//! delegates everything to it — except `get_range` / `get_ranges`, which first try a
//! **local** read when this process is co-located with a node holding the object:
//! resolve holders via the gateway's `?location` oracle, obtain the volume file
//! descriptor over the node's unix socket, and `mmap` the requested byte range
//! (zero-copy, shared page cache). Any miss (not co-located, no oracle, a raced id, a
//! socket hiccup, an out-of-range request) falls back to the inner store, so reads
//! always succeed if the object exists. A consumer that already uses `object_store`
//! gets locality by swapping its `AmazonS3` for `SomaStore` — no read-path rewrite.
//!
//! Integrity: the localfd CRC covers a whole needle, so a *whole-object* range read is
//! CRC-verified; *partial* ranges are not (the sub-range can't be checked against the
//! whole-needle CRC) and rely on soma's background scrub plus the consumer's own
//! format-level checks. See `docs/LOCALITY_DESIGN.md`.

use std::fmt;
use std::fs::File;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use parking_lot::Mutex;
use soma_client::{GatewayRemote, Located, Remote};
use soma_localfd::{LocalClient, LocalRead};

/// Configuration for the locality short-circuit. The remote `inner` store is built
/// separately by the caller (its credentials should match these).
#[derive(Debug, Clone)]
pub struct LocalityConfig {
    /// Gateway base URL for the `?location` oracle, e.g. `http://soma-gateway:9000`.
    pub gateway_endpoint: String,
    /// S3 access key id (for signing the `?location` request).
    pub access_key: String,
    /// S3 secret key.
    pub secret_key: String,
    /// SigV4 region.
    pub region: String,
    /// The bucket this store is scoped to (object_store is single-bucket).
    pub bucket: String,
    /// This process's host (k8s node name). Empty disables short-circuiting.
    pub my_host: String,
    /// Path to the co-located node's local-read socket. Empty disables it.
    pub local_socket_path: String,
}

/// The local short-circuit state: the `?location` locator and a reused socket.
struct LocalState {
    locator: Arc<dyn Remote>,
    bucket: String,
    my_host: String,
    socket: String,
    conn: Mutex<Option<LocalClient>>,
}

impl LocalState {
    fn enabled(&self) -> bool {
        !self.my_host.is_empty() && !self.socket.is_empty()
    }

    /// Resolve the object and confirm a holder is on this host.
    fn locate_local(&self, key: &str) -> Option<Located> {
        match self.locator.locate(&self.bucket, key) {
            Ok(Some(l)) if l.hosts.iter().any(|h| h == &self.my_host) => Some(l),
            _ => None,
        }
    }

    /// Obtain the volume descriptor for `object_id` over the reused socket.
    fn read_fd(&self, object_id: u64) -> Option<LocalRead> {
        let mut guard = self.conn.lock();
        let mut client = match guard.take() {
            Some(c) => c,
            None => LocalClient::connect(&self.socket).ok()?,
        };
        match client.read_fd(object_id) {
            Ok(r) => {
                *guard = Some(client); // keep for reuse
                Some(r)
            }
            Err(_) => None, // drop the connection; reconnect next time
        }
    }

    /// mmap a byte range of an object as zero-copy `Bytes`. `None` (→ remote fallback)
    /// on a bad range or a mapping failure.
    fn map_range(
        file: &File,
        payload_offset: u64,
        obj_len: u64,
        crc: u32,
        range: &Range<u64>,
    ) -> Option<Bytes> {
        let (start, end) = (range.start, range.end);
        if end > obj_len || start > end {
            return None;
        }
        let rlen = (end - start) as usize;
        if rlen == 0 {
            return Some(Bytes::new());
        }
        // mmap offsets must be page-aligned; a needle payload is not, so map from the
        // page boundary below the requested start and index in.
        let file_off = payload_offset + start;
        let page = page_size::get() as u64;
        let aligned = file_off & !(page - 1);
        let inner = (file_off - aligned) as usize;
        let map_len = inner + rlen;
        // Safety: the descriptor refers to an immutable, already-written needle; soma
        // does not truncate a live volume, and compaction copies to a new file (this
        // fd pins the old inode), so the mapping stays valid for its lifetime.
        let map = unsafe {
            memmap2::MmapOptions::new()
                .offset(aligned)
                .len(map_len)
                .map(file)
                .ok()?
        };
        // The localfd CRC covers the whole needle: verify only a whole-object read.
        if start == 0 && rlen as u64 == obj_len && crc32c::crc32c(&map[inner..inner + rlen]) != crc
        {
            return None;
        }
        Some(Bytes::from_owner(map).slice(inner..inner + rlen))
    }

    /// Try a single local range read.
    fn try_range(&self, key: &str, range: Range<u64>) -> Option<Bytes> {
        let loc = self.locate_local(key)?;
        let read = self.read_fd(loc.object_id)?;
        let (po, ol, cr) = (read.payload_offset, read.len as u64, read.crc);
        let file = File::from(read.fd);
        Self::map_range(&file, po, ol, cr, &range)
    }

    /// Try multiple local ranges with a single locate + descriptor. All-or-nothing:
    /// if any range can't be served locally, fall back to the inner store for all.
    fn try_ranges(&self, key: &str, ranges: &[Range<u64>]) -> Option<Vec<Bytes>> {
        let loc = self.locate_local(key)?;
        let read = self.read_fd(loc.object_id)?;
        let (po, ol, cr) = (read.payload_offset, read.len as u64, read.crc);
        let file = File::from(read.fd);
        let mut out = Vec::with_capacity(ranges.len());
        for r in ranges {
            out.push(Self::map_range(&file, po, ol, cr, r)?);
        }
        Some(out)
    }
}

/// An `object_store` backed by a remote S3 store plus soma's local short-circuit.
pub struct SomaStore {
    inner: Arc<dyn ObjectStore>,
    local: Arc<LocalState>,
}

impl SomaStore {
    /// Wrap `inner` (the remote S3 store) with the locality short-circuit.
    pub fn new(inner: Arc<dyn ObjectStore>, cfg: LocalityConfig) -> Self {
        let locator: Arc<dyn Remote> = Arc::new(GatewayRemote::new(
            cfg.gateway_endpoint,
            cfg.access_key,
            cfg.secret_key,
            cfg.region,
        ));
        Self {
            inner,
            local: Arc::new(LocalState {
                locator,
                bucket: cfg.bucket,
                my_host: cfg.my_host,
                socket: cfg.local_socket_path,
                conn: Mutex::new(None),
            }),
        }
    }

    /// Build over a custom locator (used by tests).
    pub fn with_locator(
        inner: Arc<dyn ObjectStore>,
        locator: Arc<dyn Remote>,
        bucket: String,
        my_host: String,
        local_socket_path: String,
    ) -> Self {
        Self {
            inner,
            local: Arc::new(LocalState {
                locator,
                bucket,
                my_host,
                socket: local_socket_path,
                conn: Mutex::new(None),
            }),
        }
    }
}

impl fmt::Debug for SomaStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SomaStore({:?})", self.inner)
    }
}

impl fmt::Display for SomaStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SomaStore(local-short-circuit over {})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for SomaStore {
    // --- reads: try local short-circuit, fall back to the inner store ---------

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        if self.local.enabled() {
            let local = self.local.clone();
            let key = location.to_string();
            let r = range.clone();
            if let Ok(Some(b)) = tokio::task::spawn_blocking(move || local.try_range(&key, r)).await
            {
                return Ok(b);
            }
        }
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        if self.local.enabled() {
            let local = self.local.clone();
            let key = location.to_string();
            let rs = ranges.to_vec();
            if let Ok(Some(v)) =
                tokio::task::spawn_blocking(move || local.try_ranges(&key, &rs)).await
            {
                return Ok(v);
            }
        }
        self.inner.get_ranges(location, ranges).await
    }

    // --- everything else delegates to the inner store -------------------------

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
