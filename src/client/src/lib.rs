//! `soma-client`: a reader that transparently **short-circuits to local storage**
//! when the calling process is co-located with a node holding the object, and
//! **falls back to the gateway** otherwise (see `docs/LOCALITY_DESIGN.md`).
//!
//! For a co-located reader, `get` resolves the object's holders via the gateway's
//! `?location` oracle, and — if one of them is on this host — reads the bytes
//! through a passed file descriptor over the node's local socket (no gateway, no
//! network). Any miss (not co-located, no oracle, a local race, a socket hiccup)
//! falls back to a normal signed S3 GET, so reads always succeed if the object
//! exists. The fallback makes the client a drop-in S3 reader even off-cluster.

mod gateway;
mod sigv4;

pub use gateway::GatewayRemote;

use std::ops::Deref;
use std::os::unix::fs::FileExt;

use memmap2::Mmap;
use parking_lot::Mutex;
use soma_localfd::LocalClient;

/// Default size at/above which a local read uses `mmap` (zero-copy) instead of a
/// `pread` copy. Below it the mmap setup/teardown costs more than the copy.
const DEFAULT_MMAP_THRESHOLD: usize = 64 * 1024;

/// An object's bytes. Backed by an `mmap` of the storage node's volume (zero-copy,
/// shared page cache) on the local large-object path, or by an owned buffer for
/// small local reads and the gateway fallback. Derefs to the payload `&[u8]`.
pub struct ObjectBytes(Repr);

enum Repr {
    Owned(Vec<u8>),
    Mapped { map: Mmap, start: usize, len: usize },
}

impl ObjectBytes {
    fn owned(v: Vec<u8>) -> Self {
        Self(Repr::Owned(v))
    }
    fn mapped(map: Mmap, start: usize, len: usize) -> Self {
        Self(Repr::Mapped { map, start, len })
    }
    /// The payload as a slice.
    pub fn as_slice(&self) -> &[u8] {
        self
    }
    /// Payload length in bytes.
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }
    /// Whether the payload is empty.
    pub fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }
    /// Copy the payload into an owned buffer.
    pub fn to_vec(&self) -> Vec<u8> {
        self.as_slice().to_vec()
    }
}

impl Deref for ObjectBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match &self.0 {
            Repr::Owned(v) => v.as_slice(),
            Repr::Mapped { map, start, len } => &map[*start..*start + *len],
        }
    }
}

impl AsRef<[u8]> for ObjectBytes {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

/// Errors from a soma-client read.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Local descriptor IO failed.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// The local short-circuit path failed (the caller normally falls back).
    #[error("local read: {0}")]
    Local(#[from] soma_localfd::Error),

    /// The payload read locally did not match its CRC (bitrot).
    #[error("payload failed CRC verification")]
    Corrupt,

    /// The gateway returned an error or was unreachable.
    #[error("gateway: {0}")]
    Gateway(String),

    /// The object does not exist.
    #[error("object not found")]
    NotFound,
}

/// Convenience result type.
pub type Result<T> = std::result::Result<T, Error>;

/// What the client needs to know about an object's placement: its id, size, and
/// the hosts of the nodes holding it.
#[derive(Debug, Clone)]
pub struct Located {
    /// Internal object id (used to read over the local socket).
    pub object_id: u64,
    /// Object size in bytes.
    pub size: u64,
    /// Hosts of the nodes holding the object (matched against the reader's host).
    pub hosts: Vec<String>,
}

/// The gateway-facing capability: resolve an object's placement, and fetch its
/// bytes. Abstracted so the short-circuit logic can be tested without a gateway.
pub trait Remote: Send + Sync {
    /// Resolve where an object lives (best-effort: `Ok(None)` means "no locality
    /// info", e.g. a single-node gateway, so the caller reads remotely).
    fn locate(&self, bucket: &str, key: &str) -> Result<Option<Located>>;

    /// Fetch an object's full bytes over the gateway (a signed S3 GET).
    fn get(&self, bucket: &str, key: &str) -> Result<Vec<u8>>;
}

/// Configuration for a turnkey client over a gateway.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Gateway base URL, e.g. `http://soma-gateway:9000`.
    pub gateway_endpoint: String,
    /// S3 access key id.
    pub access_key: String,
    /// S3 secret key.
    pub secret_key: String,
    /// SigV4 region (any value the gateway accepts; it does not pin one).
    pub region: String,
    /// This process's host (k8s node name). Empty disables short-circuiting.
    pub my_host: String,
    /// Path to the co-located node's local-read socket (a shared `hostPath`).
    /// Empty disables short-circuiting.
    pub local_socket_path: String,
    /// Local reads at/above this size use `mmap` (zero-copy); smaller reads copy via
    /// `pread`. Defaults to 64 KiB.
    pub mmap_threshold: usize,
    /// Verify the CRC of locally-read payloads (bitrot guard). Defaults to `true`;
    /// set `false` to skip the verification pass on the hottest scan paths.
    pub verify_local_crc: bool,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            gateway_endpoint: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            region: "us-east-1".to_string(),
            my_host: String::new(),
            local_socket_path: String::new(),
            mmap_threshold: DEFAULT_MMAP_THRESHOLD,
            verify_local_crc: true,
        }
    }
}

/// A transparent locality-aware reader.
pub struct SomaClient {
    remote: Box<dyn Remote>,
    my_host: String,
    local_socket_path: String,
    mmap_threshold: usize,
    verify_local_crc: bool,
    /// A reused local-socket connection (lazily opened, dropped on error). Reads
    /// are serialized through it; open more clients for concurrent local reads.
    local: Mutex<Option<LocalClient>>,
}

impl SomaClient {
    /// Build a turnkey client that talks to a gateway.
    pub fn new(cfg: ClientConfig) -> Self {
        let remote = Box::new(GatewayRemote::new(
            cfg.gateway_endpoint,
            cfg.access_key,
            cfg.secret_key,
            cfg.region,
        ));
        Self {
            remote,
            my_host: cfg.my_host,
            local_socket_path: cfg.local_socket_path,
            mmap_threshold: cfg.mmap_threshold,
            verify_local_crc: cfg.verify_local_crc,
            local: Mutex::new(None),
        }
    }

    /// Build over a custom [`Remote`] (used by tests and alternative front-ends),
    /// with default local-read tuning.
    pub fn with_remote(
        remote: Box<dyn Remote>,
        my_host: String,
        local_socket_path: String,
    ) -> Self {
        Self {
            remote,
            my_host,
            local_socket_path,
            mmap_threshold: DEFAULT_MMAP_THRESHOLD,
            verify_local_crc: true,
            local: Mutex::new(None),
        }
    }

    /// Read an object's full bytes, short-circuiting to local storage when this
    /// process is co-located with a holder, else via the gateway. The result derefs
    /// to `&[u8]` and is zero-copy on the local large-object path.
    pub fn get(&self, bucket: &str, key: &str) -> Result<ObjectBytes> {
        if self.local_enabled() {
            match self.remote.locate(bucket, key) {
                Ok(Some(loc)) if loc.hosts.iter().any(|h| h == &self.my_host) => {
                    match self.read_local(loc.object_id) {
                        Ok(bytes) => return Ok(bytes),
                        Err(e) => tracing::debug!(
                            error = %e,
                            object_id = loc.object_id,
                            "local short-circuit failed; falling back to gateway"
                        ),
                    }
                }
                Ok(_) => {} // not co-located, or no locality info → read remotely
                Err(e) => tracing::debug!(error = %e, "locate failed; falling back to gateway"),
            }
        }
        self.remote.get(bucket, key).map(ObjectBytes::owned)
    }

    /// Whether short-circuiting is possible at all (host + socket configured).
    fn local_enabled(&self) -> bool {
        !self.my_host.is_empty() && !self.local_socket_path.is_empty()
    }

    /// Read an object's payload locally via a passed descriptor. Large payloads are
    /// `mmap`ed (zero-copy); small ones are `pread` into a buffer. CRC-verified
    /// unless disabled.
    fn read_local(&self, object_id: u64) -> Result<ObjectBytes> {
        let read = {
            let mut guard = self.local.lock();
            let mut client = match guard.take() {
                Some(c) => c,
                None => LocalClient::connect(&self.local_socket_path)?,
            };
            match client.read_fd(object_id) {
                Ok(read) => {
                    *guard = Some(client); // keep the connection for reuse
                    read
                }
                // Drop the (possibly broken) connection; the next call reconnects.
                Err(e) => return Err(e.into()),
            }
        };

        // The fd references the same kernel open file as the storage node's volume;
        // the bytes never crossed the socket.
        let file = std::fs::File::from(read.fd);
        let len = read.len as usize;

        if len < self.mmap_threshold {
            // Small object: a single pread copy beats mmap setup/teardown.
            let mut buf = vec![0u8; len];
            file.read_exact_at(&mut buf, read.payload_offset)?;
            if self.verify_local_crc && crc32c::crc32c(&buf) != read.crc {
                return Err(Error::Corrupt);
            }
            return Ok(ObjectBytes::owned(buf));
        }

        // Large object: mmap the payload region (zero-copy, shared page cache). mmap
        // offsets must be page-aligned but a needle payload is not, so map from the
        // page boundary below it and index in.
        let page = page_size::get() as u64;
        let aligned = read.payload_offset & !(page - 1);
        let inner = (read.payload_offset - aligned) as usize;
        let map_len = inner + len;
        // Safety: the descriptor refers to an immutable, already-written needle; soma
        // does not truncate a volume during serving, and compaction copies to a new
        // file (this fd pins the old inode), so the mapping stays valid for its life.
        let map = unsafe {
            memmap2::MmapOptions::new()
                .offset(aligned)
                .len(map_len)
                .map(&file)?
        };
        if self.verify_local_crc && crc32c::crc32c(&map[inner..inner + len]) != read.crc {
            return Err(Error::Corrupt);
        }
        Ok(ObjectBytes::mapped(map, inner, len))
    }
}
