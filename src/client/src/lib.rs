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

use std::os::unix::fs::FileExt;

use parking_lot::Mutex;
use soma_localfd::LocalClient;

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
        }
    }
}

/// A transparent locality-aware reader.
pub struct SomaClient {
    remote: Box<dyn Remote>,
    my_host: String,
    local_socket_path: String,
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
        Self::with_remote(remote, cfg.my_host, cfg.local_socket_path)
    }

    /// Build over a custom [`Remote`] (used by tests and alternative front-ends).
    pub fn with_remote(
        remote: Box<dyn Remote>,
        my_host: String,
        local_socket_path: String,
    ) -> Self {
        Self {
            remote,
            my_host,
            local_socket_path,
            local: Mutex::new(None),
        }
    }

    /// Read an object's full bytes, short-circuiting to local storage when this
    /// process is co-located with a holder, else via the gateway.
    pub fn get(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
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
        self.remote.get(bucket, key)
    }

    /// Whether short-circuiting is possible at all (host + socket configured).
    fn local_enabled(&self) -> bool {
        !self.my_host.is_empty() && !self.local_socket_path.is_empty()
    }

    /// Read an object's payload locally via a passed descriptor, verifying its CRC.
    fn read_local(&self, object_id: u64) -> Result<Vec<u8>> {
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
        // read the payload straight from it — no bytes crossed the socket.
        let file = std::fs::File::from(read.fd);
        let mut buf = vec![0u8; read.len as usize];
        file.read_exact_at(&mut buf, read.payload_offset)?;
        if crc32c::crc32c(&buf) != read.crc {
            return Err(Error::Corrupt);
        }
        Ok(buf)
    }
}
