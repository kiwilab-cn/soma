//! Local short-circuit reads: a node-local unix-domain socket over which a
//! co-located reader obtains a storage node's bytes as a **file descriptor**
//! (`SCM_RIGHTS`), bypassing the gateway and the network entirely.
//!
//! This is the data path for soma's data-locality reads (see
//! `docs/LOCALITY_DESIGN.md`): the gateway's `?location` oracle tells a scheduler
//! which node holds an object; a task placed on that node then reads the object
//! here, locally, with no copy through the gateway.
//!
//! Trust model: the descriptor handed out is for the whole **volume** file (soma
//! packs many objects per volume), so it grants read access to that volume's other
//! objects too. The socket is therefore for compute inside soma's trust domain
//! (same tenant). Authorization of *which* object to read still belongs upstream
//! (the metadata/gateway layer that resolved the id).
//!
//! Requires a **shared-kernel** container runtime (runc); VM-isolated runtimes
//! (Kata, gVisor) cannot receive a host descriptor and must use the gateway path.

mod client;
mod protocol;
mod server;

pub use client::{LocalClient, LocalRead};
pub use protocol::{LocalReply, LocalRequest};
pub use server::{serve_local_reads, LocalServer};

/// Errors from the local-read client/server.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Socket / descriptor IO failed.
    #[error("local-read io: {0}")]
    Io(#[from] std::io::Error),

    /// A message could not be encoded/decoded.
    #[error("local-read protocol: {0}")]
    Protocol(#[from] postcard::Error),

    /// The peer closed the connection mid-message.
    #[error("local-read connection closed")]
    Closed,

    /// The reply was malformed (e.g. an `Ok` with no descriptor attached).
    #[error("local-read malformed reply: {0}")]
    Malformed(&'static str),

    /// The node reported it could not serve the read.
    #[error("local-read remote error: {0}")]
    Remote(String),

    /// The object does not exist on the node.
    #[error("local-read object not found")]
    NotFound,
}

/// Convenience result type.
pub type Result<T> = std::result::Result<T, Error>;
