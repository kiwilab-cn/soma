//! Storage backend for Soma — durable needle IO over append-only volume files,
//! with `.idx` checkpointing and crash-safe recovery.
//!
//! This crate turns the pure on-disk format from `soma-core` into real,
//! fsync-durable storage. [`LocalFsBackend`] is the M0 single-node
//! implementation; later milestones add replicated and erasure-coded backends
//! behind the same [`StorageBackend`] trait (see `docs/ARCHITECTURE.md` §7).
//!
//! **Durability contract.** [`StorageBackend::put`] appends a needle and fsyncs
//! it before returning, so the bytes are durable by the time the caller commits
//! the object's location to the metadata store. This is the ordering the write
//! protocol relies on (`docs/MVP_DESIGN.md` §7): durable bytes first, metadata
//! commit second.
//!
//! The trait is **synchronous** by design. Like the metadata engine it pairs
//! with, the storage layer does blocking, fsync-bound IO; the async edge (the S3
//! server) bridges to it via `spawn_blocking`. Keeping it sync makes the core
//! exhaustively testable without a runtime and avoids surprising async-in-disk-IO
//! footguns.

mod cache;
mod error;
mod idxfile;
mod local;

pub use cache::{CacheStats, CachingBackend};
pub use error::{Error, Result};
pub use local::{BackendConfig, LocalFsBackend};

use soma_core::{ObjectId, ObjectLocation};

/// A byte range within an object, used for S3 `Range` reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    /// Start offset within the object payload.
    pub offset: u64,
    /// Number of bytes to read from `offset`.
    pub length: u64,
}

/// A durable object-bytes store.
///
/// Implementations own how bytes are laid out and made durable; they do **not**
/// own the authority for "which object name maps to which location" — that is the
/// metadata store's job. A backend deals only in [`ObjectId`] and
/// [`ObjectLocation`].
pub trait StorageBackend: Send + Sync {
    /// Append `data` as a needle, fsync it, and return its durable location.
    fn put(&self, object_id: ObjectId, data: &[u8]) -> Result<ObjectLocation>;

    /// Read an object's bytes, optionally just a byte range, verifying the
    /// payload CRC (bitrot guard).
    fn get(&self, loc: ObjectLocation, range: Option<ByteRange>) -> Result<Vec<u8>>;

    /// Append a tombstone (delete marker) for `object_id` and return its
    /// location. Physical space is reclaimed later by compaction.
    fn delete(&self, object_id: ObjectId) -> Result<ObjectLocation>;

    /// Flush all volumes to disk (`fsync`).
    fn sync(&self) -> Result<()>;

    /// Write a `.idx` checkpoint for every volume so recovery can skip a full
    /// scan. Purely an accelerator — never required for correctness.
    fn checkpoint(&self) -> Result<()>;
}
