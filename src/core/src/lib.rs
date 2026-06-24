//! Core types and on-disk format for Soma.
//!
//! This crate owns the lowest layer: how an object is framed on disk (the
//! *needle*), how needles are packed into *volume* files, the compact in-RAM
//! *hot index* that maps an object id to its byte location, and the shared error
//! type. It contains **no IO policy and no S3 concepts** — it operates on byte
//! slices and in-memory structures so it can be exhaustively unit- and
//! property-tested. The `soma-backend` crate layers file IO, fsync, and `.idx`
//! checkpointing on top.
//!
//! See `docs/ARCHITECTURE.md` §4 and `docs/MVP_DESIGN.md` §4 for the design.

// Tests legitimately use `unwrap`/`expect`/`panic` (asserting invariants); the
// workspace denies them in library code only.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod error;
mod id;
mod index;
mod needle;

pub use error::{Error, Result};
pub use id::{ObjectId, VolumeId};
pub use index::{HotIndex, IdxEntry, NeedleLoc, ObjectLocation, IDX_ENTRY_LEN};
pub use needle::{
    align_up, encode_needle, padded_needle_len, scan, verify_data, NeedleHeader, ScanOutcome,
    ScannedNeedle, FLAG_TOMBSTONE, FORMAT_VERSION, HEADER_LEN, MAGIC, NEEDLE_ALIGN,
};
