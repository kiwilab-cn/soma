//! Metadata store for Soma — the single authority mapping `(bucket, key)` to a
//! needle location and version, with conditional-write (CAS) semantics.
//!
//! The `MetadataStore` trait and its `redb` implementation land in the
//! `feat/m0-meta` branch (see `docs/MVP_DESIGN.md` §6.1). This crate is currently
//! a placeholder that establishes the workspace boundary.
