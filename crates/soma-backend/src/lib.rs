//! Storage backend for Soma — durable needle IO over append-only volume files,
//! write aggregation, `.idx` checkpointing, and crash-safe rebuild.
//!
//! The `StorageBackend` trait and `LocalFsBackend` land in the `feat/m0-backend`
//! branch (see `docs/MVP_DESIGN.md` §6.2). This crate is currently a placeholder
//! that establishes the workspace boundary, building on `soma-core`'s needle
//! codec and hot index.
