//! Cluster RPC for Soma (M2a).
//!
//! Connects the stateless **gateway** to the **metadata** and **storage** roles
//! over gRPC (`tonic`). The wire carries postcard-encoded payloads (see
//! `wire.rs`); the gateway-side clients implement the existing synchronous
//! `MetadataStore` / `StorageBackend` traits, bridging to the async RPC via a
//! `handle.spawn` + std-channel hop (`bridge.rs`) so they are safe to call from
//! the S3 layer's `spawn_blocking` threads.

/// Generated gRPC code.
pub mod pb {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::pedantic,
        missing_docs
    )]
    tonic::include_proto!("soma.cluster");
}

mod bridge;
mod meta;
mod replicated;
mod ring;
mod storage;
mod wire;

pub use meta::{serve_meta, MetaClient};
pub use replicated::ReplicatedBackend;
pub use storage::{serve_storage, StorageClient};
