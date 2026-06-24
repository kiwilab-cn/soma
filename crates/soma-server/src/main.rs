//! Soma server entry point.
//!
//! In M0 this assembles the S3 protocol layer, the metadata store, and the
//! storage backend into a running node. The HTTP server and assembly land in the
//! `feat/m0-s3` / `feat/m0-integration` branches; for now this is a skeleton that
//! confirms the workspace builds and runs.

fn main() {
    let version = env!("CARGO_PKG_VERSION");
    println!("soma-server {version} — M0 skeleton (not yet serving)");
    println!("on-disk needle header is {} bytes", soma_core::HEADER_LEN);
}
