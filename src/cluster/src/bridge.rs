//! Sync → async bridge.
//!
//! The `MetadataStore` / `StorageBackend` traits are synchronous (the S3 layer
//! calls them inside `spawn_blocking`). The RPC clients are async. We bridge by
//! **spawning** the future on a captured runtime handle and blocking the calling
//! thread on a `std` channel — never `block_on` from inside a runtime thread,
//! which is unsafe from `spawn_blocking` workers.

use std::future::Future;

use tokio::runtime::Handle;

/// A handle to the runtime used to drive RPC futures.
#[derive(Clone)]
pub(crate) struct Bridge {
    handle: Handle,
}

/// The bridge's runtime went away before the future completed.
pub(crate) struct BridgeClosed;

impl Bridge {
    /// Capture the current runtime handle. Must be called from within a Tokio
    /// runtime (the gateway's async startup).
    pub(crate) fn new() -> Self {
        Self {
            handle: Handle::current(),
        }
    }

    /// Run `fut` to completion on the runtime, blocking the calling (sync) thread.
    pub(crate) fn run<F, T>(&self, fut: F) -> Result<T, BridgeClosed>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.handle.spawn(async move {
            let _ = tx.send(fut.await);
        });
        rx.recv().map_err(|_| BridgeClosed)
    }
}
