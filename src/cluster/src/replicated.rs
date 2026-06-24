//! N-way quorum replication across storage nodes.
//!
//! [`ReplicatedBackend`] is a `StorageBackend` the gateway uses in place of a
//! single storage node. A write fans out to the object's `replication_factor`
//! replica nodes (chosen by the consistent-hash [`Ring`]) and succeeds once
//! `write_quorum` of them durably ack; a read tries the replicas in turn and
//! returns the first success (failover). The metadata remains the authority for
//! what is committed, so replication needs no consensus (see `docs/M2_DESIGN.md`
//! §5).

use std::sync::Arc;

use soma_backend::{ByteRange, Error, Result, StorageBackend};
use soma_core::ObjectId;

use crate::ring::Ring;
use crate::StorageClient;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Number of virtual points per node on the ring.
const VNODES: usize = 64;

/// A quorum-replicated storage backend over a fixed set of storage nodes.
pub struct ReplicatedBackend {
    nodes: Vec<Arc<dyn StorageBackend>>,
    ring: Ring,
    replication_factor: usize,
    write_quorum: usize,
}

impl ReplicatedBackend {
    /// Build over already-constructed node backends.
    pub fn new(nodes: Vec<Arc<dyn StorageBackend>>, rf: usize, wq: usize) -> Self {
        let n = nodes.len();
        let ring = Ring::new(n, VNODES);
        let replication_factor = rf.min(n).max(1);
        let write_quorum = wq.min(replication_factor).max(1);
        Self {
            nodes,
            ring,
            replication_factor,
            write_quorum,
        }
    }

    /// Connect (lazily) to each storage endpoint and build the replicated backend.
    pub async fn connect(
        endpoints: Vec<String>,
        rf: usize,
        wq: usize,
    ) -> std::result::Result<Self, BoxError> {
        let mut nodes: Vec<Arc<dyn StorageBackend>> = Vec::with_capacity(endpoints.len());
        for ep in endpoints {
            nodes.push(Arc::new(StorageClient::connect(ep).await?));
        }
        if nodes.is_empty() {
            return Err("no storage endpoints configured".into());
        }
        Ok(Self::new(nodes, rf, wq))
    }

    fn replicas(&self, object_id: ObjectId) -> Vec<usize> {
        self.ring.replicas(object_id, self.replication_factor)
    }
}

impl StorageBackend for ReplicatedBackend {
    fn put(&self, object_id: ObjectId, data: &[u8]) -> Result<()> {
        let mut acks = 0;
        let mut last_err = None;
        for &node in &self.replicas(object_id) {
            match self.nodes[node].put(object_id, data) {
                Ok(()) => acks += 1,
                Err(e) => last_err = Some(e),
            }
        }
        if acks >= self.write_quorum {
            Ok(())
        } else {
            Err(last_err.unwrap_or_else(|| {
                Error::Remote(format!(
                    "write quorum not met: {acks}/{} acked",
                    self.write_quorum
                ))
            }))
        }
    }

    fn get(&self, object_id: ObjectId, range: Option<ByteRange>) -> Result<Vec<u8>> {
        let replicas = self.replicas(object_id);

        // Ranged reads: fail over to the first replica that has the bytes; no
        // repair (we don't hold the full object to rewrite).
        if range.is_some() {
            let mut last_err = None;
            for &node in &replicas {
                match self.nodes[node].get(object_id, range) {
                    Ok(data) => return Ok(data),
                    Err(e) => last_err = Some(e),
                }
            }
            return Err(last_err.unwrap_or(Error::ObjectNotFound(object_id)));
        }

        // Full reads: query every replica so read-repair can refill any that are
        // up but missing the object (e.g. a node that was down during the write).
        // (This trades read amplification for self-heal; a background/async repair
        // pass is a later optimization.)
        let mut found: Option<Vec<u8>> = None;
        let mut missing = Vec::new();
        let mut last_err = None;
        for &node in &replicas {
            match self.nodes[node].get(object_id, None) {
                Ok(data) => {
                    if found.is_none() {
                        found = Some(data);
                    }
                }
                Err(Error::ObjectNotFound(_)) => missing.push(node),
                Err(e) => last_err = Some(e),
            }
        }
        let data = found.ok_or_else(|| last_err.unwrap_or(Error::ObjectNotFound(object_id)))?;
        // Read-repair: best-effort rewrite to replicas that lacked the object.
        for &node in &missing {
            let _ = self.nodes[node].put(object_id, &data);
        }
        Ok(data)
    }

    fn delete(&self, object_id: ObjectId) -> Result<()> {
        let mut acks = 0;
        let mut last_err = None;
        for &node in &self.replicas(object_id) {
            match self.nodes[node].delete(object_id) {
                Ok(()) => acks += 1,
                Err(e) => last_err = Some(e),
            }
        }
        if acks >= self.write_quorum {
            Ok(())
        } else {
            Err(last_err.unwrap_or_else(|| Error::Remote("delete quorum not met".to_string())))
        }
    }

    fn sync(&self) -> Result<()> {
        for node in &self.nodes {
            node.sync()?;
        }
        Ok(())
    }

    fn checkpoint(&self) -> Result<()> {
        for node in &self.nodes {
            node.checkpoint()?;
        }
        Ok(())
    }
}
