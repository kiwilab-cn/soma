//! N-way quorum replication across storage nodes.
//!
//! [`ReplicatedBackend`] is a `StorageBackend` the gateway uses in place of a
//! single storage node. A write fans out to the object's replica nodes (resolved
//! through the placement-group [`Placement`]) and succeeds once `write_quorum` of
//! them durably ack; a read tries the replicas in turn and returns the first
//! success (failover), repairing any that are up but missing the object. The
//! metadata remains the authority for what is committed, so replication needs no
//! consensus (see `docs/M2_DESIGN.md` §5).

use std::collections::HashMap;
use std::sync::Arc;

use soma_backend::{ByteRange, Error, Result, StorageBackend};
use soma_core::ObjectId;

use crate::placement::{Placement, DEFAULT_PG_COUNT};
use crate::StorageClient;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A quorum-replicated storage backend over a placement-group map.
pub struct ReplicatedBackend {
    placement: Placement,
    write_quorum: usize,
}

impl ReplicatedBackend {
    /// Build over a resolved [`Placement`] (the gateway path). `write_quorum` is
    /// clamped to `[1, node_count]`.
    pub fn from_placement(placement: Placement, write_quorum: usize) -> Self {
        let write_quorum = write_quorum.clamp(1, placement.node_count().max(1));
        Self {
            placement,
            write_quorum,
        }
    }

    /// Build over an explicit list of node backends (node id = list index). Used by
    /// tests and any no-metadata path. `rf` is the replica width.
    pub fn new(nodes: Vec<Arc<dyn StorageBackend>>, rf: usize, wq: usize) -> Self {
        let clients: HashMap<String, Arc<dyn StorageBackend>> = nodes
            .into_iter()
            .enumerate()
            .map(|(i, n)| (i.to_string(), n))
            .collect();
        let width = rf.clamp(1, clients.len().max(1));
        Self::from_placement(Placement::local(clients, width, DEFAULT_PG_COUNT), wq)
    }

    /// Connect (lazily) to each storage endpoint and build the replicated backend
    /// (node id = endpoint). Used by tests / the no-metadata path.
    pub async fn connect(
        endpoints: Vec<String>,
        rf: usize,
        wq: usize,
    ) -> std::result::Result<Self, BoxError> {
        if endpoints.is_empty() {
            return Err("no storage endpoints configured".into());
        }
        let mut clients: HashMap<String, Arc<dyn StorageBackend>> = HashMap::new();
        for ep in endpoints {
            let client = StorageClient::connect(ep.clone()).await?;
            clients.insert(ep, Arc::new(client));
        }
        let width = rf.clamp(1, clients.len());
        Ok(Self::from_placement(
            Placement::local(clients, width, DEFAULT_PG_COUNT),
            wq,
        ))
    }
}

impl StorageBackend for ReplicatedBackend {
    fn put(&self, object_id: ObjectId, data: &[u8]) -> Result<()> {
        let mut acks = 0;
        let mut last_err = None;
        for node in self.placement.write_nodes(object_id) {
            match node.put(object_id, data) {
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
        let replicas = self.placement.read_nodes(object_id);

        // Ranged reads: fail over to the first replica that has the bytes; no
        // repair (we don't hold the full object to rewrite).
        if range.is_some() {
            let mut last_err = None;
            for node in &replicas {
                match node.get(object_id, range) {
                    Ok(data) => return Ok(data),
                    Err(e) => last_err = Some(e),
                }
            }
            return Err(last_err.unwrap_or(Error::ObjectNotFound(object_id)));
        }

        // Full reads: query every replica so read-repair can refill any that are
        // up but missing the object (e.g. a node that was down during the write).
        let mut found: Option<Vec<u8>> = None;
        let mut missing = Vec::new();
        let mut last_err = None;
        for node in &replicas {
            match node.get(object_id, None) {
                Ok(data) => {
                    if found.is_none() {
                        found = Some(data);
                    }
                }
                Err(Error::ObjectNotFound(_)) => missing.push(node.clone()),
                Err(e) => last_err = Some(e),
            }
        }
        let data = found.ok_or_else(|| last_err.unwrap_or(Error::ObjectNotFound(object_id)))?;
        // Read-repair: best-effort rewrite to replicas that lacked the object.
        for node in &missing {
            let _ = node.put(object_id, &data);
        }
        Ok(data)
    }

    fn delete(&self, object_id: ObjectId) -> Result<()> {
        let mut acks = 0;
        let mut last_err = None;
        for node in self.placement.write_nodes(object_id) {
            match node.delete(object_id) {
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
        for node in self.placement.all_nodes() {
            node.sync()?;
        }
        Ok(())
    }

    fn checkpoint(&self) -> Result<()> {
        for node in self.placement.all_nodes() {
            node.checkpoint()?;
        }
        Ok(())
    }
}
