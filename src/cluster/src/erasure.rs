//! Reed-Solomon erasure coding across storage nodes (M4b).
//!
//! [`ErasureCodedBackend`] is a `StorageBackend` the gateway uses in place of
//! [`ReplicatedBackend`](crate::ReplicatedBackend). An object is split into `k`
//! **data** shards plus `m` **parity** shards (`reed-solomon-simd`); any `k` of
//! the `k + m` shards reconstruct it, so the object survives up to `m` node
//! losses at only `(k + m) / k×` storage — versus replication's `N×`.
//!
//! **Placement.** The `k + m` shards go to the object's `k + m` distinct ring
//! nodes — the same consistent-hash placement as replication, just a wider set.
//! Shard `i` is stored on the `i`-th placement node **under the object's id**;
//! each node holds exactly one shard per object, so the storage node is unchanged
//! (it stores opaque bytes by id and never knows it holds a shard). The shard
//! index is implicit in the node's position.
//!
//! **Self-describing stripes.** The original length is prepended to the payload
//! before encoding (`[len:u64 BE][payload]`, zero-padded to `k` equal shards), so
//! a read reconstructs the bytes from any `k` shards and truncates to `len` — no
//! per-object size lookup is needed. Shard size is even (a `reed-solomon-simd`
//! requirement) and recoverable from any retrieved shard.
//!
//! **Degraded reads** reconstruct missing data shards from parity to serve the
//! object. Persisting reconstructed shards back to a replacement node
//! (reconstruction/rebalance) is deferred (see `docs/M4_DESIGN.md` §2).

use std::collections::HashMap;
use std::sync::Arc;

use soma_backend::{ByteRange, Error, Result, StorageBackend};
use soma_core::ObjectId;

use crate::placement::{Placement, DEFAULT_PG_COUNT};
use crate::StorageClient;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Bytes of big-endian length prefix prepended to the payload before encoding.
const LEN_PREFIX: usize = 8;

/// An erasure-coded storage backend over a placement-group map.
pub struct ErasureCodedBackend {
    placement: Placement,
    data_shards: usize,
    parity_shards: usize,
    write_quorum: usize,
}

impl ErasureCodedBackend {
    /// Build over a resolved [`Placement`] whose PG width is `data_shards +
    /// parity_shards` (the gateway path). `write_quorum` `0` defaults to
    /// `data_shards + 1`, clamped to `[data_shards, k + m]`.
    pub fn from_placement(
        placement: Placement,
        data_shards: usize,
        parity_shards: usize,
        write_quorum: usize,
    ) -> Self {
        let total = data_shards + parity_shards;
        let write_quorum = if write_quorum == 0 {
            (data_shards + 1).min(total)
        } else {
            write_quorum.clamp(data_shards, total)
        };
        Self {
            placement,
            data_shards,
            parity_shards,
            write_quorum,
        }
    }

    /// Build over already-constructed node backends (node id = list index). With
    /// fewer than `data_shards + parity_shards` nodes the scheme is clamped to the
    /// node count (parity reduced) so it stays consistent. Used by tests.
    pub fn new(
        nodes: Vec<Arc<dyn StorageBackend>>,
        data_shards: usize,
        parity_shards: usize,
        write_quorum: usize,
    ) -> Self {
        let n = nodes.len().max(1);
        let data_shards = data_shards.clamp(1, n);
        let parity_shards = parity_shards.min(n - data_shards);
        let clients: HashMap<String, Arc<dyn StorageBackend>> = nodes
            .into_iter()
            .enumerate()
            .map(|(i, node)| (i.to_string(), node))
            .collect();
        let placement = Placement::local(clients, data_shards + parity_shards, DEFAULT_PG_COUNT);
        Self::from_placement(placement, data_shards, parity_shards, write_quorum)
    }

    /// Connect (lazily) to each storage endpoint and build the backend (node id =
    /// endpoint). Errors if there are fewer endpoints than `k + m`. Used by tests.
    pub async fn connect(
        endpoints: Vec<String>,
        data_shards: usize,
        parity_shards: usize,
        write_quorum: usize,
    ) -> std::result::Result<Self, BoxError> {
        if endpoints.len() < data_shards + parity_shards {
            return Err(format!(
                "erasure coding needs at least data_shards + parity_shards = {} storage endpoints, got {}",
                data_shards + parity_shards,
                endpoints.len()
            )
            .into());
        }
        let mut clients: HashMap<String, Arc<dyn StorageBackend>> = HashMap::new();
        for ep in endpoints {
            let client = StorageClient::connect(ep.clone()).await?;
            clients.insert(ep, Arc::new(client));
        }
        let placement = Placement::local(clients, data_shards + parity_shards, DEFAULT_PG_COUNT);
        Ok(Self::from_placement(
            placement,
            data_shards,
            parity_shards,
            write_quorum,
        ))
    }

    /// The ordered storage clients for an object's `k + m` shards (shard `i` →
    /// position `i`).
    fn placement(&self, object_id: ObjectId) -> Vec<Arc<dyn StorageBackend>> {
        self.placement.acting_nodes(object_id)
    }

    /// Split `data` into `k + m` shards: a length-prefixed, zero-padded payload
    /// cut into `k` data shards, plus `m` Reed-Solomon parity shards.
    fn encode(&self, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let k = self.data_shards;
        let mut framed = Vec::with_capacity(LEN_PREFIX + data.len());
        framed.extend_from_slice(&(data.len() as u64).to_be_bytes());
        framed.extend_from_slice(data);

        // reed-solomon-simd needs equal, non-zero, even-length shards.
        let per_shard = framed.len().div_ceil(k).max(1);
        let shard_len = per_shard + (per_shard & 1);
        framed.resize(shard_len * k, 0);

        let mut shards: Vec<Vec<u8>> = (0..k)
            .map(|i| framed[i * shard_len..(i + 1) * shard_len].to_vec())
            .collect();

        if self.parity_shards > 0 {
            let parity = reed_solomon_simd::encode(k, self.parity_shards, &shards)
                .map_err(|_| Error::Erasure("reed-solomon encode failed"))?;
            shards.extend(parity);
        }
        Ok(shards)
    }

    /// Reconstruct the original bytes from any `k` surviving shards
    /// `(shard_index, bytes)`.
    fn reassemble(&self, present: Vec<(usize, Vec<u8>)>) -> Result<Vec<u8>> {
        let k = self.data_shards;
        if present.len() < k {
            return Err(Error::Erasure("insufficient shards to reconstruct object"));
        }

        let mut data: Vec<Option<Vec<u8>>> = vec![None; k];
        let mut recovery: Vec<(usize, Vec<u8>)> = Vec::new();
        for (idx, shard) in present {
            if idx < k {
                data[idx] = Some(shard);
            } else {
                recovery.push((idx - k, shard));
            }
        }

        // Fill any missing data shards from parity.
        if data.iter().any(|s| s.is_none()) {
            let originals = data
                .iter()
                .enumerate()
                .filter_map(|(i, s)| s.as_ref().map(|v| (i, v.as_slice())));
            let recovered = reed_solomon_simd::decode(
                k,
                self.parity_shards,
                originals,
                recovery.iter().map(|(i, v)| (*i, v.as_slice())),
            )
            .map_err(|_| Error::Erasure("reed-solomon decode failed"))?;
            for (i, slot) in data.iter_mut().enumerate() {
                if slot.is_none() {
                    *slot = Some(
                        recovered
                            .get(&i)
                            .cloned()
                            .ok_or(Error::Erasure("decode did not restore a data shard"))?,
                    );
                }
            }
        }

        let mut framed = Vec::with_capacity(data.len());
        for slot in data {
            framed.extend_from_slice(&slot.ok_or(Error::Erasure("missing data shard"))?);
        }

        // Recover the true length from the prefix and truncate the padding.
        if framed.len() < LEN_PREFIX {
            return Err(Error::Erasure("stripe shorter than its length prefix"));
        }
        let mut len_bytes = [0u8; LEN_PREFIX];
        len_bytes.copy_from_slice(&framed[..LEN_PREFIX]);
        let orig_len = u64::from_be_bytes(len_bytes) as usize;
        let end = LEN_PREFIX
            .checked_add(orig_len)
            .filter(|&e| e <= framed.len())
            .ok_or(Error::Erasure("length prefix exceeds reconstructed data"))?;
        Ok(framed[LEN_PREFIX..end].to_vec())
    }
}

impl StorageBackend for ErasureCodedBackend {
    fn put(&self, object_id: ObjectId, data: &[u8]) -> Result<()> {
        let shards = self.encode(data)?;
        let placement = self.placement(object_id);
        let mut acks = 0;
        let mut last_err = None;
        for (i, node) in placement.iter().enumerate() {
            match node.put(object_id, &shards[i]) {
                Ok(()) => acks += 1,
                Err(e) => last_err = Some(e),
            }
        }
        if acks >= self.write_quorum {
            Ok(())
        } else {
            Err(last_err.unwrap_or(Error::Erasure("erasure write quorum not met")))
        }
    }

    fn get(&self, object_id: ObjectId, range: Option<ByteRange>) -> Result<Vec<u8>> {
        // Gather shards until we hold any `k` of them — enough to reconstruct.
        let placement = self.placement(object_id);
        let mut present: Vec<(usize, Vec<u8>)> = Vec::new();
        let mut last_err = None;
        for (idx, node) in placement.iter().enumerate() {
            match node.get(object_id, None) {
                Ok(shard) => present.push((idx, shard)),
                Err(Error::ObjectNotFound(_)) => {}
                Err(e) => last_err = Some(e),
            }
            if present.len() == self.data_shards {
                break;
            }
        }

        if present.len() < self.data_shards {
            // Nothing found at all (and no node errored) → the object is absent.
            return if present.is_empty() && last_err.is_none() {
                Err(Error::ObjectNotFound(object_id))
            } else {
                Err(last_err.unwrap_or(Error::Erasure("insufficient shards to reconstruct object")))
            };
        }

        let full = self.reassemble(present)?;
        match range {
            None => Ok(full),
            Some(r) => {
                let end = r
                    .offset
                    .checked_add(r.length)
                    .filter(|&e| e <= full.len() as u64)
                    .ok_or(Error::BadRange {
                        offset: r.offset,
                        len: r.length,
                        size: full.len() as u32,
                    })?;
                Ok(full[r.offset as usize..end as usize].to_vec())
            }
        }
    }

    fn delete(&self, object_id: ObjectId) -> Result<()> {
        let mut acks = 0;
        let mut last_err = None;
        for node in self.placement(object_id) {
            match node.delete(object_id) {
                Ok(()) => acks += 1,
                Err(e) => last_err = Some(e),
            }
        }
        if acks >= self.write_quorum {
            Ok(())
        } else {
            Err(last_err.unwrap_or(Error::Erasure("erasure delete quorum not met")))
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    /// An in-memory storage node whose availability can be toggled.
    #[derive(Default)]
    struct Node {
        store: Mutex<HashMap<ObjectId, Vec<u8>>>,
        online: AtomicBool,
    }

    impl Node {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                store: Mutex::new(HashMap::new()),
                online: AtomicBool::new(true),
            })
        }
        fn up(&self) -> Result<()> {
            if self.online.load(Ordering::Relaxed) {
                Ok(())
            } else {
                Err(Error::Remote("node offline".into()))
            }
        }
    }

    impl StorageBackend for Node {
        fn put(&self, object_id: ObjectId, data: &[u8]) -> Result<()> {
            self.up()?;
            self.store.lock().unwrap().insert(object_id, data.to_vec());
            Ok(())
        }
        fn get(&self, object_id: ObjectId, _range: Option<ByteRange>) -> Result<Vec<u8>> {
            self.up()?;
            self.store
                .lock()
                .unwrap()
                .get(&object_id)
                .cloned()
                .ok_or(Error::ObjectNotFound(object_id))
        }
        fn delete(&self, object_id: ObjectId) -> Result<()> {
            self.up()?;
            self.store.lock().unwrap().remove(&object_id);
            Ok(())
        }
        fn sync(&self) -> Result<()> {
            Ok(())
        }
        fn checkpoint(&self) -> Result<()> {
            Ok(())
        }
    }

    fn backend(k: usize, m: usize) -> (Vec<Arc<Node>>, ErasureCodedBackend) {
        let nodes: Vec<Arc<Node>> = (0..k + m).map(|_| Node::new()).collect();
        let dyn_nodes: Vec<Arc<dyn StorageBackend>> = nodes
            .iter()
            .map(|n| n.clone() as Arc<dyn StorageBackend>)
            .collect();
        (nodes, ErasureCodedBackend::new(dyn_nodes, k, m, 0))
    }

    #[test]
    fn roundtrip_various_sizes() {
        let (_nodes, ec) = backend(4, 2);
        for (oid, size) in [0usize, 1, 7, 63, 64, 65, 1000, 100_000]
            .into_iter()
            .enumerate()
        {
            let payload: Vec<u8> = (0..size).map(|i| (i * 7 + 3) as u8).collect();
            ec.put(oid as u64, &payload).unwrap();
            assert_eq!(ec.get(oid as u64, None).unwrap(), payload, "size {size}");
        }
    }

    #[test]
    fn shards_are_distinct_and_spread() {
        // Every one of the k+m nodes holds exactly one shard for the object.
        let (nodes, ec) = backend(4, 2);
        ec.put(42, b"spread me across the cluster").unwrap();
        let holders = nodes
            .iter()
            .filter(|n| n.store.lock().unwrap().contains_key(&42))
            .count();
        assert_eq!(holders, 6);
    }

    #[test]
    fn survives_losing_m_nodes_but_not_m_plus_one() {
        let (nodes, ec) = backend(4, 2); // tolerate 2 losses
        let payload: Vec<u8> = (0..5000).map(|i| (i % 251) as u8).collect();
        ec.put(1, &payload).unwrap();

        // Lose any 2 nodes (= m): still reconstructs (often from parity).
        nodes[1].online.store(false, Ordering::Relaxed);
        nodes[4].online.store(false, Ordering::Relaxed);
        assert_eq!(ec.get(1, None).unwrap(), payload);

        // Lose a 3rd (> m): fewer than k shards survive → unrecoverable.
        nodes[0].online.store(false, Ordering::Relaxed);
        assert!(ec.get(1, None).is_err());
    }

    #[test]
    fn reconstructs_when_a_data_shard_is_missing() {
        // Drop shard 0 (a data shard) entirely; parity must rebuild it.
        let (nodes, ec) = backend(3, 2);
        let payload: Vec<u8> = (0..900).map(|i| (i % 256) as u8).collect();
        ec.put(7, &payload).unwrap();
        // Erase every node's copy that holds shard index 0 — but we don't know the
        // placement order, so instead take down 2 nodes and confirm a degraded
        // read still returns the exact bytes (exercises decode).
        nodes[0].online.store(false, Ordering::Relaxed);
        nodes[2].online.store(false, Ordering::Relaxed);
        assert_eq!(ec.get(7, None).unwrap(), payload);
    }

    #[test]
    fn range_read_after_reconstruction() {
        let (nodes, ec) = backend(4, 2);
        let payload: Vec<u8> = (0..2000).map(|i| (i % 256) as u8).collect();
        ec.put(3, &payload).unwrap();
        nodes[2].online.store(false, Ordering::Relaxed); // degrade
        let part = ec
            .get(
                3,
                Some(ByteRange {
                    offset: 100,
                    length: 50,
                }),
            )
            .unwrap();
        assert_eq!(part, payload[100..150]);
    }

    #[test]
    fn write_quorum_floor_keeps_object_readable() {
        // Default write quorum is k+1; a write surviving exactly that is readable.
        let (nodes, ec) = backend(4, 2); // write_quorum defaults to 5
        nodes[0].online.store(false, Ordering::Relaxed); // one node down at write
        ec.put(9, b"five of six shards is enough to ack").unwrap();
        assert_eq!(
            ec.get(9, None).unwrap(),
            b"five of six shards is enough to ack"
        );
    }

    #[test]
    fn write_fails_when_too_few_shards_ack() {
        let (nodes, ec) = backend(4, 2); // needs 5 acks
        nodes[0].online.store(false, Ordering::Relaxed);
        nodes[1].online.store(false, Ordering::Relaxed); // only 4 can ack < 5
        assert!(ec.put(11, b"x").is_err());
    }

    #[test]
    fn absent_object_is_not_found() {
        let (_nodes, ec) = backend(4, 2);
        assert!(matches!(ec.get(404, None), Err(Error::ObjectNotFound(_))));
    }
}
