//! Data-locality oracle: resolve an object to the nodes (and their topology) that
//! hold its bytes, so a co-located compute scheduler can place reads on or near the
//! data — the HDFS `getFileBlockLocations` analogue (see `docs/M4_DESIGN.md`).
//!
//! Placement only knows node ids per PG; this wraps it with the cluster's data
//! layout (replication width or erasure `k+m`) to assign each node a role and
//! attach its membership topology.

use soma_core::ObjectId;
use soma_meta::{DataLayout, LocationOracle, NodeLocation, ObjectLocations, ShardRole};

use crate::placement::Placement;

/// A [`LocationOracle`] backed by the gateway's live [`Placement`] view plus the
/// cluster's configured data layout.
pub struct PlacementOracle {
    placement: Placement,
    layout: DataLayout,
}

impl PlacementOracle {
    /// Wrap a placement view with the layout the gateway writes objects under.
    pub fn new(placement: Placement, layout: DataLayout) -> Self {
        Self { placement, layout }
    }
}

impl LocationOracle for PlacementOracle {
    fn locate(&self, object_id: ObjectId, size: u64) -> Option<ObjectLocations> {
        let ids = self.placement.acting_ids(object_id);
        if ids.is_empty() {
            return None;
        }
        // For erasure layout, the first `k` placement positions are data shards and
        // the rest parity; for replication every node is a full replica.
        let data_shards = match self.layout {
            DataLayout::Erasure { data_shards, .. } => data_shards,
            DataLayout::Replicated { .. } => 0,
        };
        let nodes = ids
            .iter()
            .enumerate()
            .map(|(i, id)| {
                let role = match self.layout {
                    DataLayout::Replicated { .. } => ShardRole::Replica,
                    DataLayout::Erasure { .. } if i < data_shards => {
                        ShardRole::DataShard { index: i }
                    }
                    DataLayout::Erasure { .. } => ShardRole::ParityShard {
                        index: i - data_shards,
                    },
                };
                let m = self.placement.member(id);
                NodeLocation {
                    node_id: id.clone(),
                    endpoint: m.as_ref().map(|m| m.endpoint.clone()).unwrap_or_default(),
                    zone: m.as_ref().map(|m| m.zone.clone()).unwrap_or_default(),
                    host: m.as_ref().map(|m| m.host.clone()).unwrap_or_default(),
                    role,
                }
            })
            .collect();
        Some(ObjectLocations {
            object_id,
            size,
            layout: self.layout,
            nodes,
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use std::collections::HashMap;
    use std::sync::Arc;

    use soma_backend::{ByteRange, Result as BResult, StorageBackend};
    use soma_core::ObjectId;
    use soma_meta::{DataLayout, LocationOracle, ShardRole};

    use super::PlacementOracle;
    use crate::placement::Placement;
    use crate::placement::DEFAULT_PG_COUNT;

    /// A no-op backend: the oracle never touches bytes, only node ids.
    struct Dummy;
    impl StorageBackend for Dummy {
        fn put(&self, _: ObjectId, _: &[u8]) -> BResult<()> {
            Ok(())
        }
        fn get(&self, _: ObjectId, _: Option<ByteRange>) -> BResult<Vec<u8>> {
            Ok(Vec::new())
        }
        fn delete(&self, _: ObjectId) -> BResult<()> {
            Ok(())
        }
        fn sync(&self) -> BResult<()> {
            Ok(())
        }
        fn checkpoint(&self) -> BResult<()> {
            Ok(())
        }
    }

    fn placement(n: usize, width: usize) -> Placement {
        let clients: HashMap<String, Arc<dyn StorageBackend>> = (0..n)
            .map(|i| (format!("n{i}"), Arc::new(Dummy) as Arc<dyn StorageBackend>))
            .collect();
        Placement::local(clients, width, DEFAULT_PG_COUNT)
    }

    #[test]
    fn replicated_layout_marks_every_node_a_replica() {
        let oracle = PlacementOracle::new(placement(3, 3), DataLayout::Replicated { width: 3 });
        let loc = oracle.locate(42, 100).unwrap();
        assert_eq!(loc.object_id, 42);
        assert_eq!(loc.size, 100);
        assert_eq!(loc.nodes.len(), 3);
        assert!(loc.nodes.iter().all(|n| n.role == ShardRole::Replica));
    }

    #[test]
    fn erasure_layout_splits_data_then_parity_shards() {
        let oracle = PlacementOracle::new(
            placement(6, 6),
            DataLayout::Erasure {
                data_shards: 4,
                parity_shards: 2,
            },
        );
        let loc = oracle.locate(7, 999).unwrap();
        assert_eq!(loc.nodes.len(), 6);
        // The first 4 placement positions are data shards, the last 2 parity —
        // indices contiguous from 0 in each group.
        let data: Vec<usize> = loc
            .nodes
            .iter()
            .filter_map(|n| match n.role {
                ShardRole::DataShard { index } => Some(index),
                _ => None,
            })
            .collect();
        let parity: Vec<usize> = loc
            .nodes
            .iter()
            .filter_map(|n| match n.role {
                ShardRole::ParityShard { index } => Some(index),
                _ => None,
            })
            .collect();
        assert_eq!(data, vec![0, 1, 2, 3]);
        assert_eq!(parity, vec![0, 1]);
    }

    #[test]
    fn unresolvable_pg_returns_none() {
        // No nodes → no placement → no locations.
        let oracle = PlacementOracle::new(placement(0, 1), DataLayout::Replicated { width: 1 });
        assert!(oracle.locate(1, 1).is_none());
    }
}
