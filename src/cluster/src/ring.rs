//! Consistent-hash ring for replica placement.
//!
//! Storage nodes are placed on a hash ring with virtual nodes for balance. An
//! object's replica set is the next `rf` **distinct** nodes clockwise from the
//! object's hash. Adding/removing a node only remaps a small fraction of objects.
//!
//! The hash is `std`'s `DefaultHasher` (SipHash-1-3 with fixed keys) — stable
//! across processes of the same binary, so every gateway computes the same
//! placement. (A dedicated stable hash like xxHash would be preferable long-term.)

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

fn hash64<T: Hash>(value: &T) -> u64 {
    let mut h = DefaultHasher::new();
    value.hash(&mut h);
    h.finish()
}

/// A consistent-hash ring over `node_count` storage nodes (referenced by index).
pub(crate) struct Ring {
    /// `(point_hash, node_index)`, sorted by hash.
    points: Vec<(u64, usize)>,
    node_count: usize,
}

impl Ring {
    /// Build a ring over `node_count` nodes with `vnodes` virtual points each.
    pub(crate) fn new(node_count: usize, vnodes: usize) -> Self {
        let mut points = Vec::with_capacity(node_count * vnodes);
        for node in 0..node_count {
            for v in 0..vnodes {
                points.push((hash64(&(node, v)), node));
            }
        }
        points.sort_unstable_by_key(|(h, _)| *h);
        Ring { points, node_count }
    }

    /// The replica node indices for `object_id`: the next `rf` distinct nodes
    /// clockwise from the object's hash (clamped to the node count).
    pub(crate) fn replicas(&self, object_id: u64, rf: usize) -> Vec<usize> {
        let rf = rf.min(self.node_count);
        if rf == 0 || self.points.is_empty() {
            return Vec::new();
        }
        let h = hash64(&object_id);
        let start = self.points.partition_point(|(ph, _)| *ph < h);
        let n = self.points.len();
        let mut result = Vec::with_capacity(rf);
        for i in 0..n {
            if result.len() == rf {
                break;
            }
            let (_, node) = self.points[(start + i) % n];
            if !result.contains(&node) {
                result.push(node);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn replicas_are_distinct_and_sized() {
        let ring = Ring::new(5, 64);
        for oid in 0..1000u64 {
            let r = ring.replicas(oid, 3);
            assert_eq!(r.len(), 3);
            // distinct
            let mut sorted = r.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(sorted.len(), 3);
            // valid indices
            assert!(r.iter().all(|&n| n < 5));
        }
    }

    #[test]
    fn rf_clamped_to_node_count() {
        let ring = Ring::new(2, 16);
        assert_eq!(ring.replicas(7, 5).len(), 2);
    }

    #[test]
    fn placement_is_deterministic() {
        let a = Ring::new(4, 32);
        let b = Ring::new(4, 32);
        for oid in [1u64, 42, 9999, u64::MAX] {
            assert_eq!(a.replicas(oid, 3), b.replicas(oid, 3));
        }
    }

    #[test]
    fn roughly_balanced() {
        let ring = Ring::new(4, 128);
        let mut counts = [0usize; 4];
        for oid in 0..8000u64 {
            counts[ring.replicas(oid, 1)[0]] += 1;
        }
        // Each node should own a non-trivial share (very loose bound).
        assert!(counts.iter().all(|&c| c > 8000 / 4 / 3));
    }
}
