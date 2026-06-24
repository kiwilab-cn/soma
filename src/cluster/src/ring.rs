//! Consistent-hash ring for placement-group placement.
//!
//! Storage nodes are placed on a hash ring (by stable `node_id`, never array
//! index) with virtual nodes for balance. A key's placement is the next `width`
//! **distinct** node ids clockwise from the key's hash. Adding/removing a node
//! only remaps a small fraction of keys.
//!
//! M3 places **placement groups** (not objects) onto nodes: `place(pg, width)`
//! computes the target node set for a PG. The hash is `std`'s `DefaultHasher`
//! (SipHash-1-3 with fixed keys) — stable across processes of the same binary, so
//! every gateway computes the same target.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub(crate) fn hash64<T: Hash>(value: &T) -> u64 {
    let mut h = DefaultHasher::new();
    value.hash(&mut h);
    h.finish()
}

/// A consistent-hash ring over a set of storage nodes referenced by `node_id`.
pub(crate) struct Ring {
    /// `(point_hash, node_index)`, sorted by hash.
    points: Vec<(u64, usize)>,
    /// Node ids, indexed by the `node_index` stored in `points`.
    ids: Vec<String>,
}

impl Ring {
    /// Build a ring over `ids` with `vnodes` virtual points each.
    pub(crate) fn new(ids: Vec<String>, vnodes: usize) -> Self {
        let mut points = Vec::with_capacity(ids.len() * vnodes);
        for (idx, id) in ids.iter().enumerate() {
            for v in 0..vnodes {
                points.push((hash64(&(id, v)), idx));
            }
        }
        points.sort_unstable_by_key(|(h, _)| *h);
        Ring { points, ids }
    }

    /// The placement for `key`: the next `width` distinct node ids clockwise from
    /// the key's hash (clamped to the node count).
    pub(crate) fn place(&self, key: u64, width: usize) -> Vec<String> {
        let width = width.min(self.ids.len());
        if width == 0 || self.points.is_empty() {
            return Vec::new();
        }
        let h = hash64(&key);
        let start = self.points.partition_point(|(ph, _)| *ph < h);
        let n = self.points.len();
        let mut chosen: Vec<usize> = Vec::with_capacity(width);
        for i in 0..n {
            if chosen.len() == width {
                break;
            }
            let (_, idx) = self.points[(start + i) % n];
            if !chosen.contains(&idx) {
                chosen.push(idx);
            }
        }
        chosen.into_iter().map(|i| self.ids[i].clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    fn ids(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("node-{i}")).collect()
    }

    #[test]
    fn placement_is_distinct_and_sized() {
        let ring = Ring::new(ids(5), 64);
        for pg in 0..1000u64 {
            let r = ring.place(pg, 3);
            assert_eq!(r.len(), 3);
            let mut sorted = r.clone();
            sorted.sort();
            sorted.dedup();
            assert_eq!(sorted.len(), 3); // distinct
            assert!(r.iter().all(|id| id.starts_with("node-")));
        }
    }

    #[test]
    fn width_clamped_to_node_count() {
        let ring = Ring::new(ids(2), 16);
        assert_eq!(ring.place(7, 5).len(), 2);
    }

    #[test]
    fn placement_is_deterministic() {
        let a = Ring::new(ids(4), 32);
        let b = Ring::new(ids(4), 32);
        for pg in [1u64, 42, 9999, u64::MAX] {
            assert_eq!(a.place(pg, 3), b.place(pg, 3));
        }
    }

    #[test]
    fn roughly_balanced() {
        let ring = Ring::new(ids(4), 128);
        let mut counts = std::collections::HashMap::new();
        for pg in 0..8000u64 {
            *counts.entry(ring.place(pg, 1)[0].clone()).or_insert(0usize) += 1;
        }
        assert!(counts.values().all(|&c| c > 8000 / 4 / 3));
    }
}
