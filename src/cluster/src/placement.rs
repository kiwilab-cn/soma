//! Placement-group routing (M3): `object_id → pg → PG table → node set`.
//!
//! [`Placement`] is what a backend consults to find an object's nodes. It holds
//! the `node_id → client` map and the PG→node_ids table (the stored, mutable
//! authority — see `docs/M3_DESIGN.md` §2). An object hashes to a placement group;
//! the PG table maps that group to an ordered node-id list; the map resolves those
//! to storage clients. Decoupling object placement from the live node ring this
//! way is what lets M3b migrate a PG without disturbing objects in other PGs.

use std::collections::HashMap;
use std::sync::Arc;

use soma_backend::StorageBackend;
use soma_meta::{MetadataStore, NodeState, PgPlacement};

use crate::ring::{hash64, Ring};
use crate::{MetaClient, StorageClient};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Virtual points per node on the ring.
const VNODES: usize = 64;
/// Default number of placement groups (fixed for the cluster's life).
pub const DEFAULT_PG_COUNT: u32 = 256;
/// A member is considered live if it heartbeated within this many seconds.
const LIVENESS_SECS: u64 = 30;

/// Resolved placement: which storage clients hold an object's replicas/shards.
pub struct Placement {
    clients: HashMap<String, Arc<dyn StorageBackend>>,
    pg_table: HashMap<u32, Vec<String>>,
    pg_count: u32,
}

impl Placement {
    /// Build from a `node_id → client` map and a `pg → node_ids` table.
    pub fn new(
        clients: HashMap<String, Arc<dyn StorageBackend>>,
        pg_table: HashMap<u32, Vec<String>>,
        pg_count: u32,
    ) -> Self {
        Self {
            clients,
            pg_table,
            pg_count,
        }
    }

    /// The placement group an object belongs to.
    pub fn pg_of(&self, object_id: u64) -> u32 {
        (hash64(&object_id) % self.pg_count.max(1) as u64) as u32
    }

    /// The ordered storage clients for an object (replicas, or shards by index).
    /// Missing nodes (in the table but with no live client) are skipped.
    pub fn nodes_for(&self, object_id: u64) -> Vec<Arc<dyn StorageBackend>> {
        let pg = self.pg_of(object_id);
        match self.pg_table.get(&pg) {
            Some(ids) => ids
                .iter()
                .filter_map(|id| self.clients.get(id).cloned())
                .collect(),
            None => Vec::new(),
        }
    }

    /// All distinct storage clients (for cluster-wide ops like sync/checkpoint).
    pub fn all_nodes(&self) -> Vec<Arc<dyn StorageBackend>> {
        self.clients.values().cloned().collect()
    }

    /// Number of known storage clients.
    pub fn node_count(&self) -> usize {
        self.clients.len()
    }

    /// Build a `Placement` over an explicit set of clients keyed by node id,
    /// computing the PG table locally (used for tests and the no-metadata path).
    pub fn local(
        clients: HashMap<String, Arc<dyn StorageBackend>>,
        width: usize,
        pg_count: u32,
    ) -> Self {
        let mut ids: Vec<String> = clients.keys().cloned().collect();
        ids.sort();
        let pg_table = compute_pg_table(&ids, width, pg_count)
            .into_iter()
            .map(|(pg, p)| (pg, p.node_ids))
            .collect();
        Self::new(clients, pg_table, pg_count)
    }

    /// Build the gateway's placement from cluster membership: wait for at least
    /// `width` live members, connect a client to each, seed the PG table (idempotent)
    /// and load the authoritative table back.
    pub async fn from_membership(
        meta: Arc<MetaClient>,
        width: usize,
        pg_count: u32,
        now: u64,
    ) -> Result<Self, BoxError> {
        let members = wait_for_members(&meta, width, now).await?;

        let mut clients: HashMap<String, Arc<dyn StorageBackend>> = HashMap::new();
        let mut ids: Vec<String> = Vec::new();
        for m in &members {
            let client = StorageClient::connect(m.endpoint.clone()).await?;
            clients.insert(m.node_id.clone(), Arc::new(client));
            ids.push(m.node_id.clone());
        }
        ids.sort();

        // Seed the table if empty (first gateway wins, atomically), then read the
        // authoritative table — which may have been seeded by another gateway.
        let computed = compute_pg_table(&ids, width.min(ids.len()), pg_count);
        let m2 = meta.clone();
        tokio::task::spawn_blocking(move || m2.seed_pg_table(&computed)).await??;
        let m3 = meta.clone();
        let loaded = tokio::task::spawn_blocking(move || m3.list_pg_table()).await??;
        let pg_table: HashMap<u32, Vec<String>> =
            loaded.into_iter().map(|(pg, p)| (pg, p.node_ids)).collect();

        Ok(Self::new(clients, pg_table, pg_count))
    }
}

/// Compute the target PG table from a node-id set: each PG maps to its `width`
/// ring-chosen node ids.
pub fn compute_pg_table(
    node_ids: &[String],
    width: usize,
    pg_count: u32,
) -> Vec<(u32, PgPlacement)> {
    let ring = Ring::new(node_ids.to_vec(), VNODES);
    (0..pg_count)
        .map(|pg| {
            (
                pg,
                PgPlacement {
                    node_ids: ring.place(pg as u64, width),
                    generation: 1,
                },
            )
        })
        .collect()
}

/// True if a member heartbeated recently enough to be considered live.
fn is_live(member: &soma_meta::NodeInfo, now: u64) -> bool {
    member.state == NodeState::Active && now.saturating_sub(member.last_heartbeat) <= LIVENESS_SECS
}

/// Poll membership until at least `width` live members are present (bounded wait).
async fn wait_for_members(
    meta: &Arc<MetaClient>,
    width: usize,
    now: u64,
) -> Result<Vec<soma_meta::NodeInfo>, BoxError> {
    // Up to ~30s, so a freshly-scaled gateway waits for storage nodes to register.
    for attempt in 0..30 {
        let m = meta.clone();
        let members = tokio::task::spawn_blocking(move || m.list_members()).await??;
        let live: Vec<_> = members.into_iter().filter(|n| is_live(n, now)).collect();
        if live.len() >= width.max(1) {
            return Ok(live);
        }
        if attempt == 0 {
            tracing::warn!(
                have = live.len(),
                need = width,
                "gateway waiting for storage nodes to register"
            );
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    Err(format!("timed out waiting for {width} live storage members").into())
}
