//! Placement-group routing (M3): `object_id → pg → PG table → node set`.
//!
//! [`Placement`] is what a backend consults to find an object's nodes. It holds
//! the `node_id → client` map and the PG→nodes table (the stored, mutable
//! authority — see `docs/M3_DESIGN.md` §2), behind an `RwLock` so the gateway can
//! refresh it live as membership changes and PGs migrate.
//!
//! **Migration-aware routing.** A PG may be *migrating* from an acting set to a
//! target set. Writes go to `acting ∪ target` (so nothing written mid-migration is
//! lost when the PG finalizes); reads try `target` then `acting` (failover). When
//! not migrating, all three collapse to the acting set, so the common path is
//! unchanged.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
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

/// A PG's routing: the acting (durable) set, and a target set while migrating.
#[derive(Clone, Default)]
struct PgRoute {
    acting: Vec<String>,
    target: Vec<String>,
}

struct Inner {
    clients: HashMap<String, Arc<dyn StorageBackend>>,
    pg_table: HashMap<u32, PgRoute>,
}

/// Resolved, live-refreshable placement. Cheap to clone (shares one `RwLock`), so
/// a backend and a background refresher can hold the same view.
#[derive(Clone)]
pub struct Placement {
    inner: Arc<RwLock<Inner>>,
    pg_count: u32,
}

impl Placement {
    fn from_inner(inner: Inner, pg_count: u32) -> Self {
        Self {
            inner: Arc::new(RwLock::new(inner)),
            pg_count,
        }
    }

    /// The placement group an object belongs to.
    pub fn pg_of(&self, object_id: u64) -> u32 {
        (hash64(&object_id) % self.pg_count.max(1) as u64) as u32
    }

    /// Acting-set clients (the durable home; used by erasure coding, which does not
    /// dual-write).
    pub fn acting_nodes(&self, object_id: u64) -> Vec<Arc<dyn StorageBackend>> {
        let inner = self.inner.read();
        match inner.pg_table.get(&self.pg_of(object_id)) {
            Some(r) => resolve(&inner.clients, &r.acting),
            None => Vec::new(),
        }
    }

    /// Write-set clients: `acting ∪ target`, so a write during migration reaches
    /// both homes.
    pub fn write_nodes(&self, object_id: u64) -> Vec<Arc<dyn StorageBackend>> {
        let inner = self.inner.read();
        match inner.pg_table.get(&self.pg_of(object_id)) {
            Some(r) => {
                let mut ids = r.acting.clone();
                for t in &r.target {
                    if !ids.contains(t) {
                        ids.push(t.clone());
                    }
                }
                resolve(&inner.clients, &ids)
            }
            None => Vec::new(),
        }
    }

    /// Read-set clients in failover order: `target` first (the new home), then
    /// `acting`.
    pub fn read_nodes(&self, object_id: u64) -> Vec<Arc<dyn StorageBackend>> {
        let inner = self.inner.read();
        match inner.pg_table.get(&self.pg_of(object_id)) {
            Some(r) => {
                let mut ids = r.target.clone();
                for a in &r.acting {
                    if !ids.contains(a) {
                        ids.push(a.clone());
                    }
                }
                resolve(&inner.clients, &ids)
            }
            None => Vec::new(),
        }
    }

    /// All distinct storage clients (for cluster-wide ops like sync/checkpoint).
    pub fn all_nodes(&self) -> Vec<Arc<dyn StorageBackend>> {
        self.inner.read().clients.values().cloned().collect()
    }

    /// Number of known storage clients.
    pub fn node_count(&self) -> usize {
        self.inner.read().clients.len()
    }

    /// Build over an explicit set of clients keyed by node id, computing a static
    /// (non-migrating) PG table locally. Used by tests and the no-metadata path.
    pub fn local(
        clients: HashMap<String, Arc<dyn StorageBackend>>,
        width: usize,
        pg_count: u32,
    ) -> Self {
        let mut ids: Vec<String> = clients.keys().cloned().collect();
        ids.sort();
        let pg_table = compute_pg_table(&ids, width, pg_count)
            .into_iter()
            .map(|(pg, p)| {
                (
                    pg,
                    PgRoute {
                        acting: p.node_ids,
                        target: p.target,
                    },
                )
            })
            .collect();
        Self::from_inner(Inner { clients, pg_table }, pg_count)
    }

    /// Build the gateway's placement from cluster membership: wait for at least
    /// `width` live members, connect a client to each, seed the PG table
    /// (idempotent) and load the authoritative table back.
    pub async fn from_membership(
        meta: Arc<MetaClient>,
        width: usize,
        pg_count: u32,
        now: u64,
    ) -> Result<Self, BoxError> {
        let members = wait_for_members(&meta, width, now).await?;
        let (clients, ids) = connect_clients(&HashMap::new(), &members).await?;

        // Seed the table if empty (first gateway wins atomically), then read the
        // authoritative table back (another gateway may have seeded it).
        let computed = compute_pg_table(&ids, width.min(ids.len()), pg_count);
        let m2 = meta.clone();
        tokio::task::spawn_blocking(move || m2.seed_pg_table(&computed)).await??;
        let pg_table = load_pg_table(&meta).await?;

        Ok(Self::from_inner(Inner { clients, pg_table }, pg_count))
    }

    /// Re-read membership + the PG table and swap the live view. Picks up newly
    /// registered nodes (connecting clients) and migration/finalize transitions.
    pub async fn refresh(&self, meta: &Arc<MetaClient>, now: u64) -> Result<(), BoxError> {
        let members = {
            let m = meta.clone();
            let all = tokio::task::spawn_blocking(move || m.list_members()).await??;
            all.into_iter()
                .filter(|n| member_is_live(n, now))
                .collect::<Vec<_>>()
        };
        let existing = self.inner.read().clients.clone();
        let (clients, _ids) = connect_clients(&existing, &members).await?;
        let pg_table = load_pg_table(meta).await?;

        let mut w = self.inner.write();
        w.clients = clients;
        w.pg_table = pg_table;
        Ok(())
    }
}

/// Map node ids to their clients (skipping any without a live client).
fn resolve(
    clients: &HashMap<String, Arc<dyn StorageBackend>>,
    ids: &[String],
) -> Vec<Arc<dyn StorageBackend>> {
    ids.iter()
        .filter_map(|id| clients.get(id).cloned())
        .collect()
}

/// Connect a client to each member, reusing any already-connected client.
async fn connect_clients(
    existing: &HashMap<String, Arc<dyn StorageBackend>>,
    members: &[soma_meta::NodeInfo],
) -> Result<(HashMap<String, Arc<dyn StorageBackend>>, Vec<String>), BoxError> {
    let mut clients: HashMap<String, Arc<dyn StorageBackend>> = HashMap::new();
    let mut ids = Vec::new();
    for m in members {
        let client = match existing.get(&m.node_id) {
            Some(c) => c.clone(),
            None => Arc::new(StorageClient::connect(m.endpoint.clone()).await?),
        };
        clients.insert(m.node_id.clone(), client);
        ids.push(m.node_id.clone());
    }
    ids.sort();
    Ok((clients, ids))
}

/// Load the PG table from metadata into routes.
async fn load_pg_table(meta: &Arc<MetaClient>) -> Result<HashMap<u32, PgRoute>, BoxError> {
    let m = meta.clone();
    let table = tokio::task::spawn_blocking(move || m.list_pg_table()).await??;
    Ok(table
        .into_iter()
        .map(|(pg, p)| {
            (
                pg,
                PgRoute {
                    acting: p.node_ids,
                    target: p.target,
                },
            )
        })
        .collect())
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
                    target: Vec::new(),
                    generation: 1,
                },
            )
        })
        .collect()
}

/// True if a member heartbeated recently enough to be considered live.
pub(crate) fn member_is_live(member: &soma_meta::NodeInfo, now: u64) -> bool {
    member.state == NodeState::Active && now.saturating_sub(member.last_heartbeat) <= LIVENESS_SECS
}

/// Poll membership until at least `width` live members are present (bounded wait).
async fn wait_for_members(
    meta: &Arc<MetaClient>,
    width: usize,
    now: u64,
) -> Result<Vec<soma_meta::NodeInfo>, BoxError> {
    for attempt in 0..30 {
        let m = meta.clone();
        let members = tokio::task::spawn_blocking(move || m.list_members()).await??;
        let live: Vec<_> = members
            .into_iter()
            .filter(|n| member_is_live(n, now))
            .collect();
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
