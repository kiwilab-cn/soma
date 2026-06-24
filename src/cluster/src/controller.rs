//! Rebalance + self-heal controller (M3b/M3c): reconciles the stored PG table
//! toward the target placement implied by live membership, and moves data for
//! migrating PGs.
//!
//! It runs in the **meta** role, holding the concrete metadata store (direct, no
//! RPC) and a `StorageClient` per node. The mover is **throttled** — a bounded
//! number of object copies per pass — so rebalance bleeds in using spare bandwidth
//! and never disturbs foreground S3 throughput (the project's constraint).
//!
//! Each reconcile first sweeps liveness: a node whose heartbeat is stale is marked
//! `Down` (explicit, observable state). Only `Active` nodes are placement targets,
//! so a `Down` or `Draining` node's PGs migrate off it — node-loss re-replication
//! and graceful drain are the same machinery as scale-out (M3c).
//!
//! Migration protocol (replication; erasure reconstruction is M3d):
//! 1. For a PG whose acting set ≠ target, `begin_migration` records the target.
//!    Gateways pick this up on refresh and start dual-writing (`acting ∪ target`).
//! 2. The mover copies each of the PG's objects from an acting node to the new
//!    target nodes, a few per pass.
//! 3. Once every object is present on the target **and** the PG has been migrating
//!    longer than `settle` (so all gateways have refreshed and are dual-writing),
//!    `finalize_migration` flips the acting set to the target and stale replicas on
//!    dropped nodes are reclaimed (best-effort).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use soma_backend::{Error as BackendError, StorageBackend};
use soma_meta::{MetadataStore, NodeState, RedbMetaStore};

use crate::placement::compute_pg_table;
use crate::ring::hash64;
use crate::StorageClient;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// What a single reconcile pass did (for logs and tests).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Migrations newly begun this pass.
    pub started: usize,
    /// Object replicas copied this pass.
    pub copied: usize,
    /// Migrations finalized this pass.
    pub finalized: usize,
    /// PGs still migrating after this pass.
    pub migrating: usize,
}

/// How the cluster stores data, which decides how the mover moves it.
#[derive(Debug, Clone, Copy)]
pub enum Durability {
    /// N-way replication; the mover copies whole objects (M3b).
    Replicated { factor: usize },
    /// Reed-Solomon `k+m`; the mover reconstructs shards for repaired slots (M3d).
    Erasure {
        data_shards: usize,
        parity_shards: usize,
    },
}

impl Durability {
    /// Nodes per placement group.
    fn width(&self) -> usize {
        match self {
            Durability::Replicated { factor } => *factor,
            Durability::Erasure {
                data_shards,
                parity_shards,
            } => data_shards + parity_shards,
        }
    }
}

struct Inner {
    store: Arc<RedbMetaStore>,
    clients: RwLock<HashMap<String, Arc<dyn StorageBackend>>>,
    started: Mutex<HashMap<u32, Instant>>,
    durability: Durability,
    pg_count: u32,
    settle: Duration,
    max_copies_per_pass: usize,
    down_after_secs: u64,
}

/// Drives PG migration to match live membership. Cheap to clone (shared state).
#[derive(Clone)]
pub struct RebalanceController {
    inner: Arc<Inner>,
}

impl RebalanceController {
    /// Create a controller over the concrete metadata store. `durability` decides
    /// PG width and how the mover moves data; `settle` is the minimum time a PG
    /// migrates before it may finalize (≥ the gateway refresh interval);
    /// `max_copies_per_pass` throttles the mover.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Arc<RedbMetaStore>,
        durability: Durability,
        pg_count: u32,
        settle: Duration,
        max_copies_per_pass: usize,
        down_after_secs: u64,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                store,
                clients: RwLock::new(HashMap::new()),
                started: Mutex::new(HashMap::new()),
                durability,
                pg_count,
                settle,
                max_copies_per_pass,
                down_after_secs,
            }),
        }
    }

    /// Run the reconcile loop forever at `interval`.
    pub async fn run(self, interval: Duration) {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            match self.reconcile_once(now_secs()).await {
                Ok(r) if r.started + r.copied + r.finalized > 0 => tracing::info!(
                    started = r.started,
                    copied = r.copied,
                    finalized = r.finalized,
                    migrating = r.migrating,
                    "rebalance progress"
                ),
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "rebalance reconcile failed"),
            }
        }
    }

    /// Connect a client for a node if not already connected.
    async fn ensure_client(&self, node_id: &str, endpoint: &str) -> Result<(), BoxError> {
        if self.inner.clients.read().contains_key(node_id) {
            return Ok(());
        }
        let client = StorageClient::connect(endpoint.to_string()).await?;
        self.inner
            .clients
            .write()
            .insert(node_id.to_string(), Arc::new(client));
        Ok(())
    }

    /// One reconcile pass: begin migrations for changed PGs, move a throttled
    /// batch of data, and finalize PGs whose data has landed and settled.
    pub async fn reconcile_once(&self, now: u64) -> Result<ReconcileReport, BoxError> {
        let store = self.inner.store.clone();
        let mut members = tokio::task::spawn_blocking(move || store.list_members()).await??;

        // Liveness sweep: mark a node Down once its heartbeat is stale. This is the
        // explicit, observable state a future admin UI reads — placement targets
        // then exclude it and its PGs re-replicate (self-heal).
        for m in &mut members {
            let stale = now.saturating_sub(m.last_heartbeat) > self.inner.down_after_secs;
            if stale && m.state != NodeState::Down {
                let s = self.inner.store.clone();
                let id = m.node_id.clone();
                tokio::task::spawn_blocking(move || s.set_node_state(&id, NodeState::Down))
                    .await??;
                m.state = NodeState::Down;
            }
        }

        // Connect to every reachable node (Active sources/targets and Draining
        // sources). Down nodes are unreachable; the mover routes around them.
        for m in members.iter().filter(|m| m.state != NodeState::Down) {
            self.ensure_client(&m.node_id, &m.endpoint).await?;
        }

        // Target-eligible nodes are Active only — Down and Draining are excluded so
        // their data migrates off them.
        let mut node_ids: Vec<String> = members
            .iter()
            .filter(|m| m.state == NodeState::Active)
            .map(|m| m.node_id.clone())
            .collect();
        node_ids.sort();

        let this = self.clone();
        let report =
            tokio::task::spawn_blocking(move || this.reconcile_blocking(node_ids)).await??;
        Ok(report)
    }

    /// The synchronous heart of a reconcile (store + storage IO are all blocking).
    fn reconcile_blocking(&self, node_ids: Vec<String>) -> Result<ReconcileReport, BoxError> {
        let inner = &self.inner;
        let mut report = ReconcileReport::default();
        if node_ids.is_empty() {
            return Ok(report);
        }

        let width = inner.durability.width().min(node_ids.len());
        // Replication targets come straight from the ring; erasure targets are a
        // slot-preserving repair (computed per PG below) so surviving shards never
        // move (which would corrupt in-flight reads).
        let ring_targets: HashMap<u32, Vec<String>> = match inner.durability {
            Durability::Replicated { .. } => compute_pg_table(&node_ids, width, inner.pg_count)
                .into_iter()
                .map(|(pg, p)| (pg, p.node_ids))
                .collect(),
            Durability::Erasure { .. } => HashMap::new(),
        };
        let table = inner.store.list_pg_table()?;
        let clients = inner.clients.read().clone();
        let object_ids = inner.store.list_object_ids()?;
        let mut budget = inner.max_copies_per_pass;

        for (pg, placement) in &table {
            let desired = match inner.durability {
                Durability::Replicated { .. } => ring_targets.get(pg).cloned().unwrap_or_default(),
                Durability::Erasure { .. } => ec_target(&placement.node_ids, &node_ids, width),
            };

            if !placement.is_migrating() {
                if !desired.is_empty() && placement.node_ids != desired {
                    inner.store.begin_migration(*pg, desired)?;
                    inner.started.lock().insert(*pg, Instant::now());
                    report.started += 1;
                    report.migrating += 1;
                }
                continue;
            }

            report.migrating += 1;
            let (done, copied) = match inner.durability {
                Durability::Replicated { .. } => migrate_pg(
                    *pg,
                    &placement.node_ids,
                    &placement.target,
                    &object_ids,
                    &clients,
                    inner.pg_count,
                    &mut budget,
                )?,
                Durability::Erasure {
                    data_shards,
                    parity_shards,
                } => migrate_pg_ec(
                    *pg,
                    &placement.node_ids,
                    &placement.target,
                    &object_ids,
                    &clients,
                    data_shards,
                    parity_shards,
                    inner.pg_count,
                    &mut budget,
                )?,
            };
            report.copied += copied;

            let settled = {
                let mut started = inner.started.lock();
                started.entry(*pg).or_insert_with(Instant::now).elapsed() >= inner.settle
            };
            if done && settled {
                inner.store.finalize_migration(*pg)?;
                inner.started.lock().remove(pg);
                // Reclaim stale replicas on nodes dropped from the acting set
                // (best-effort; a Down node is unreachable and reclaimed by GC).
                cleanup_stale(
                    *pg,
                    &placement.node_ids,
                    &placement.target,
                    &object_ids,
                    &clients,
                    inner.pg_count,
                );
                report.finalized += 1;
                report.migrating -= 1;
            }
        }
        Ok(report)
    }
}

/// Copy a migrating PG's objects onto its new target nodes (throttled by `budget`).
/// Returns `(done, copied)` — `done` is true when every object is present on every
/// target node.
fn migrate_pg(
    pg: u32,
    acting: &[String],
    target: &[String],
    object_ids: &[u64],
    clients: &HashMap<String, Arc<dyn StorageBackend>>,
    pg_count: u32,
    budget: &mut usize,
) -> Result<(bool, usize), BoxError> {
    let new_homes: Vec<String> = target
        .iter()
        .filter(|t| !acting.contains(t))
        .cloned()
        .collect();
    if new_homes.is_empty() {
        return Ok((true, 0)); // target ⊆ acting — nothing to move
    }

    let mut copied = 0;
    let mut all_present = true;
    for &oid in object_ids {
        if (hash64(&oid) % pg_count.max(1) as u64) as u32 != pg {
            continue;
        }
        // Which new homes still lack this object?
        let mut missing = Vec::new();
        for home in &new_homes {
            match clients.get(home) {
                Some(c) => match c.get(oid, None) {
                    Ok(_) => {}
                    Err(BackendError::ObjectNotFound(_)) => missing.push(home.clone()),
                    Err(_) => all_present = false, // a node down → not done this pass
                },
                None => all_present = false,
            }
        }
        if missing.is_empty() {
            continue;
        }
        if *budget == 0 {
            return Ok((false, copied)); // throttled — resume next pass
        }
        // Read the object once from any acting node, then push to each new home.
        let bytes = match read_from_any(acting, clients, oid) {
            Some(b) => b,
            None => {
                all_present = false;
                continue;
            }
        };
        for home in missing {
            if *budget == 0 {
                all_present = false;
                break;
            }
            if let Some(c) = clients.get(&home) {
                c.put(oid, &bytes)?;
                copied += 1;
                *budget -= 1;
            }
        }
    }
    Ok((all_present, copied))
}

/// Slot-preserving erasure target: keep acting nodes that are still active at
/// their slots, and fill each inactive slot with a spare active node. Surviving
/// shards stay put (only repaired slots move), so erasure migration never reorders
/// shards. Returns `acting` unchanged when every slot is healthy (so a pure
/// scale-out triggers no erasure migration).
fn ec_target(acting: &[String], active: &[String], _width: usize) -> Vec<String> {
    use std::collections::HashSet;
    let active_set: HashSet<&String> = active.iter().collect();
    let mut result = acting.to_vec();
    let in_use: HashSet<String> = result
        .iter()
        .filter(|n| active_set.contains(n))
        .cloned()
        .collect();
    let mut spares: Vec<String> = active
        .iter()
        .filter(|n| !in_use.contains(*n))
        .cloned()
        .collect();
    spares.sort();
    let mut next = 0;
    for slot in result.iter_mut() {
        // Replace an inactive node's slot with a spare, if one is available;
        // otherwise leave it (degraded, retried on a later pass).
        if !active_set.contains(slot) && next < spares.len() {
            *slot = spares[next].clone();
            next += 1;
        }
    }
    result
}

/// Reconstruct an erasure-coded PG's shards onto repaired slots (M3d). For each
/// slot whose node changed, the new node needs that slot's shard: gather `k`
/// surviving shards, reconstruct the full set, and write the one shard. Surviving
/// slots are untouched. Returns `(done, copied)`.
#[allow(clippy::too_many_arguments)]
fn migrate_pg_ec(
    pg: u32,
    acting: &[String],
    target: &[String],
    object_ids: &[u64],
    clients: &HashMap<String, Arc<dyn StorageBackend>>,
    k: usize,
    m: usize,
    pg_count: u32,
    budget: &mut usize,
) -> Result<(bool, usize), BoxError> {
    let changed: Vec<usize> = (0..target.len())
        .filter(|&i| acting.get(i) != Some(&target[i]))
        .collect();
    if changed.is_empty() {
        return Ok((true, 0));
    }

    let mut copied = 0;
    let mut all_present = true;
    for &oid in object_ids {
        if (hash64(&oid) % pg_count.max(1) as u64) as u32 != pg {
            continue;
        }
        // Which repaired slots' new node still lacks its shard?
        let mut needed = Vec::new();
        for &i in &changed {
            match clients.get(&target[i]).map(|c| c.get(oid, None)) {
                Some(Ok(_)) => {}
                Some(Err(BackendError::ObjectNotFound(_))) => needed.push(i),
                _ => all_present = false,
            }
        }
        if needed.is_empty() {
            continue;
        }
        if *budget == 0 {
            return Ok((false, copied));
        }
        // Gather k surviving shards (acting node j holds shard j).
        let mut present: Vec<(usize, Vec<u8>)> = Vec::new();
        for (j, node) in acting.iter().enumerate() {
            if present.len() >= k {
                break;
            }
            if let Some(c) = clients.get(node) {
                if let Ok(shard) = c.get(oid, None) {
                    present.push((j, shard));
                }
            }
        }
        if present.len() < k {
            all_present = false;
            continue;
        }
        let shards = match crate::ec::reconstruct_all_shards(present, k, m) {
            Ok(s) => s,
            Err(_) => {
                all_present = false;
                continue;
            }
        };
        for i in needed {
            if *budget == 0 {
                all_present = false;
                break;
            }
            if let (Some(c), Some(shard)) = (clients.get(&target[i]), shards.get(i)) {
                c.put(oid, shard)?;
                copied += 1;
                *budget -= 1;
            }
        }
    }
    Ok((all_present, copied))
}

/// Delete a PG's objects from nodes dropped from the acting set (best-effort).
fn cleanup_stale(
    pg: u32,
    old_acting: &[String],
    target: &[String],
    object_ids: &[u64],
    clients: &HashMap<String, Arc<dyn StorageBackend>>,
    pg_count: u32,
) {
    let dropped: Vec<&String> = old_acting.iter().filter(|a| !target.contains(a)).collect();
    if dropped.is_empty() {
        return;
    }
    for &oid in object_ids {
        if (hash64(&oid) % pg_count.max(1) as u64) as u32 != pg {
            continue;
        }
        for node in &dropped {
            if let Some(c) = clients.get(*node) {
                let _ = c.delete(oid);
            }
        }
    }
}

/// Read an object from the first acting node that has it.
fn read_from_any(
    acting: &[String],
    clients: &HashMap<String, Arc<dyn StorageBackend>>,
    oid: u64,
) -> Option<Vec<u8>> {
    for node in acting {
        if let Some(c) = clients.get(node) {
            if let Ok(bytes) = c.get(oid, None) {
                return Some(bytes);
            }
        }
    }
    None
}

/// Current unix seconds (membership liveness clock).
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
