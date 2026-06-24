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

struct Inner {
    store: Arc<RedbMetaStore>,
    clients: RwLock<HashMap<String, Arc<dyn StorageBackend>>>,
    started: Mutex<HashMap<u32, Instant>>,
    width: usize,
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
    /// Create a controller over the concrete metadata store. `width` is the
    /// replica factor; `settle` is the minimum time a PG migrates before it may
    /// finalize (≥ the gateway refresh interval); `max_copies_per_pass` throttles
    /// the mover.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Arc<RedbMetaStore>,
        width: usize,
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
                width,
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

        let target_table: HashMap<u32, Vec<String>> =
            compute_pg_table(&node_ids, inner.width.min(node_ids.len()), inner.pg_count)
                .into_iter()
                .map(|(pg, p)| (pg, p.node_ids))
                .collect();
        let table = inner.store.list_pg_table()?;
        let clients = inner.clients.read().clone();
        let object_ids = inner.store.list_object_ids()?;
        let mut budget = inner.max_copies_per_pass;

        for (pg, placement) in &table {
            if !placement.is_migrating() {
                let desired = target_table.get(pg).cloned().unwrap_or_default();
                if !desired.is_empty() && placement.node_ids != desired {
                    inner.store.begin_migration(*pg, desired)?;
                    inner.started.lock().insert(*pg, Instant::now());
                    report.started += 1;
                    report.migrating += 1;
                }
                continue;
            }

            report.migrating += 1;
            let (done, copied) = migrate_pg(
                *pg,
                &placement.node_ids,
                &placement.target,
                &object_ids,
                &clients,
                inner.pg_count,
                &mut budget,
            )?;
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
