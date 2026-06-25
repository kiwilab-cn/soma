//! Soma server entry point. One binary, four roles (`--role` / config `role`):
//! `standalone` (single process, the M0/M1 behavior), `gateway` (stateless S3
//! front-end), `meta` (metadata gRPC node), and `storage` (storage gRPC node).

mod admin;
mod config;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use soma_backend::{
    BackendConfig, CachingBackend, Crypto, LocalFsBackend, LocalReader, StaticKeyProvider,
    StorageBackend,
};
use soma_cluster::{
    serve_meta, serve_storage, Durability, ErasureCodedBackend, MetaClient, Placement,
    PlacementOracle, RebalanceController, ReplicatedBackend, DEFAULT_PG_COUNT,
};
use soma_localfd::serve_local_reads;
use soma_meta::{DataLayout, MetadataStore, NodeTopology, RedbMetaStore};
use soma_s3::{router, Credentials, S3Service};

use admin::AdminState;
use config::Config;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cfg = Config::load(config_path().as_deref())?;

    match cfg.role.as_str() {
        "standalone" => run_standalone(cfg).await,
        "gateway" => run_gateway(cfg).await,
        "meta" => run_meta(cfg).await,
        "storage" => run_storage(cfg).await,
        other => {
            Err(format!("unknown role '{other}' (expected standalone|gateway|meta|storage)").into())
        }
    }
}

/// Open the local metadata store.
fn open_meta(cfg: &Config) -> Result<Arc<dyn MetadataStore>, BoxError> {
    std::fs::create_dir_all(&cfg.data_dir)?;
    let path = format!("{}/meta.redb", cfg.data_dir);
    Ok(Arc::new(RedbMetaStore::open(&path)?))
}

/// Open the local storage backend (no cache — caching lives on the gateway).
fn open_backend(cfg: &Config) -> Result<Arc<dyn StorageBackend>, BoxError> {
    std::fs::create_dir_all(&cfg.data_dir)?;
    Ok(Arc::new(LocalFsBackend::open(
        &cfg.data_dir,
        BackendConfig {
            volume_max: cfg.volume_max_bytes(),
        },
    )?))
}

fn build_credentials(cfg: &Config) -> Credentials {
    let mut creds = Credentials::new();
    for c in &cfg.credentials {
        creds.add(&c.access_key, &c.secret_key);
    }
    creds
}

/// Assemble the S3 service with (if a master key is configured) object crypto for
/// per-bucket server-side encryption. Per-bucket quotas and rate limits are
/// configured via the admin API and live in each bucket's metadata.
fn build_service(
    meta: Arc<dyn MetadataStore>,
    backend: Arc<dyn StorageBackend>,
    cfg: &Config,
) -> Result<S3Service, BoxError> {
    let mut service = S3Service::new(meta, backend, build_credentials(cfg));
    if let Some(crypto) = maybe_crypto(cfg)? {
        service = service.with_crypto(crypto);
    }
    Ok(service)
}

/// Build object crypto from the configured master key, if any. Buckets opt into
/// encryption via `PutBucketEncryption`; without a master key, those calls fail.
fn maybe_crypto(cfg: &Config) -> Result<Option<Crypto>, BoxError> {
    if cfg.encryption.master_key.is_empty() {
        return Ok(None);
    }
    let keys = StaticKeyProvider::from_base64(&cfg.encryption.master_key)?;
    let mut crypto = Crypto::new(&keys);
    if cfg.encryption.chunk_size_bytes > 0 {
        crypto = crypto.with_chunk_size(cfg.encryption.chunk_size_bytes);
    }
    Ok(Some(crypto))
}

/// Wrap a backend in the read cache if enabled.
fn maybe_cache(cfg: &Config, backend: Arc<dyn StorageBackend>) -> Arc<dyn StorageBackend> {
    if cfg.cache.enabled {
        Arc::new(CachingBackend::new(
            backend,
            cfg.cache_max_bytes() as usize,
            cfg.cache_max_object_bytes(),
        ))
    } else {
        backend
    }
}

/// Current unix time in seconds (the membership clock; the meta store itself does
/// no clock access and takes `now` from callers).
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Background membership loop for a storage node: register with the meta node,
/// then heartbeat on an interval (re-registering if the meta store forgot us).
/// Best-effort — a meta outage never crashes the storage node.
async fn run_membership(
    meta_endpoint: String,
    node_id: String,
    endpoint: String,
    topology: NodeTopology,
    interval_secs: u64,
) {
    let client = match MetaClient::connect(meta_endpoint).await {
        Ok(c) => Arc::new(c),
        Err(e) => {
            tracing::warn!(error = %e, "membership: cannot reach meta; node will not register");
            return;
        }
    };
    let mut registered = false;
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval_secs.max(1)));
    loop {
        ticker.tick().await;
        let (c, nid, ep, topo, now, need_register) = (
            client.clone(),
            node_id.clone(),
            endpoint.clone(),
            topology.clone(),
            now_secs(),
            !registered,
        );
        let res = tokio::task::spawn_blocking(move || {
            if need_register {
                c.register_node(&nid, &ep, topo, now)
            } else {
                c.heartbeat(&nid, now)
            }
        })
        .await;
        match res {
            Ok(Ok(())) => {
                if need_register {
                    registered = true;
                    tracing::info!(node_id = %node_id, endpoint = %endpoint, "registered with cluster membership");
                }
            }
            // A failed heartbeat (e.g. the meta store forgot us) → re-register next tick.
            Ok(Err(e)) => {
                registered = false;
                tracing::debug!(error = %e, "membership heartbeat failed; will re-register");
            }
            Err(e) => tracing::warn!(error = %e, "membership task join error"),
        }
    }
}

// --- roles -----------------------------------------------------------------

/// Single process: metadata + storage + S3 + admin in one.
async fn run_standalone(cfg: Config) -> Result<(), BoxError> {
    let metrics = PrometheusBuilder::new().install_recorder()?;
    let meta = open_meta(&cfg)?;
    let backend = maybe_cache(&cfg, open_backend(&cfg)?);
    let admin_meta = meta.clone(); // for per-bucket QoS admin endpoints
    let service = build_service(meta, backend, &cfg)?;
    // Standalone is single-node: no cluster membership, so no drain endpoint, but
    // the local meta store still backs the per-bucket quota / rate-limit admin API.
    serve_s3_and_admin(&cfg, service, metrics, "standalone", Some(admin_meta)).await
}

/// Stateless gateway: S3 front-end over remote metadata + storage nodes.
async fn run_gateway(cfg: Config) -> Result<(), BoxError> {
    let metrics = PrometheusBuilder::new().install_recorder()?;
    let meta_client = Arc::new(MetaClient::connect(cfg.meta_endpoint.clone()).await?);

    // Resolve placement from cluster membership: the PG width is the replica
    // factor (replication) or k+m shards (erasure). The gateway builds its node
    // set from the membership table, not static config.
    let width = if cfg.erasure.enabled {
        cfg.erasure.data_shards + cfg.erasure.parity_shards
    } else {
        cfg.replication_factor
    };
    let placement =
        Placement::from_membership(meta_client.clone(), width, DEFAULT_PG_COUNT, now_secs())
            .await?;
    let node_count = placement.node_count();

    // Refresh placement periodically so node joins and PG migrations are picked up
    // live (membership + PG table). The backend shares this view (Arc).
    {
        let refresher = placement.clone();
        let meta_for_refresh = meta_client.clone();
        let refresh_secs = cfg.placement_refresh_secs.max(1);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(refresh_secs));
            loop {
                ticker.tick().await;
                if let Err(e) = refresher.refresh(&meta_for_refresh, now_secs()).await {
                    tracing::debug!(error = %e, "placement refresh failed");
                }
            }
        });
    }

    // The data-locality oracle shares the gateway's live placement view (cheap
    // Arc clone) so `GET object?location` reports where each object's bytes live.
    let layout = if cfg.erasure.enabled {
        DataLayout::Erasure {
            data_shards: cfg.erasure.data_shards,
            parity_shards: cfg.erasure.parity_shards,
        }
    } else {
        DataLayout::Replicated { width }
    };
    let oracle = Arc::new(PlacementOracle::new(placement.clone(), layout));

    let storage: Arc<dyn StorageBackend> = if cfg.erasure.enabled {
        Arc::new(ErasureCodedBackend::from_placement(
            placement,
            cfg.erasure.data_shards,
            cfg.erasure.parity_shards,
            cfg.erasure.write_quorum,
        ))
    } else {
        Arc::new(ReplicatedBackend::from_placement(
            placement,
            cfg.write_quorum,
        ))
    };
    let meta: Arc<dyn MetadataStore> = meta_client;
    let admin_meta = meta.clone(); // for the drain endpoint
    let backend = maybe_cache(&cfg, storage);
    let service = build_service(meta, backend, &cfg)?.with_oracle(oracle);
    if cfg.erasure.enabled {
        tracing::info!(
            meta = %cfg.meta_endpoint,
            storage_nodes = node_count,
            pg_count = DEFAULT_PG_COUNT,
            durability = "erasure",
            data_shards = cfg.erasure.data_shards,
            parity_shards = cfg.erasure.parity_shards,
            "gateway connected to cluster"
        );
    } else {
        tracing::info!(
            meta = %cfg.meta_endpoint,
            storage_nodes = node_count,
            pg_count = DEFAULT_PG_COUNT,
            durability = "replicated",
            replication_factor = cfg.replication_factor,
            write_quorum = cfg.write_quorum,
            "gateway connected to cluster"
        );
    }
    serve_s3_and_admin(&cfg, service, metrics, "gateway", Some(admin_meta)).await
}

/// Metadata node: serves `MetadataStore` over gRPC, plus the rebalance controller.
async fn run_meta(cfg: Config) -> Result<(), BoxError> {
    std::fs::create_dir_all(&cfg.data_dir)?;
    let store = Arc::new(RedbMetaStore::open(format!("{}/meta.redb", cfg.data_dir))?);

    // The rebalance controller reconciles placement toward live membership and
    // moves data for migrating PGs (throttled). It needs the concrete store.
    if cfg.rebalance.enabled {
        let durability = if cfg.erasure.enabled {
            Durability::Erasure {
                data_shards: cfg.erasure.data_shards,
                parity_shards: cfg.erasure.parity_shards,
            }
        } else {
            Durability::Replicated {
                factor: cfg.replication_factor,
            }
        };
        let controller = RebalanceController::new(
            store.clone(),
            durability,
            DEFAULT_PG_COUNT,
            std::time::Duration::from_secs(cfg.rebalance.settle_secs),
            cfg.rebalance.max_copies_per_pass,
            cfg.rebalance.down_after_secs,
            cfg.rebalance.max_garbage_per_pass,
        );
        tracing::info!(
            interval_secs = cfg.rebalance.interval_secs,
            settle_secs = cfg.rebalance.settle_secs,
            max_copies_per_pass = cfg.rebalance.max_copies_per_pass,
            durability = ?durability,
            "rebalance controller enabled"
        );
        tokio::spawn(controller.run(std::time::Duration::from_secs(
            cfg.rebalance.interval_secs.max(1),
        )));
    }

    let addr: SocketAddr = cfg.listen.parse()?;
    let serving: Arc<dyn MetadataStore> = store;
    tracing::info!(listen = %addr, data_dir = %cfg.data_dir, "soma meta node listening");
    serve_meta(addr, serving).await?;
    Ok(())
}

/// Storage node: serves `StorageBackend` over gRPC, with a background scrubber.
async fn run_storage(cfg: Config) -> Result<(), BoxError> {
    std::fs::create_dir_all(&cfg.data_dir)?;
    let backend = Arc::new(LocalFsBackend::open(
        &cfg.data_dir,
        BackendConfig {
            volume_max: cfg.volume_max_bytes(),
        },
    )?);

    // Background compaction: reclaim dead-needle space from sealed volumes.
    let compact_secs = cfg.storage.compact_interval_secs;
    if compact_secs > 0 {
        let b = backend.clone();
        let ratio = cfg.storage.compact_min_reclaim_ratio;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(compact_secs));
            loop {
                ticker.tick().await;
                let b2 = b.clone();
                if let Ok(Ok(report)) = tokio::task::spawn_blocking(move || b2.compact(ratio)).await
                {
                    if report.volumes_compacted > 0 {
                        tracing::info!(
                            volumes = report.volumes_compacted,
                            bytes_reclaimed = report.bytes_reclaimed,
                            needles_kept = report.needles_kept,
                            "compaction reclaimed space"
                        );
                    }
                }
            }
        });
    }

    let scrub_secs = cfg.storage.scrub_interval_secs;
    if scrub_secs > 0 {
        let b = backend.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(scrub_secs));
            loop {
                ticker.tick().await;
                let b2 = b.clone();
                if let Ok(Ok(report)) = tokio::task::spawn_blocking(move || b2.scrub()).await {
                    if report.corrupt.is_empty() {
                        tracing::debug!(checked = report.checked, "scrub clean");
                    } else {
                        tracing::warn!(
                            checked = report.checked,
                            corrupt = report.corrupt.len(),
                            "scrub detected payload corruption"
                        );
                    }
                }
            }
        });
    }

    // Self-register with cluster membership and heartbeat (M3). Best-effort.
    if !cfg.meta_endpoint.is_empty() && cfg.storage.heartbeat_interval_secs > 0 {
        let node_id = if cfg.node_id.is_empty() {
            cfg.listen.clone()
        } else {
            cfg.node_id.clone()
        };
        let advertise = if cfg.advertise_endpoint.is_empty() {
            format!("http://{}", cfg.listen)
        } else {
            cfg.advertise_endpoint.clone()
        };
        let topology = NodeTopology {
            zone: cfg.zone.clone(),
            host: cfg.host.clone(),
        };
        tracing::info!(node_id = %node_id, advertise = %advertise, zone = %topology.zone, host = %topology.host, meta = %cfg.meta_endpoint, "storage joining cluster membership");
        tokio::spawn(run_membership(
            cfg.meta_endpoint.clone(),
            node_id,
            advertise,
            topology,
            cfg.storage.heartbeat_interval_secs,
        ));
    }

    // Local short-circuit read socket (data-locality): co-located compute reads an
    // object's bytes via a passed file descriptor, bypassing the gateway. Off
    // unless configured; best-effort (a bind failure never crashes the node). Held
    // for the node's lifetime so the accept loop keeps running.
    let _local_server = if cfg.local_socket_path.is_empty() {
        None
    } else {
        let reader: Arc<dyn LocalReader> = backend.clone();
        match serve_local_reads(&cfg.local_socket_path, reader) {
            Ok(srv) => {
                tracing::info!(socket = %cfg.local_socket_path, "serving local short-circuit reads");
                Some(srv)
            }
            Err(e) => {
                tracing::warn!(error = %e, socket = %cfg.local_socket_path, "could not bind local-read socket");
                None
            }
        }
    };

    let addr: SocketAddr = cfg.listen.parse()?;
    let serving: Arc<dyn StorageBackend> = backend;
    tracing::info!(
        listen = %addr,
        data_dir = %cfg.data_dir,
        scrub_interval_secs = scrub_secs,
        "soma storage node listening"
    );
    serve_storage(addr, serving).await?;
    Ok(())
}

/// Serve the S3 router and the admin (health/metrics) server.
async fn serve_s3_and_admin(
    cfg: &Config,
    service: S3Service,
    metrics: PrometheusHandle,
    role: &str,
    admin_meta: Option<Arc<dyn MetadataStore>>,
) -> Result<(), BoxError> {
    let ready = Arc::new(AtomicBool::new(false));
    let admin_listener = tokio::net::TcpListener::bind(&cfg.admin_listen).await?;
    let admin_state = AdminState {
        metrics,
        ready: ready.clone(),
        meta: admin_meta,
    };
    tokio::spawn(async move {
        let _ = axum::serve(admin_listener, admin::router(admin_state)).await;
    });

    let listener = tokio::net::TcpListener::bind(&cfg.listen).await?;
    ready.store(true, Ordering::Relaxed);
    tracing::info!(
        role,
        listen = %cfg.listen,
        admin_listen = %cfg.admin_listen,
        credentials = cfg.credentials.len(),
        cache_enabled = cfg.cache.enabled,
        encryption_available = !cfg.encryption.master_key.is_empty(),
        "soma-server listening"
    );

    axum::serve(listener, router(service))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

/// Resolve the config file path from `--config <path>` / `--config=<path>`, or
/// the `SOMA_CONFIG` environment variable.
fn config_path() -> Option<String> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--config" {
            return args.next();
        }
        if let Some(p) = arg.strip_prefix("--config=") {
            return Some(p.to_string());
        }
    }
    std::env::var("SOMA_CONFIG").ok()
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}
