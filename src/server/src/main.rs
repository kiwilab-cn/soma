//! Soma server entry point. One binary, four roles (`--role` / config `role`):
//! `standalone` (single process, the M0/M1 behavior), `gateway` (stateless S3
//! front-end), `meta` (metadata gRPC node), and `storage` (storage gRPC node).

mod admin;
mod config;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use soma_backend::{BackendConfig, CachingBackend, LocalFsBackend, StorageBackend};
use soma_cluster::{serve_meta, serve_storage, MetaClient, ReplicatedBackend};
use soma_meta::{MetadataStore, RedbMetaStore};
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
    std::fs::create_dir_all(&cfg.data_dir)?;

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
    let path = format!("{}/meta.redb", cfg.data_dir);
    Ok(Arc::new(RedbMetaStore::open(&path)?))
}

/// Open the local storage backend (no cache — caching lives on the gateway).
fn open_backend(cfg: &Config) -> Result<Arc<dyn StorageBackend>, BoxError> {
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

// --- roles -----------------------------------------------------------------

/// Single process: metadata + storage + S3 + admin in one.
async fn run_standalone(cfg: Config) -> Result<(), BoxError> {
    let metrics = PrometheusBuilder::new().install_recorder()?;
    let meta = open_meta(&cfg)?;
    let backend = maybe_cache(&cfg, open_backend(&cfg)?);
    let service = S3Service::new(meta, backend, build_credentials(&cfg));
    serve_s3_and_admin(&cfg, service, metrics, "standalone").await
}

/// Stateless gateway: S3 front-end over remote metadata + storage nodes.
async fn run_gateway(cfg: Config) -> Result<(), BoxError> {
    let metrics = PrometheusBuilder::new().install_recorder()?;
    let meta: Arc<dyn MetadataStore> =
        Arc::new(MetaClient::connect(cfg.meta_endpoint.clone()).await?);
    let storage: Arc<dyn StorageBackend> = Arc::new(
        ReplicatedBackend::connect(
            cfg.storage_endpoints.clone(),
            cfg.replication_factor,
            cfg.write_quorum,
        )
        .await?,
    );
    let backend = maybe_cache(&cfg, storage);
    let service = S3Service::new(meta, backend, build_credentials(&cfg));
    tracing::info!(
        meta = %cfg.meta_endpoint,
        storage_nodes = cfg.storage_endpoints.len(),
        replication_factor = cfg.replication_factor,
        write_quorum = cfg.write_quorum,
        "gateway connected to cluster"
    );
    serve_s3_and_admin(&cfg, service, metrics, "gateway").await
}

/// Metadata node: serves `MetadataStore` over gRPC.
async fn run_meta(cfg: Config) -> Result<(), BoxError> {
    let store = open_meta(&cfg)?;
    let addr: SocketAddr = cfg.listen.parse()?;
    tracing::info!(listen = %addr, data_dir = %cfg.data_dir, "soma meta node listening");
    serve_meta(addr, store).await?;
    Ok(())
}

/// Storage node: serves `StorageBackend` over gRPC.
async fn run_storage(cfg: Config) -> Result<(), BoxError> {
    let backend = open_backend(&cfg)?;
    let addr: SocketAddr = cfg.listen.parse()?;
    tracing::info!(listen = %addr, data_dir = %cfg.data_dir, "soma storage node listening");
    serve_storage(addr, backend).await?;
    Ok(())
}

/// Serve the S3 router and the admin (health/metrics) server.
async fn serve_s3_and_admin(
    cfg: &Config,
    service: S3Service,
    metrics: PrometheusHandle,
    role: &str,
) -> Result<(), BoxError> {
    let ready = Arc::new(AtomicBool::new(false));
    let admin_listener = tokio::net::TcpListener::bind(&cfg.admin_listen).await?;
    let admin_state = AdminState {
        metrics,
        ready: ready.clone(),
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
