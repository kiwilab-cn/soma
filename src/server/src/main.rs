//! Soma server entry point: loads configuration, installs the metrics recorder,
//! starts the admin server, then assembles the S3 protocol layer, the metadata
//! store, and the storage backend into a running single-node S3 endpoint.

mod admin;
mod config;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use metrics_exporter_prometheus::PrometheusBuilder;

use soma_backend::{BackendConfig, CachingBackend, LocalFsBackend, StorageBackend};
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

    // Install the global Prometheus recorder; its handle renders `/metrics`.
    let metrics_handle = PrometheusBuilder::new().install_recorder()?;

    let cfg = Config::load(config_path().as_deref())?;

    std::fs::create_dir_all(&cfg.data_dir)?;
    let meta_path = format!("{}/meta.redb", cfg.data_dir);
    let meta: Arc<dyn MetadataStore> = Arc::new(RedbMetaStore::open(&meta_path)?);
    let fs_backend = Arc::new(LocalFsBackend::open(
        &cfg.data_dir,
        BackendConfig {
            volume_max: cfg.volume_max_bytes(),
        },
    )?);
    let backend: Arc<dyn StorageBackend> = if cfg.cache.enabled {
        Arc::new(CachingBackend::new(
            fs_backend,
            cfg.cache_max_bytes() as usize,
            cfg.cache_max_object_bytes(),
        ))
    } else {
        fs_backend
    };

    let mut creds = Credentials::new();
    for c in &cfg.credentials {
        creds.add(&c.access_key, &c.secret_key);
    }
    let service = S3Service::new(meta, backend, creds);

    // Admin (health + metrics) server on its own port.
    let ready = Arc::new(AtomicBool::new(false));
    let admin_listener = tokio::net::TcpListener::bind(&cfg.admin_listen).await?;
    let admin_state = AdminState {
        metrics: metrics_handle,
        ready: ready.clone(),
    };
    tokio::spawn(async move {
        let _ = axum::serve(admin_listener, admin::router(admin_state)).await;
    });

    let listener = tokio::net::TcpListener::bind(&cfg.listen).await?;
    ready.store(true, Ordering::Relaxed);
    tracing::info!(
        listen = %cfg.listen,
        admin_listen = %cfg.admin_listen,
        data_dir = %cfg.data_dir,
        credentials = cfg.credentials.len(),
        cache_enabled = cfg.cache.enabled,
        cache_max_bytes = cfg.cache_max_bytes(),
        cache_max_object_bytes = cfg.cache_max_object_bytes(),
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
