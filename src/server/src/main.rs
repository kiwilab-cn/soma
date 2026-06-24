//! Soma server entry point: assembles the S3 protocol layer, the metadata store,
//! and the storage backend into a running single-node S3 endpoint (M0).

use std::sync::Arc;

use soma_backend::{BackendConfig, LocalFsBackend, StorageBackend};
use soma_meta::{MetadataStore, RedbMetaStore};
use soma_s3::{router, Credentials, S3Service};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Server configuration, sourced from the environment with sensible defaults.
struct Config {
    data_dir: String,
    listen: String,
    access_key: String,
    secret_key: String,
}

impl Config {
    fn from_env() -> Self {
        let env = |k: &str, default: &str| std::env::var(k).unwrap_or_else(|_| default.to_string());
        Self {
            data_dir: env("SOMA_DATA_DIR", "./soma-data"),
            listen: env("SOMA_LISTEN", "0.0.0.0:9000"),
            access_key: env("SOMA_ACCESS_KEY", "soma"),
            secret_key: env("SOMA_SECRET_KEY", "soma-secret"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cfg = Config::from_env();

    let meta_path = format!("{}/meta.redb", cfg.data_dir);
    std::fs::create_dir_all(&cfg.data_dir)?;
    let meta: Arc<dyn MetadataStore> = Arc::new(RedbMetaStore::open(&meta_path)?);
    let backend: Arc<dyn StorageBackend> = Arc::new(LocalFsBackend::open(
        &cfg.data_dir,
        BackendConfig::default(),
    )?);

    let creds = Credentials::single(cfg.access_key.clone(), cfg.secret_key);
    let service = S3Service::new(meta, backend, creds);

    let listener = tokio::net::TcpListener::bind(&cfg.listen).await?;
    tracing::info!(
        listen = %cfg.listen,
        data_dir = %cfg.data_dir,
        access_key = %cfg.access_key,
        "soma-server (M0) listening"
    );

    axum::serve(listener, router(service))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}
