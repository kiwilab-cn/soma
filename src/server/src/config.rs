//! Server configuration, loaded via `figment` in precedence order
//! **defaults → TOML file → environment** (`SOMA_*`, nested keys split on `__`).
//!
//! Human-readable byte sizes (e.g. `"4GiB"`, `"512MiB"`) are stored as strings and
//! parsed with `bytesize` at use time, so they round-trip cleanly through the
//! config layers; accessor methods return resolved byte counts.

use figment::providers::{Env, Format, Serialized, Toml};
use figment::Figment;
use serde::{Deserialize, Serialize};
use soma_s3::TenantPolicy;

/// Default S3 listen address.
const DEFAULT_LISTEN: &str = "0.0.0.0:9000";
/// Default admin (health + metrics) listen address.
const DEFAULT_ADMIN_LISTEN: &str = "0.0.0.0:9001";
/// Default data directory.
const DEFAULT_DATA_DIR: &str = "./soma-data";
/// Default volume rotation size (bytes) — used as the infallible fallback for the
/// accessor; the string form is validated at load time.
const DEFAULT_VOLUME_MAX: u64 = 4 * 1024 * 1024 * 1024;
/// Default read-cache capacity (bytes).
const DEFAULT_CACHE_MAX: u64 = 512 * 1024 * 1024;
/// Default maximum cacheable object size (bytes).
const DEFAULT_CACHE_OBJECT_MAX: u64 = 1024 * 1024;

/// Configuration errors.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Loading or merging the configuration failed. Boxed because `figment::Error`
    /// is large (clippy `result_large_err`).
    #[error(transparent)]
    Figment(Box<figment::Error>),

    /// A human-readable byte size could not be parsed.
    #[error("invalid size for {field}: '{value}' ({reason})")]
    BadSize {
        /// Dotted field path.
        field: &'static str,
        /// The offending value.
        value: String,
        /// Why it failed.
        reason: String,
    },
}

/// Top-level server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Process role: `standalone` (default, single process), `gateway`, `meta`,
    /// or `storage`.
    pub role: String,
    /// Listen address — S3 for `gateway`/`standalone`, gRPC for `meta`/`storage`.
    pub listen: String,
    /// Admin (health + metrics) listen address (gateway/standalone).
    pub admin_listen: String,
    /// Data directory (volumes + metadata).
    pub data_dir: String,
    /// Gateway → metadata node endpoint (e.g. `http://meta:9100`). Also where a
    /// storage node registers its membership.
    pub meta_endpoint: String,
    /// Gateway → storage node endpoints (e.g. `["http://storage-0:9200", ...]`).
    pub storage_endpoints: Vec<String>,
    /// Stable node identity for the storage role (e.g. the StatefulSet pod name).
    /// Empty defaults to the listen address.
    pub node_id: String,
    /// The address other nodes reach this storage node at (e.g.
    /// `http://soma-storage-0…:9200`). Empty defaults to `http://{listen}`.
    pub advertise_endpoint: String,
    /// Number of replicas per object.
    pub replication_factor: usize,
    /// Replicas that must durably ack a write to succeed.
    pub write_quorum: usize,
    /// Gateway: how often (seconds) to refresh placement (membership + PG table)
    /// so node joins and migrations are picked up live.
    pub placement_refresh_secs: u64,
    /// Rebalance controller tuning (meta role).
    pub rebalance: RebalanceConfig,
    /// Storage tuning.
    pub storage: StorageConfig,
    /// Read-cache tuning.
    pub cache: CacheConfig,
    /// Erasure-coding tuning. When enabled, the gateway stripes objects with
    /// Reed-Solomon `k+m` instead of N-way replication.
    pub erasure: ErasureConfig,
    /// Encryption-at-rest tuning.
    pub encryption: EncryptionConfig,
    /// Static access credentials.
    pub credentials: Vec<Credential>,
    /// Per-tenant QoS (quotas + rate limits), keyed by access key. Empty = none.
    pub tenants: Vec<TenantConfig>,
}

/// Per-tenant QoS limits. A tenant is identified by its access key; any limit set
/// to zero/empty is unlimited.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TenantConfig {
    /// The access key this policy applies to.
    pub access_key: String,
    /// Max total live bytes (human-readable, e.g. `"10GiB"`; empty = unlimited).
    pub max_bytes: String,
    /// Max live object count (0 = unlimited).
    pub max_objects: u64,
    /// Sustained request rate per second (0 = no rate limit).
    pub rate_limit_rps: f64,
    /// Token-bucket burst capacity in requests (defaults to `rate_limit_rps`).
    pub rate_limit_burst: f64,
}

/// Rebalance controller tuning (meta role). The mover is throttled so migration
/// uses spare bandwidth and never disturbs foreground throughput.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RebalanceConfig {
    /// Whether the controller runs (meta role).
    pub enabled: bool,
    /// Reconcile interval in seconds.
    pub interval_secs: u64,
    /// Minimum seconds a PG migrates before it may finalize (≥ the gateway
    /// `placement_refresh_secs`, so all gateways are dual-writing first).
    pub settle_secs: u64,
    /// Max object copies the mover performs per reconcile pass (the throttle).
    pub max_copies_per_pass: usize,
    /// Seconds without a heartbeat before a node is marked `Down` (triggering
    /// re-replication of its placement groups).
    pub down_after_secs: u64,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 30,
            settle_secs: 30,
            max_copies_per_pass: 64,
            down_after_secs: 90,
        }
    }
}

/// Storage tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// Rotate to a new volume past this size (human-readable, e.g. `"4GiB"`).
    pub volume_max: String,
    /// Bitrot scrub interval in seconds for the storage role (0 disables).
    pub scrub_interval_secs: u64,
    /// Membership heartbeat interval in seconds for the storage role (0 disables
    /// registration).
    pub heartbeat_interval_secs: u64,
    /// Volume compaction interval in seconds for the storage role (0 disables).
    pub compact_interval_secs: u64,
    /// Only compact a volume when at least this fraction of it is reclaimable.
    pub compact_min_reclaim_ratio: f64,
}

/// Erasure-coding tuning. Opt-in: when `enabled`, the gateway stripes each object
/// into `data_shards` data + `parity_shards` parity shards across distinct nodes
/// (it then needs at least `data_shards + parity_shards` storage endpoints).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ErasureConfig {
    /// Whether erasure coding replaces replication on the gateway.
    pub enabled: bool,
    /// Number of data shards (`k`).
    pub data_shards: usize,
    /// Number of parity shards (`m`); the object survives up to `m` node losses.
    pub parity_shards: usize,
    /// Shard writes that must ack for a write to succeed. `0` defaults to
    /// `data_shards + 1`; clamped to `[data_shards, data_shards + parity_shards]`.
    pub write_quorum: usize,
}

impl Default for ErasureConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            data_shards: 4,
            parity_shards: 2,
            write_quorum: 0,
        }
    }
}

/// Encryption-at-rest tuning. Opt-in: when `enabled`, the gateway/standalone
/// backend is wrapped in envelope encryption under `master_key`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct EncryptionConfig {
    /// Whether envelope encryption at rest is enabled.
    pub enabled: bool,
    /// Base64-encoded 32-byte master key (KEK). Typically injected from a
    /// Kubernetes Secret via `SOMA_MASTER_KEY`; keep it out of plaintext config.
    pub master_key: String,
}

/// Read-cache tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CacheConfig {
    /// Whether the in-memory read cache is enabled.
    pub enabled: bool,
    /// Total cache capacity (human-readable, e.g. `"512MiB"`).
    pub max_bytes: String,
    /// Objects larger than this bypass the cache (human-readable, e.g. `"1MiB"`).
    pub max_object_bytes: String,
}

/// A static access-key / secret-key pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credential {
    /// Access key id.
    pub access_key: String,
    /// Secret key.
    pub secret_key: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            role: "standalone".to_string(),
            listen: DEFAULT_LISTEN.to_string(),
            admin_listen: DEFAULT_ADMIN_LISTEN.to_string(),
            data_dir: DEFAULT_DATA_DIR.to_string(),
            meta_endpoint: "http://127.0.0.1:9100".to_string(),
            storage_endpoints: vec!["http://127.0.0.1:9200".to_string()],
            node_id: String::new(),
            advertise_endpoint: String::new(),
            replication_factor: 3,
            write_quorum: 2,
            placement_refresh_secs: 10,
            rebalance: RebalanceConfig::default(),
            storage: StorageConfig::default(),
            cache: CacheConfig::default(),
            erasure: ErasureConfig::default(),
            encryption: EncryptionConfig::default(),
            credentials: vec![Credential {
                access_key: "soma".to_string(),
                secret_key: "soma-secret".to_string(),
            }],
            tenants: Vec::new(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            volume_max: "4GiB".to_string(),
            scrub_interval_secs: 3600,
            heartbeat_interval_secs: 10,
            compact_interval_secs: 3600,
            compact_min_reclaim_ratio: 0.2,
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_bytes: "512MiB".to_string(),
            max_object_bytes: "1MiB".to_string(),
        }
    }
}

impl Config {
    /// Load configuration: defaults, then an optional TOML file, then `SOMA_*`
    /// environment overrides. A single credential pair may also be supplied via
    /// `SOMA_ACCESS_KEY` / `SOMA_SECRET_KEY` (e.g. from a Kubernetes Secret).
    pub fn load(config_path: Option<&str>) -> Result<Self, ConfigError> {
        let mut fig = Figment::from(Serialized::defaults(Config::default()));
        if let Some(path) = config_path {
            fig = fig.merge(Toml::file(path));
        }
        fig = fig.merge(Env::prefixed("SOMA_").split("__"));
        let mut cfg: Config = fig.extract()?;

        // Convenience single-credential override from the environment.
        if let (Ok(ak), Ok(sk)) = (env_var("SOMA_ACCESS_KEY"), env_var("SOMA_SECRET_KEY")) {
            cfg.credentials = vec![Credential {
                access_key: ak,
                secret_key: sk,
            }];
        }

        // Convenience master-key override (e.g. mounted from a Kubernetes Secret).
        if let Ok(mk) = env_var("SOMA_MASTER_KEY") {
            cfg.encryption.master_key = mk;
        }

        // Validate sizes eagerly so a bad value fails fast at startup.
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        parse_size("storage.volume_max", &self.storage.volume_max)?;
        parse_size("cache.max_bytes", &self.cache.max_bytes)?;
        parse_size("cache.max_object_bytes", &self.cache.max_object_bytes)?;
        for t in &self.tenants {
            if !t.max_bytes.is_empty() {
                parse_size("tenants.max_bytes", &t.max_bytes)?;
            }
        }
        Ok(())
    }

    /// Resolve the per-tenant QoS limits into a `(access_key → TenantPolicy)` map.
    pub fn tenant_policies(&self) -> std::collections::HashMap<String, TenantPolicy> {
        self.tenants
            .iter()
            .map(|t| {
                let max_bytes = if t.max_bytes.is_empty() {
                    0
                } else {
                    parse_size("tenants.max_bytes", &t.max_bytes).unwrap_or(0)
                };
                let burst = if t.rate_limit_burst > 0.0 {
                    t.rate_limit_burst
                } else {
                    t.rate_limit_rps
                };
                (
                    t.access_key.clone(),
                    TenantPolicy {
                        max_bytes,
                        max_objects: t.max_objects,
                        rps: t.rate_limit_rps,
                        burst,
                    },
                )
            })
            .collect()
    }

    /// Resolved volume rotation size in bytes.
    pub fn volume_max_bytes(&self) -> u64 {
        parse_size("storage.volume_max", &self.storage.volume_max).unwrap_or(DEFAULT_VOLUME_MAX)
    }

    /// Resolved cache capacity in bytes.
    pub fn cache_max_bytes(&self) -> u64 {
        parse_size("cache.max_bytes", &self.cache.max_bytes).unwrap_or(DEFAULT_CACHE_MAX)
    }

    /// Resolved maximum cacheable object size in bytes.
    pub fn cache_max_object_bytes(&self) -> u64 {
        parse_size("cache.max_object_bytes", &self.cache.max_object_bytes)
            .unwrap_or(DEFAULT_CACHE_OBJECT_MAX)
    }
}

/// Parse a human-readable size into bytes, tagging errors with the field path.
fn parse_size(field: &'static str, value: &str) -> Result<u64, ConfigError> {
    value
        .parse::<bytesize::ByteSize>()
        .map(|b| b.as_u64())
        .map_err(|reason| ConfigError::BadSize {
            field,
            value: value.to_string(),
            reason,
        })
}

impl From<figment::Error> for ConfigError {
    fn from(e: figment::Error) -> Self {
        ConfigError::Figment(Box::new(e))
    }
}

/// Read an environment variable as a `Result` (so it composes in `if let`).
fn env_var(key: &str) -> Result<String, std::env::VarError> {
    std::env::var(key)
}

#[cfg(test)]
mod tests {
    // `result_large_err`: the figment::Jail closure must return figment::Error.
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::result_large_err
    )]
    use super::*;

    #[test]
    fn defaults_are_sane() {
        // Run under a Jail so this is isolated from (and serialized with) the
        // other env-mutating tests in this binary.
        figment::Jail::expect_with(|_jail| {
            let cfg = Config::load(None).unwrap();
            assert_eq!(cfg.listen, DEFAULT_LISTEN);
            assert_eq!(cfg.admin_listen, DEFAULT_ADMIN_LISTEN);
            assert_eq!(cfg.volume_max_bytes(), DEFAULT_VOLUME_MAX);
            assert_eq!(cfg.cache_max_bytes(), DEFAULT_CACHE_MAX);
            assert!(cfg.cache.enabled);
            assert_eq!(cfg.credentials.len(), 1);
            assert_eq!(cfg.credentials[0].access_key, "soma");
            Ok(())
        });
    }

    #[test]
    fn file_then_env_override() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "soma.toml",
                r#"
                listen = "127.0.0.1:7000"
                data_dir = "/data/soma"
                [cache]
                max_bytes = "1GiB"
                "#,
            )?;
            // File wins over defaults.
            let cfg = Config::load(Some("soma.toml")).unwrap();
            assert_eq!(cfg.listen, "127.0.0.1:7000");
            assert_eq!(cfg.data_dir, "/data/soma");
            assert_eq!(cfg.cache_max_bytes(), 1024 * 1024 * 1024);

            // Env wins over the file.
            jail.set_env("SOMA_LISTEN", "0.0.0.0:8080");
            jail.set_env("SOMA_CACHE__MAX_BYTES", "2GiB");
            let cfg = Config::load(Some("soma.toml")).unwrap();
            assert_eq!(cfg.listen, "0.0.0.0:8080");
            assert_eq!(cfg.cache_max_bytes(), 2 * 1024 * 1024 * 1024);
            assert_eq!(cfg.data_dir, "/data/soma"); // still from the file
            Ok(())
        });
    }

    #[test]
    fn credentials_from_env() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("SOMA_ACCESS_KEY", "myak");
            jail.set_env("SOMA_SECRET_KEY", "mysk");
            let cfg = Config::load(None).unwrap();
            assert_eq!(cfg.credentials.len(), 1);
            assert_eq!(cfg.credentials[0].access_key, "myak");
            assert_eq!(cfg.credentials[0].secret_key, "mysk");
            Ok(())
        });
    }

    #[test]
    fn bad_size_is_rejected() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("SOMA_STORAGE__VOLUME_MAX", "not-a-size");
            let err = Config::load(None).unwrap_err();
            assert!(matches!(err, ConfigError::BadSize { .. }));
            Ok(())
        });
    }
}
