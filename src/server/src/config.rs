//! Server configuration, loaded via `figment` in precedence order
//! **defaults → TOML file → environment** (`SOMA_*`, nested keys split on `__`).
//!
//! Human-readable byte sizes (e.g. `"4GiB"`, `"512MiB"`) are stored as strings and
//! parsed with `bytesize` at use time, so they round-trip cleanly through the
//! config layers; accessor methods return resolved byte counts.

use figment::providers::{Env, Format, Serialized, Toml};
use figment::Figment;
use serde::{Deserialize, Serialize};

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
    /// S3 endpoint listen address.
    pub listen: String,
    /// Admin (health + metrics) listen address.
    pub admin_listen: String,
    /// Data directory (volumes + metadata).
    pub data_dir: String,
    /// Storage tuning.
    pub storage: StorageConfig,
    /// Read-cache tuning.
    pub cache: CacheConfig,
    /// Static access credentials.
    pub credentials: Vec<Credential>,
}

/// Storage tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// Rotate to a new volume past this size (human-readable, e.g. `"4GiB"`).
    pub volume_max: String,
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
            listen: DEFAULT_LISTEN.to_string(),
            admin_listen: DEFAULT_ADMIN_LISTEN.to_string(),
            data_dir: DEFAULT_DATA_DIR.to_string(),
            storage: StorageConfig::default(),
            cache: CacheConfig::default(),
            credentials: vec![Credential {
                access_key: "soma".to_string(),
                secret_key: "soma-secret".to_string(),
            }],
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            volume_max: "4GiB".to_string(),
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

        // Validate sizes eagerly so a bad value fails fast at startup.
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        parse_size("storage.volume_max", &self.storage.volume_max)?;
        parse_size("cache.max_bytes", &self.cache.max_bytes)?;
        parse_size("cache.max_object_bytes", &self.cache.max_object_bytes)?;
        Ok(())
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
        let cfg = Config::load(None).unwrap();
        assert_eq!(cfg.listen, DEFAULT_LISTEN);
        assert_eq!(cfg.admin_listen, DEFAULT_ADMIN_LISTEN);
        assert_eq!(cfg.volume_max_bytes(), DEFAULT_VOLUME_MAX);
        assert_eq!(cfg.cache_max_bytes(), DEFAULT_CACHE_MAX);
        assert!(cfg.cache.enabled);
        assert_eq!(cfg.credentials.len(), 1);
        assert_eq!(cfg.credentials[0].access_key, "soma");
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
