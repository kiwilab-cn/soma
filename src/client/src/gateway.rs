//! The gateway-backed [`Remote`]: `?location` over HTTP for placement, and a
//! signed S3 GET for the bytes. Blocking (uses `ureq`).

use std::io::Read;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;

use crate::sigv4::{encode_path_segment, sign_get};
use crate::{Error, Located, Remote, Result};

/// Reads from a soma gateway over HTTP.
pub struct GatewayRemote {
    /// Base URL with no trailing slash, e.g. `http://gateway:9000`.
    endpoint: String,
    /// Authority (`host:port`) for the `Host` header and signing.
    host: String,
    access_key: String,
    secret_key: String,
    region: String,
}

impl GatewayRemote {
    /// Build over a gateway base URL and credentials.
    pub fn new(endpoint: String, access_key: String, secret_key: String, region: String) -> Self {
        let endpoint = endpoint.trim_end_matches('/').to_string();
        let host = authority_of(&endpoint);
        Self {
            endpoint,
            host,
            access_key,
            secret_key,
            region,
        }
    }

    /// Issue a signed GET to `path` (+ optional raw `query`), returning the
    /// response on `2xx`. `Ok(None)` for a 404/501 (caller decides what that
    /// means); other statuses and transport errors are `Err`.
    fn signed_get(&self, path: &str, query: &str) -> Result<Option<ureq::Response>> {
        let signed = sign_get(
            &self.host,
            path,
            query,
            &self.access_key,
            &self.secret_key,
            &self.region,
            now_unix(),
        );
        let url = if query.is_empty() {
            format!("{}{}", self.endpoint, path)
        } else {
            format!("{}{}?{}", self.endpoint, path, query)
        };
        let resp = ureq::get(&url)
            .set("x-amz-date", &signed.amz_date)
            .set("x-amz-content-sha256", &signed.content_sha256)
            .set("Authorization", &signed.authorization)
            .call();
        match resp {
            Ok(r) => Ok(Some(r)),
            Err(ureq::Error::Status(404, _)) | Err(ureq::Error::Status(501, _)) => Ok(None),
            Err(ureq::Error::Status(code, r)) => Err(Error::Gateway(format!(
                "status {code}: {}",
                r.into_string().unwrap_or_default()
            ))),
            Err(e) => Err(Error::Gateway(e.to_string())),
        }
    }
}

impl Remote for GatewayRemote {
    fn locate(&self, bucket: &str, key: &str) -> Result<Option<Located>> {
        let path = object_path(bucket, key);
        // Best-effort: any failure → no locality info, so the caller reads remotely.
        let resp = match self.signed_get(&path, "location") {
            Ok(Some(r)) => r,
            Ok(None) => return Ok(None),
            Err(e) => {
                tracing::debug!(error = %e, "?location request failed");
                return Ok(None);
            }
        };
        let doc: LocationDoc = match serde_json::from_reader(resp.into_reader()) {
            Ok(d) => d,
            Err(e) => {
                tracing::debug!(error = %e, "?location response was not valid JSON");
                return Ok(None);
            }
        };
        let hosts = doc
            .nodes
            .into_iter()
            .map(|n| n.host)
            .filter(|h| !h.is_empty())
            .collect();
        Ok(Some(Located {
            object_id: doc.object_id,
            size: doc.size,
            hosts,
        }))
    }

    fn get(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
        let path = object_path(bucket, key);
        match self.signed_get(&path, "")? {
            Some(resp) => {
                let mut buf = Vec::new();
                resp.into_reader()
                    .read_to_end(&mut buf)
                    .map_err(Error::Io)?;
                Ok(buf)
            }
            None => Err(Error::NotFound),
        }
    }
}

/// The request path for an object (`/bucket/encoded-key`).
fn object_path(bucket: &str, key: &str) -> String {
    format!("/{}/{}", bucket, encode_path_segment(key))
}

/// Strip the scheme and any path from a base URL, leaving `host:port`.
fn authority_of(endpoint: &str) -> String {
    let no_scheme = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);
    no_scheme
        .split('/')
        .next()
        .unwrap_or(no_scheme)
        .to_string()
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The `?location` JSON document (a subset — extra fields are ignored).
#[derive(Debug, Deserialize)]
struct LocationDoc {
    object_id: u64,
    size: u64,
    nodes: Vec<NodeDoc>,
}

#[derive(Debug, Deserialize)]
struct NodeDoc {
    #[serde(default)]
    host: String,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn authority_is_extracted() {
        assert_eq!(authority_of("http://127.0.0.1:9000"), "127.0.0.1:9000");
        assert_eq!(authority_of("https://gw:9000/"), "gw:9000");
        assert_eq!(authority_of("http://gw:9000/base"), "gw:9000");
    }

    #[test]
    fn object_path_encodes_key() {
        assert_eq!(object_path("b", "a/b c"), "/b/a/b%20c");
        assert_eq!(object_path("b", "plain"), "/b/plain");
    }

    #[test]
    fn location_doc_parses_and_ignores_extra() {
        let json = r#"{"key":"k","object_id":42,"size":5,
            "layout":{"type":"replicated","width":1},
            "nodes":[{"node_id":"n0","endpoint":"e","zone":"z","host":"h0","role":"replica"}]}"#;
        let doc: LocationDoc = serde_json::from_str(json).unwrap();
        assert_eq!(doc.object_id, 42);
        assert_eq!(doc.size, 5);
        assert_eq!(doc.nodes.len(), 1);
        assert_eq!(doc.nodes[0].host, "h0");
    }
}
