//! `S3Service`: maps S3 operations onto the metadata store and storage backend.
//!
//! The stores are synchronous (fsync-bound IO); each operation runs its blocking
//! work on a `spawn_blocking` thread so the async HTTP layer is never blocked.

use std::collections::HashMap;
use std::sync::Arc;

use axum::http::{header, HeaderMap};
use bytes::Bytes;
use md5::{Digest, Md5};

use soma_backend::{ByteRange, StorageBackend};
use soma_meta::{
    BucketMeta, BucketOpts, ETag, ListRequest, ListResult, MetadataStore, ObjectPut, PutCondition,
    Version,
};

use crate::error::{S3Error, S3Result};
use crate::sigv4::{self, AuthError};

/// Maps access key ids to secret keys.
#[derive(Debug, Clone, Default)]
pub struct Credentials(HashMap<String, String>);

impl Credentials {
    /// An empty credential set.
    pub fn new() -> Self {
        Self::default()
    }

    /// A credential set with one key pair.
    pub fn single(access_key: impl Into<String>, secret_key: impl Into<String>) -> Self {
        let mut c = Self::new();
        c.add(access_key, secret_key);
        c
    }

    /// Add a key pair.
    pub fn add(&mut self, access_key: impl Into<String>, secret_key: impl Into<String>) {
        self.0.insert(access_key.into(), secret_key.into());
    }

    fn secret(&self, access_key: &str) -> Option<&str> {
        self.0.get(access_key).map(String::as_str)
    }
}

/// The S3 service: shared metadata store, storage backend, and credentials.
#[derive(Clone)]
pub struct S3Service {
    meta: Arc<dyn MetadataStore>,
    backend: Arc<dyn StorageBackend>,
    creds: Arc<Credentials>,
}

/// Result of a successful `PutObject`.
pub struct PutObjectOk {
    /// The object's ETag (hex, unquoted).
    pub etag: String,
    /// The new version.
    pub version: Version,
}

/// Result of a successful `GetObject`.
pub struct GetObjectOk {
    /// The (possibly partial) object bytes.
    pub data: Vec<u8>,
    /// ETag (hex, unquoted).
    pub etag: String,
    /// Full object size.
    pub size: u64,
    /// Creation time (unix seconds).
    pub created_at: u64,
    /// `Content-Range` value when this is a partial (206) response.
    pub content_range: Option<String>,
}

/// Result of a successful `HeadObject`.
pub struct HeadObjectOk {
    /// ETag (hex, unquoted).
    pub etag: String,
    /// Object size.
    pub size: u64,
    /// Creation time (unix seconds).
    pub created_at: u64,
}

impl S3Service {
    /// Construct a service over the given stores and credentials.
    pub fn new(
        meta: Arc<dyn MetadataStore>,
        backend: Arc<dyn StorageBackend>,
        creds: Credentials,
    ) -> Self {
        Self {
            meta,
            backend,
            creds: Arc::new(creds),
        }
    }

    /// Verify the request's SigV4 signature. Pure CPU; no IO.
    pub fn authorize(
        &self,
        method: &str,
        path: &str,
        query: &str,
        headers: &HeaderMap,
    ) -> S3Result<()> {
        let auth_value = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .ok_or(AuthError::Missing)?;
        let auth = sigv4::parse_authorization(auth_value)?;
        let secret = self
            .creds
            .secret(&auth.access_key)
            .ok_or(AuthError::UnknownAccessKey)?;
        let amz_date = header_str(headers, "x-amz-date");
        let payload_hash = headers
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("UNSIGNED-PAYLOAD");
        sigv4::verify(
            method,
            path,
            query,
            headers,
            amz_date,
            payload_hash,
            &auth,
            secret,
        )?;
        Ok(())
    }

    /// `CreateBucket`.
    pub async fn create_bucket(&self, bucket: String) -> S3Result<()> {
        let meta = self.meta.clone();
        block(move || {
            meta.create_bucket(&bucket, BucketOpts::default())?;
            Ok(())
        })
        .await
    }

    /// `DeleteBucket`.
    pub async fn delete_bucket(&self, bucket: String) -> S3Result<()> {
        let meta = self.meta.clone();
        block(move || {
            meta.delete_bucket(&bucket)?;
            Ok(())
        })
        .await
    }

    /// `ListBuckets`.
    pub async fn list_buckets(&self) -> S3Result<Vec<BucketMeta>> {
        let meta = self.meta.clone();
        block(move || Ok(meta.list_buckets()?)).await
    }

    /// `ListObjectsV2`.
    pub async fn list_objects(&self, bucket: String, req: ListRequest) -> S3Result<ListResult> {
        let meta = self.meta.clone();
        block(move || Ok(meta.list_objects(&bucket, &req)?)).await
    }

    /// `PutObject`. Stores bytes durably, then commits metadata under `cond`.
    pub async fn put_object(
        &self,
        bucket: String,
        key: String,
        body: Bytes,
        cond: PutCondition,
        now: u64,
    ) -> S3Result<PutObjectOk> {
        let meta = self.meta.clone();
        let backend = self.backend.clone();
        let etag = md5_hex(&body);
        let etag_stored = etag.clone();
        let size = body.len() as u64;

        let version = block(move || {
            let id = meta.next_object_id()?;
            let location = backend.put(id, &body)?;
            let version = meta.put_object(
                &bucket,
                &key,
                ObjectPut {
                    object_id: id,
                    location,
                    size,
                    etag: ETag(etag_stored),
                    created_at: now,
                },
                cond,
            )?;
            Ok(version)
        })
        .await?;

        Ok(PutObjectOk { etag, version })
    }

    /// `GetObject`, optionally with a `Range` header.
    pub async fn get_object(
        &self,
        bucket: String,
        key: String,
        range_header: Option<String>,
    ) -> S3Result<GetObjectOk> {
        let meta = self.meta.clone();
        let backend = self.backend.clone();
        block(move || {
            let m = meta
                .get_object(&bucket, &key)?
                .ok_or_else(|| S3Error::no_such_key(&key))?;
            match range_header {
                None => {
                    let data = backend.get(m.location, None)?;
                    Ok(GetObjectOk {
                        data,
                        etag: m.etag.0,
                        size: m.size,
                        created_at: m.created_at,
                        content_range: None,
                    })
                }
                Some(spec) => {
                    let (offset, length) = resolve_range(&spec, m.size)?;
                    let data = backend.get(m.location, Some(ByteRange { offset, length }))?;
                    let content_range =
                        format!("bytes {}-{}/{}", offset, offset + length - 1, m.size);
                    Ok(GetObjectOk {
                        data,
                        etag: m.etag.0,
                        size: m.size,
                        created_at: m.created_at,
                        content_range: Some(content_range),
                    })
                }
            }
        })
        .await
    }

    /// `HeadObject`.
    pub async fn head_object(&self, bucket: String, key: String) -> S3Result<HeadObjectOk> {
        let meta = self.meta.clone();
        block(move || {
            let m = meta
                .get_object(&bucket, &key)?
                .ok_or_else(|| S3Error::no_such_key(&key))?;
            Ok(HeadObjectOk {
                etag: m.etag.0,
                size: m.size,
                created_at: m.created_at,
            })
        })
        .await
    }

    /// `DeleteObject`. Idempotent.
    pub async fn delete_object(
        &self,
        bucket: String,
        key: String,
        cond: PutCondition,
    ) -> S3Result<()> {
        let meta = self.meta.clone();
        block(move || {
            meta.delete_object(&bucket, &key, cond)?;
            Ok(())
        })
        .await
    }
}

/// Read a header as a `&str`, defaulting to empty.
fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> &'a str {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

/// Run blocking store work on a dedicated thread.
async fn block<T, F>(f: F) -> S3Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> S3Result<T> + Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(r) => r,
        Err(e) => Err(S3Error::internal(format!("task panicked: {e}"))),
    }
}

/// Hex-encoded MD5 of `data` (the S3 single-part ETag).
fn md5_hex(data: &[u8]) -> String {
    let mut h = Md5::new();
    h.update(data);
    hex::encode(h.finalize())
}

/// Resolve an HTTP `Range` header against the object size into `(offset, length)`.
/// Supports `bytes=a-b`, `bytes=a-`, and `bytes=-n`.
fn resolve_range(spec: &str, size: u64) -> S3Result<(u64, u64)> {
    if size == 0 {
        return Err(S3Error::invalid_range());
    }
    let rng = spec
        .strip_prefix("bytes=")
        .ok_or_else(|| S3Error::invalid_argument("unsupported range unit"))?;
    let (start, end) = rng
        .split_once('-')
        .ok_or_else(|| S3Error::invalid_argument("malformed range"))?;

    let (offset, length) = match (start.trim(), end.trim()) {
        ("", "") => return Err(S3Error::invalid_argument("malformed range")),
        // Suffix: last N bytes.
        ("", n) => {
            let n: u64 = n
                .parse()
                .map_err(|_| S3Error::invalid_argument("bad range"))?;
            if n == 0 {
                return Err(S3Error::invalid_range());
            }
            let n = n.min(size);
            (size - n, n)
        }
        // Open-ended: from A to end.
        (a, "") => {
            let a: u64 = a
                .parse()
                .map_err(|_| S3Error::invalid_argument("bad range"))?;
            if a >= size {
                return Err(S3Error::invalid_range());
            }
            (a, size - a)
        }
        // Closed: A through B inclusive.
        (a, b) => {
            let a: u64 = a
                .parse()
                .map_err(|_| S3Error::invalid_argument("bad range"))?;
            let b: u64 = b
                .parse()
                .map_err(|_| S3Error::invalid_argument("bad range"))?;
            if a > b || a >= size {
                return Err(S3Error::invalid_range());
            }
            let last = b.min(size - 1);
            (a, last - a + 1)
        }
    };
    Ok((offset, length))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn range_closed() {
        assert_eq!(resolve_range("bytes=0-99", 1000).unwrap(), (0, 100));
        assert_eq!(resolve_range("bytes=10-19", 1000).unwrap(), (10, 10));
    }

    #[test]
    fn range_open_ended() {
        assert_eq!(resolve_range("bytes=990-", 1000).unwrap(), (990, 10));
    }

    #[test]
    fn range_suffix() {
        assert_eq!(resolve_range("bytes=-100", 1000).unwrap(), (900, 100));
        // Suffix larger than the object clamps to the whole object.
        assert_eq!(resolve_range("bytes=-5000", 1000).unwrap(), (0, 1000));
    }

    #[test]
    fn range_clamps_end() {
        assert_eq!(resolve_range("bytes=500-100000", 1000).unwrap(), (500, 500));
    }

    #[test]
    fn range_errors() {
        assert!(resolve_range("bytes=1000-1001", 1000).is_err()); // start past end
        assert!(resolve_range("items=0-9", 1000).is_err()); // bad unit
        assert!(resolve_range("bytes=0-9", 0).is_err()); // empty object
    }
}
