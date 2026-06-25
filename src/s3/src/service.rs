//! `S3Service`: maps S3 operations onto the metadata store and storage backend.
//!
//! The stores are synchronous (fsync-bound IO); each operation runs its blocking
//! work on a `spawn_blocking` thread so the async HTTP layer is never blocked.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::http::{header, HeaderMap};
use bytes::Bytes;
use md5::{Digest, Md5};
use parking_lot::Mutex;

use soma_backend::{ByteRange, Crypto, StorageBackend};
use soma_meta::{
    BucketMeta, BucketOpts, BucketUsage, ETag, ListRequest, ListResult, LocationOracle,
    MetadataStore, ObjectLocations, ObjectPut, PutCondition, Quota, RateLimit, SseAlgorithm, Version,
};

use crate::error::{S3Error, S3Result};
use crate::qos::RateLimiter;
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

/// One in-progress part of a multipart upload (already written to the backend).
struct PartInfo {
    /// Object id of the part's needle.
    object_id: u64,
    /// Raw 16-byte MD5 of the part (for the final multipart ETag).
    md5: [u8; 16],
}

/// In-progress multipart upload state. Held in memory only — incomplete uploads
/// are ephemeral and do not survive a restart (acceptable for M0).
struct MultipartUpload {
    bucket: String,
    key: String,
    parts: BTreeMap<u32, PartInfo>,
    /// Whether parts (and the assembled object) are encrypted (decided at create
    /// from the bucket's default SSE / the request).
    encrypt: bool,
}

/// The S3 service: shared metadata store, storage backend, and credentials.
#[derive(Clone)]
pub struct S3Service {
    meta: Arc<dyn MetadataStore>,
    backend: Arc<dyn StorageBackend>,
    creds: Arc<Credentials>,
    /// Per-bucket request rate limiter (token-bucket state; limits come from each
    /// bucket's metadata). Quotas are enforced in the metadata transaction.
    rate_limiter: Arc<RateLimiter>,
    /// Object crypto for server-side encryption (`None` if no master key is
    /// configured — encrypted buckets then can't be created or read).
    crypto: Option<Arc<Crypto>>,
    /// Data-locality oracle (`None` in single-node deployments, where there is
    /// nothing to schedule across). Answers "which nodes hold object X".
    oracle: Option<Arc<dyn LocationOracle>>,
    /// Active multipart uploads, keyed by upload id.
    uploads: Arc<Mutex<HashMap<String, MultipartUpload>>>,
    /// Monotonic source of upload ids (unique within this process).
    upload_seq: Arc<AtomicU64>,
}

/// Result of a successful `PutObject`.
pub struct PutObjectOk {
    /// The object's ETag (hex, unquoted).
    pub etag: String,
    /// The new version.
    pub version: Version,
    /// Whether the object was stored encrypted (drives the SSE response header).
    pub encrypted: bool,
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
    /// Whether the object is encrypted (drives the SSE response header).
    pub encrypted: bool,
}

/// Result of a successful `HeadObject`.
pub struct HeadObjectOk {
    /// ETag (hex, unquoted).
    pub etag: String,
    /// Object size.
    pub size: u64,
    /// Creation time (unix seconds).
    pub created_at: u64,
    /// Whether the object is encrypted (drives the SSE response header).
    pub encrypted: bool,
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
            rate_limiter: Arc::new(RateLimiter::new()),
            crypto: None,
            oracle: None,
            uploads: Arc::new(Mutex::new(HashMap::new())),
            upload_seq: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Attach a data-locality oracle so `GET object?location` reports the nodes
    /// holding each object (for co-located compute scheduling).
    pub fn with_oracle(mut self, oracle: Arc<dyn LocationOracle>) -> Self {
        self.oracle = Some(oracle);
        self
    }

    /// Attach object crypto, enabling server-side encryption for buckets that
    /// request it.
    pub fn with_crypto(mut self, crypto: Crypto) -> Self {
        self.crypto = Some(Arc::new(crypto));
        self
    }

    /// Set (or clear) a bucket's default server-side encryption.
    pub async fn set_bucket_encryption(
        &self,
        bucket: String,
        algo: Option<SseAlgorithm>,
    ) -> S3Result<()> {
        if algo.is_some() && self.crypto.is_none() {
            return Err(S3Error::not_implemented(
                "server-side encryption is not available (no master key configured)",
            ));
        }
        let meta = self.meta.clone();
        block(move || {
            meta.set_bucket_encryption(&bucket, algo)?;
            Ok(())
        })
        .await
    }

    /// A bucket's default SSE algorithm, if any.
    pub async fn bucket_encryption(&self, bucket: String) -> S3Result<Option<SseAlgorithm>> {
        let meta = self.meta.clone();
        block(move || {
            let b = meta
                .get_bucket(&bucket)?
                .ok_or_else(|| S3Error::no_such_bucket(&bucket))?;
            Ok(b.default_sse)
        })
        .await
    }

    /// Set a bucket's storage quota (zeros = unlimited). Errors if absent.
    pub async fn set_bucket_quota(&self, bucket: String, quota: Quota) -> S3Result<()> {
        let meta = self.meta.clone();
        block(move || {
            meta.set_bucket_quota(&bucket, quota)?;
            Ok(())
        })
        .await
    }

    /// Set a bucket's request rate limit (zero rps = unlimited). Errors if absent.
    pub async fn set_bucket_rate_limit(&self, bucket: String, limit: RateLimit) -> S3Result<()> {
        let meta = self.meta.clone();
        block(move || {
            meta.set_bucket_rate_limit(&bucket, limit)?;
            Ok(())
        })
        .await
    }

    /// A bucket's configured quota and rate limit, plus its current live usage.
    /// Errors if the bucket is absent.
    pub async fn bucket_qos(&self, bucket: String) -> S3Result<(Quota, RateLimit, BucketUsage)> {
        let meta = self.meta.clone();
        block(move || {
            let b = meta
                .get_bucket(&bucket)?
                .ok_or_else(|| S3Error::no_such_bucket(&bucket))?;
            let usage = meta.bucket_usage(&bucket)?;
            Ok((b.quota, b.rate_limit, usage))
        })
        .await
    }

    /// Resolve where an object's bytes physically live (the nodes + topology), for
    /// data-locality scheduling. `Ok(None)` means this deployment has no locality
    /// oracle (single-node); errors propagate `NoSuchBucket`/`NoSuchKey`.
    pub async fn object_locations(
        &self,
        bucket: String,
        key: String,
    ) -> S3Result<Option<ObjectLocations>> {
        let Some(oracle) = self.oracle.clone() else {
            return Ok(None);
        };
        let meta = self.meta.clone();
        block(move || {
            let obj = meta
                .get_object(&bucket, &key)?
                .ok_or_else(|| S3Error::no_such_key(&key))?;
            // None here means the placement group is momentarily unresolvable (no
            // live nodes) — surface it as a transient internal error.
            oracle
                .locate(obj.object_id, obj.size)
                .map(Some)
                .ok_or_else(|| S3Error::internal("object placement is currently unresolvable"))
        })
        .await
    }

    /// Verify the request's SigV4 signature, returning the authenticated access
    /// key (the tenant). Pure CPU; no IO.
    pub fn authorize(
        &self,
        method: &str,
        path: &str,
        query: &str,
        headers: &HeaderMap,
    ) -> S3Result<String> {
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
        Ok(auth.access_key)
    }

    /// Enforce the bucket's request rate limit (token bucket). Loads the bucket's
    /// configured limit and consumes a token; returns `SlowDown` if over the rate.
    /// Buckets with no configured limit (the default) are always allowed. An absent
    /// bucket is treated as unlimited (the operation itself surfaces `NoSuchBucket`).
    pub async fn check_rate_limit(&self, bucket: &str) -> S3Result<()> {
        let meta = self.meta.clone();
        let bucket_owned = bucket.to_string();
        let limit = block(move || {
            Ok(meta
                .get_bucket(&bucket_owned)?
                .map(|b| b.rate_limit)
                .unwrap_or_default())
        })
        .await?;
        if self.rate_limiter.allow(bucket, limit) {
            Ok(())
        } else {
            Err(S3Error::slow_down())
        }
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

    /// `PutObject`. Stores bytes durably, then commits metadata under `cond`. The
    /// object is encrypted when the request asked for SSE (`request_sse`) or the
    /// bucket has default encryption.
    pub async fn put_object(
        &self,
        bucket: String,
        key: String,
        body: Bytes,
        cond: PutCondition,
        now: u64,
        request_sse: bool,
    ) -> S3Result<PutObjectOk> {
        let meta = self.meta.clone();
        let backend = self.backend.clone();
        let crypto = self.crypto.clone();
        let etag = md5_hex(&body); // ETag is the MD5 of the plaintext
        let etag_stored = etag.clone();
        let size = body.len() as u64; // metadata size is the plaintext length

        let (version, encrypted) = block(move || {
            // Encrypt if the request or the bucket's default encryption asks for it.
            let bucket_sse = meta
                .get_bucket(&bucket)?
                .map(|b| b.default_sse.is_some())
                .unwrap_or(false);
            let encrypt = request_sse || bucket_sse;

            let id = meta.next_object_id()?;
            if encrypt {
                let crypto = crypto
                    .as_ref()
                    .ok_or_else(|| S3Error::internal("encryption unavailable: no master key"))?;
                let frame = crypto.seal(&body)?;
                backend.put(id, &frame)?;
            } else {
                backend.put(id, &body)?;
            }
            let version = meta.put_object(
                &bucket,
                &key,
                ObjectPut {
                    object_id: id,
                    size,
                    etag: ETag(etag_stored),
                    created_at: now,
                    encrypted: encrypt,
                },
                cond,
            )?;
            Ok((version, encrypt))
        })
        .await?;

        Ok(PutObjectOk {
            etag,
            version,
            encrypted,
        })
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
        let crypto = self.crypto.clone();
        block(move || {
            let m = meta
                .get_object(&bucket, &key)?
                .ok_or_else(|| S3Error::no_such_key(&key))?;
            // Decrypt encrypted objects; the metadata size/range are plaintext.
            let crypto = if m.encrypted {
                Some(
                    crypto
                        .as_ref()
                        .ok_or_else(|| S3Error::internal("cannot decrypt: no master key"))?,
                )
            } else {
                None
            };
            let id = m.object_id;
            let (data, content_range) = match range_header {
                None => {
                    let data = match crypto {
                        Some(c) => c.open_full(&backend.get(id, None)?)?,
                        None => backend.get(id, None)?,
                    };
                    (data, None)
                }
                Some(spec) => {
                    let (offset, length) = resolve_range(&spec, m.size)?;
                    let data = match crypto {
                        Some(c) => c.open_range(ByteRange { offset, length }, |off, len| {
                            backend.get(
                                id,
                                Some(ByteRange {
                                    offset: off,
                                    length: len,
                                }),
                            )
                        })?,
                        None => backend.get(id, Some(ByteRange { offset, length }))?,
                    };
                    let cr = format!("bytes {}-{}/{}", offset, offset + length - 1, m.size);
                    (data, Some(cr))
                }
            };
            Ok(GetObjectOk {
                data,
                etag: m.etag.0,
                size: m.size,
                created_at: m.created_at,
                content_range,
                encrypted: m.encrypted,
            })
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
                encrypted: m.encrypted,
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

    /// `CreateMultipartUpload`. Registers an upload and returns its id. Whether the
    /// upload's parts and final object are encrypted is fixed here from the
    /// request's SSE header or the bucket's default encryption.
    pub async fn create_multipart(
        &self,
        bucket: String,
        key: String,
        request_sse: bool,
    ) -> S3Result<String> {
        // The bucket must exist; read its default SSE while we're here.
        let meta = self.meta.clone();
        let b = bucket.clone();
        let bucket_sse =
            block(move || Ok(meta.get_bucket(&b)?.map(|m| m.default_sse.is_some()))).await?;
        let bucket_sse = bucket_sse.ok_or_else(|| S3Error::no_such_bucket(&bucket))?;
        let encrypt = request_sse || bucket_sse;
        if encrypt && self.crypto.is_none() {
            return Err(S3Error::not_implemented(
                "server-side encryption is not available (no master key configured)",
            ));
        }
        let n = self.upload_seq.fetch_add(1, Ordering::Relaxed);
        let upload_id = format!("soma-{n:016x}");
        self.uploads.lock().insert(
            upload_id.clone(),
            MultipartUpload {
                bucket,
                key,
                parts: BTreeMap::new(),
                encrypt,
            },
        );
        Ok(upload_id)
    }

    /// `UploadPart`. Writes the part to the backend and records it; returns its
    /// ETag (hex MD5).
    pub async fn upload_part(
        &self,
        upload_id: String,
        part_number: u32,
        body: Bytes,
    ) -> S3Result<String> {
        let encrypt = match self.uploads.lock().get(&upload_id) {
            Some(up) => up.encrypt,
            None => return Err(S3Error::no_such_upload(&upload_id)),
        };
        let meta = self.meta.clone();
        let backend = self.backend.clone();
        let crypto = self.crypto.clone();
        let md5 = md5_raw(&body); // ETag over the plaintext part
        let etag = hex::encode(md5);
        let object_id = block(move || {
            let id = meta.next_object_id()?;
            if encrypt {
                let crypto = crypto
                    .as_ref()
                    .ok_or_else(|| S3Error::internal("encryption unavailable: no master key"))?;
                backend.put(id, &crypto.seal(&body)?)?;
            } else {
                backend.put(id, &body)?;
            }
            Ok(id)
        })
        .await?;

        match self.uploads.lock().get_mut(&upload_id) {
            Some(up) => {
                up.parts.insert(part_number, PartInfo { object_id, md5 });
            }
            None => return Err(S3Error::no_such_upload(&upload_id)),
        }
        Ok(etag)
    }

    /// `CompleteMultipartUpload`. Assembles the requested parts into one object
    /// and returns the multipart ETag (`md5(concat of part md5s)-N`).
    pub async fn complete_multipart(
        &self,
        bucket: String,
        key: String,
        upload_id: String,
        requested: Vec<(u32, String)>,
        now: u64,
    ) -> S3Result<String> {
        if requested.is_empty() {
            return Err(S3Error::invalid_argument("no parts specified"));
        }
        // Collect the part object ids (in requested order) and their MD5 digests.
        let (part_ids, digests, encrypt) = {
            let reg = self.uploads.lock();
            let up = reg
                .get(&upload_id)
                .ok_or_else(|| S3Error::no_such_upload(&upload_id))?;
            if up.bucket != bucket || up.key != key {
                return Err(S3Error::invalid_argument(
                    "upload id does not match bucket/key",
                ));
            }
            let mut part_ids = Vec::with_capacity(requested.len());
            let mut digests = Vec::with_capacity(requested.len() * 16);
            for (pn, _etag) in &requested {
                let part = up
                    .parts
                    .get(pn)
                    .ok_or_else(|| S3Error::invalid_part(format!("missing part {pn}")))?;
                part_ids.push(part.object_id);
                digests.extend_from_slice(&part.md5);
            }
            (part_ids, digests, up.encrypt)
        };

        let final_etag = format!("{}-{}", md5_hex(&digests), requested.len());

        let meta = self.meta.clone();
        let backend = self.backend.clone();
        let crypto = self.crypto.clone();
        let etag_stored = final_etag.clone();
        let (b, k) = (bucket.clone(), key.clone());
        block(move || {
            // Assemble the plaintext (decrypting each part if the upload is
            // encrypted), then store the final object — re-sealed under a fresh
            // object key so the assembled bytes are a single coherent frame.
            let crypto =
                if encrypt {
                    Some(crypto.as_ref().ok_or_else(|| {
                        S3Error::internal("encryption unavailable: no master key")
                    })?)
                } else {
                    None
                };
            let mut assembled = Vec::new();
            for part_id in &part_ids {
                let raw = backend.get(*part_id, None)?;
                match crypto {
                    Some(c) => assembled.extend_from_slice(&c.open_full(&raw)?),
                    None => assembled.extend_from_slice(&raw),
                }
            }
            let size = assembled.len() as u64; // plaintext length
            let id = meta.next_object_id()?;
            match crypto {
                Some(c) => backend.put(id, &c.seal(&assembled)?)?,
                None => backend.put(id, &assembled)?,
            }
            meta.put_object(
                &b,
                &k,
                ObjectPut {
                    object_id: id,
                    size,
                    etag: ETag(etag_stored),
                    created_at: now,
                    encrypted: encrypt,
                },
                PutCondition::None,
            )?;
            // The part needles are now orphaned (the object has a fresh id) — hand
            // their ids to GC.
            meta.mark_garbage(&part_ids)?;
            Ok(())
        })
        .await?;

        self.uploads.lock().remove(&upload_id);
        Ok(final_etag)
    }

    /// `AbortMultipartUpload`. Discards the upload's state and hands its already-
    /// written part needles to GC.
    pub async fn abort_multipart(&self, upload_id: String) -> S3Result<()> {
        let part_ids: Vec<u64> = match self.uploads.lock().remove(&upload_id) {
            Some(up) => up.parts.values().map(|p| p.object_id).collect(),
            None => return Ok(()),
        };
        if !part_ids.is_empty() {
            let meta = self.meta.clone();
            block(move || {
                meta.mark_garbage(&part_ids)?;
                Ok(())
            })
            .await?;
        }
        Ok(())
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

/// Raw 16-byte MD5 of `data`.
fn md5_raw(data: &[u8]) -> [u8; 16] {
    let mut h = Md5::new();
    h.update(data);
    h.finalize().into()
}

/// Hex-encoded MD5 of `data` (the S3 single-part ETag).
fn md5_hex(data: &[u8]) -> String {
    hex::encode(md5_raw(data))
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
