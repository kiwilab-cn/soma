//! S3-compatible HTTP service for Soma.
//!
//! Exposes a [`router`] that maps the S3 REST API onto an [`S3Service`]. M0
//! covers bucket lifecycle, single-part object CRUD, range reads, `ListObjectsV2`
//! (prefix/delimiter/pagination), conditional writes, and SigV4 auth. Multipart
//! upload is rejected with `NotImplemented` (a follow-up milestone).
//!
//! Routing uses a single fallback handler that dispatches on method + path +
//! query, matching S3's "operation is determined by query parameters" model.

mod error;
mod qos;
mod service;
mod sigv4;
mod xml;

#[cfg(test)]
mod tests;

pub use error::{S3Error, S3Result};
pub use service::{Access, Credentials, GetObjectOk, HeadObjectOk, PutObjectOk, S3Service};

use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Router;
use base64::Engine;
use percent_encoding::percent_decode_str;

use soma_meta::{ETag, ListRequest, PutCondition};

/// Maximum request body Soma will buffer for a single-part `PutObject` (5 GiB,
/// the S3 single-PUT limit).
const MAX_BODY: usize = 5 * 1024 * 1024 * 1024;

/// Build the S3 HTTP router over the given service.
pub fn router(service: S3Service) -> Router {
    Router::new().fallback(handle).with_state(service)
}

async fn handle(State(svc): State<S3Service>, req: Request) -> Response {
    let start = std::time::Instant::now();
    let method_label = req.method().as_str().to_owned();

    let resp = serve_request(svc, req).await;

    metrics::counter!(
        "soma_s3_requests_total",
        "method" => method_label.clone(),
        "status" => resp.status().as_u16().to_string(),
    )
    .increment(1);
    metrics::histogram!("soma_s3_request_duration_seconds", "method" => method_label)
        .record(start.elapsed().as_secs_f64());

    resp
}

async fn serve_request(svc: S3Service, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let method = parts.method;
    let uri = parts.uri;
    let headers = parts.headers;
    let path = uri.path().to_string();
    let query = uri.query().unwrap_or("").to_string();

    // Authenticate; the access key (the principal) gates per-bucket authorization.
    let principal = match svc.authorize(method.as_str(), &path, &query, &headers) {
        Ok(k) => k,
        Err(e) => return e.into_response(),
    };

    match dispatch(&svc, &method, &path, &query, &headers, body, &principal).await {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

async fn dispatch(
    svc: &S3Service,
    method: &Method,
    path: &str,
    query: &str,
    headers: &HeaderMap,
    body: Body,
    principal: &str,
) -> S3Result<Response> {
    let (bucket, key) = split_path(path);
    let (bucket, key) = (bucket.as_str(), key.as_str());
    let q = parse_query(query);

    // Service level: GET / -> ListBuckets (filtered to the principal's buckets).
    if bucket.is_empty() {
        return match *method {
            Method::GET => list_buckets(svc, principal).await,
            _ => Err(S3Error::not_implemented(
                "unsupported service-level operation",
            )),
        };
    }

    // Per-bucket authorization + rate limiting (one metadata read). `CreateBucket`
    // has no pre-check — the creating key becomes the owner (create→own).
    if let Some(access) = required_access(method, key, &q) {
        svc.check_bucket_access(principal, bucket, access).await?;
    }

    // Object level.
    if !key.is_empty() {
        // `?location` sub-resource: report which nodes hold the object's bytes
        // (data-locality oracle, a soma extension — see docs/M4_DESIGN.md).
        if qhas(&q, "location") {
            return match *method {
                Method::GET => get_object_locations(svc, bucket, key).await,
                _ => Err(S3Error::not_implemented("unsupported location operation")),
            };
        }
        // Multipart sub-resources (distinguished by query parameters).
        if *method == Method::POST && qhas(&q, "uploads") {
            return create_multipart(svc, bucket, key, headers).await;
        }
        if let Some(upload_id) = qget(&q, "uploadId") {
            return match *method {
                Method::PUT => upload_part(svc, upload_id, &q, body).await,
                Method::POST => complete_multipart(svc, bucket, key, upload_id, body).await,
                Method::DELETE => {
                    svc.abort_multipart(upload_id.to_string()).await?;
                    Ok(StatusCode::NO_CONTENT.into_response())
                }
                _ => Err(S3Error::not_implemented("unsupported multipart operation")),
            };
        }
        return match *method {
            Method::PUT => put_object(svc, bucket, key, headers, body).await,
            Method::GET => get_object(svc, bucket, key, headers, false).await,
            Method::HEAD => get_object(svc, bucket, key, headers, true).await,
            Method::DELETE => delete_object(svc, bucket, key, headers).await,
            _ => Err(S3Error::not_implemented("unsupported object operation")),
        };
    }

    // Bucket level.
    // `?encryption` sub-resource: default bucket encryption (S3 SSE).
    if qhas(&q, "encryption") {
        return match *method {
            Method::PUT => put_bucket_encryption(svc, bucket, body).await,
            Method::GET => get_bucket_encryption(svc, bucket).await,
            Method::DELETE => {
                svc.set_bucket_encryption(bucket.to_string(), None).await?;
                Ok(StatusCode::NO_CONTENT.into_response())
            }
            _ => Err(S3Error::not_implemented("unsupported encryption operation")),
        };
    }
    match *method {
        Method::PUT => {
            svc.create_bucket(bucket.to_string(), principal.to_string())
                .await?;
            Ok(([(header::LOCATION, format!("/{bucket}"))], StatusCode::OK).into_response())
        }
        Method::DELETE => {
            svc.delete_bucket(bucket.to_string()).await?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        Method::GET => list_objects(svc, bucket, &q).await,
        Method::HEAD => {
            // HeadBucket: 200 if it exists, else 404.
            match svc.list_buckets().await?.iter().any(|b| b.name == bucket) {
                true => Ok(StatusCode::OK.into_response()),
                false => Err(S3Error::no_such_bucket(bucket)),
            }
        }
        _ => Err(S3Error::not_implemented("unsupported bucket operation")),
    }
}

async fn list_buckets(svc: &S3Service, principal: &str) -> S3Result<Response> {
    let buckets = svc.list_buckets().await?;
    // A tenant only sees buckets it may read (its own, public, shared, or unowned).
    let visible: Vec<_> = buckets
        .into_iter()
        .filter(|b| b.can_read(principal))
        .collect();
    let body = xml::list_all_buckets(&visible, now_secs());
    Ok(xml_response(StatusCode::OK, body))
}

/// The access an operation needs on `bucket`, or `None` when there is nothing to
/// pre-authorize (service-level, or `CreateBucket` which is create→own). Mirrors
/// the routing in [`dispatch`].
fn required_access(method: &Method, object_key: &str, q: &[(String, String)]) -> Option<Access> {
    if !object_key.is_empty() {
        // Object level: GET/HEAD read, everything else (PUT/POST/DELETE) writes.
        return Some(match *method {
            Method::GET | Method::HEAD => Access::Read,
            _ => Access::Write,
        });
    }
    // Bucket level.
    if qhas(q, "encryption") {
        return Some(if *method == Method::GET {
            Access::Read
        } else {
            Access::Write
        });
    }
    match *method {
        Method::PUT => None, // CreateBucket — create→own, nothing to pre-authorize
        Method::DELETE => Some(Access::Write),
        Method::GET | Method::HEAD => Some(Access::Read),
        _ => None,
    }
}

async fn list_objects(svc: &S3Service, bucket: &str, q: &[(String, String)]) -> S3Result<Response> {
    let prefix = qget(q, "prefix").unwrap_or("").to_string();
    let delimiter = qget(q, "delimiter")
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let max_keys = qget(q, "max-keys")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);
    let continuation_token = match qget(q, "continuation-token") {
        Some(t) if !t.is_empty() => Some(
            base64::engine::general_purpose::STANDARD
                .decode(t)
                .map_err(|_| S3Error::invalid_argument("bad continuation-token"))?,
        ),
        _ => None,
    };

    let req = ListRequest {
        prefix: prefix.clone(),
        delimiter: delimiter.clone(),
        continuation_token,
        max_keys,
    };
    let result = svc.list_objects(bucket.to_string(), req).await?;

    let next_token = result
        .next_continuation_token
        .as_ref()
        .map(|t| base64::engine::general_purpose::STANDARD.encode(t));

    let body = xml::list_objects_v2(&xml::ListObjectsXml {
        bucket,
        prefix: &prefix,
        delimiter: delimiter.as_deref(),
        max_keys,
        is_truncated: result.is_truncated,
        next_token: next_token.as_deref(),
        objects: &result.objects,
        common_prefixes: &result.common_prefixes,
    });
    Ok(xml_response(StatusCode::OK, body))
}

/// `GET object?location` — report the nodes holding the object's bytes and their
/// topology, as a soma-specific JSON document (HDFS `getFileBlockLocations`
/// analogue). Returns `501` when this deployment has no locality oracle (single
/// node, nothing to schedule across).
async fn get_object_locations(svc: &S3Service, bucket: &str, key: &str) -> S3Result<Response> {
    let Some(loc) = svc
        .object_locations(bucket.to_string(), key.to_string())
        .await?
    else {
        return Err(S3Error::not_implemented(
            "object location API requires a clustered deployment",
        ));
    };
    Ok(json_response(StatusCode::OK, render_locations(key, &loc)))
}

/// Render [`ObjectLocations`] as the `?location` JSON body.
fn render_locations(key: &str, loc: &soma_meta::ObjectLocations) -> String {
    use soma_meta::{DataLayout, ShardRole};
    let layout = match loc.layout {
        DataLayout::Replicated { width } => {
            format!("{{\"type\":\"replicated\",\"width\":{width}}}")
        }
        DataLayout::Erasure {
            data_shards,
            parity_shards,
        } => format!(
            "{{\"type\":\"erasure\",\"data_shards\":{data_shards},\"parity_shards\":{parity_shards}}}"
        ),
    };
    let nodes: Vec<String> = loc
        .nodes
        .iter()
        .map(|n| {
            let role = match n.role {
                ShardRole::Replica => "replica".to_string(),
                ShardRole::DataShard { index } => format!("data:{index}"),
                ShardRole::ParityShard { index } => format!("parity:{index}"),
            };
            format!(
                "{{\"node_id\":\"{}\",\"endpoint\":\"{}\",\"zone\":\"{}\",\"host\":\"{}\",\"role\":\"{}\"}}",
                json_escape(&n.node_id),
                json_escape(&n.endpoint),
                json_escape(&n.zone),
                json_escape(&n.host),
                role,
            )
        })
        .collect();
    format!(
        "{{\"key\":\"{}\",\"object_id\":{},\"size\":{},\"layout\":{},\"nodes\":[{}]}}\n",
        json_escape(key),
        loc.object_id,
        loc.size,
        layout,
        nodes.join(",")
    )
}

/// Minimal JSON string escaping (quotes, backslashes, control chars).
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

async fn put_object(
    svc: &S3Service,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
    body: Body,
) -> S3Result<Response> {
    // Chunked streaming payloads are not decoded in M0.
    if let Some(h) = headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
    {
        if h.starts_with("STREAMING") {
            return Err(S3Error::not_implemented(
                "chunked streaming upload is not supported",
            ));
        }
    }

    let cond = put_condition(headers)?;
    let request_sse = requested_sse(headers)?;
    let bytes = axum::body::to_bytes(body, MAX_BODY)
        .await
        .map_err(|e| S3Error::invalid_argument(format!("reading body: {e}")))?;

    let ok = svc
        .put_object(
            bucket.to_string(),
            key.to_string(),
            bytes,
            cond,
            now_secs(),
            request_sse,
        )
        .await?;

    let mut h = HeaderMap::new();
    h.insert(header::ETAG, hv(&format!("\"{}\"", ok.etag)));
    if ok.encrypted {
        h.insert("x-amz-server-side-encryption", hv("AES256"));
    }
    Ok((StatusCode::OK, h).into_response())
}

/// Whether the request asked for server-side encryption. Only SSE-S3 (`AES256`)
/// is supported; SSE-KMS/SSE-C are rejected.
fn requested_sse(headers: &HeaderMap) -> S3Result<bool> {
    match headers
        .get("x-amz-server-side-encryption")
        .and_then(|v| v.to_str().ok())
    {
        None => Ok(false),
        Some("AES256") => Ok(true),
        Some(other) => Err(S3Error::not_implemented(format!(
            "unsupported server-side encryption '{other}' (only AES256 is supported)"
        ))),
    }
}

async fn create_multipart(
    svc: &S3Service,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
) -> S3Result<Response> {
    let upload_id = svc
        .create_multipart(bucket.to_string(), key.to_string(), requested_sse(headers)?)
        .await?;
    let body = xml::initiate_multipart_result(bucket, key, &upload_id);
    Ok(xml_response(StatusCode::OK, body))
}

/// `PutBucketEncryption`: set the bucket's default SSE. Only SSE-S3 (`AES256`) is
/// accepted; SSE-KMS is rejected.
async fn put_bucket_encryption(svc: &S3Service, bucket: &str, body: Body) -> S3Result<Response> {
    let bytes = axum::body::to_bytes(body, 64 * 1024)
        .await
        .map_err(|e| S3Error::invalid_argument(format!("reading body: {e}")))?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| S3Error::invalid_argument("body is not valid utf-8"))?;
    if text.contains("aws:kms") {
        return Err(S3Error::not_implemented(
            "SSE-KMS is not supported (only AES256)",
        ));
    }
    if !text.contains("AES256") {
        return Err(S3Error::invalid_argument(
            "expected an SSEAlgorithm of AES256",
        ));
    }
    svc.set_bucket_encryption(bucket.to_string(), Some(soma_meta::SseAlgorithm::Aes256))
        .await?;
    Ok(StatusCode::OK.into_response())
}

/// `GetBucketEncryption`: render the bucket's default SSE config, or 404.
async fn get_bucket_encryption(svc: &S3Service, bucket: &str) -> S3Result<Response> {
    match svc.bucket_encryption(bucket.to_string()).await? {
        Some(soma_meta::SseAlgorithm::Aes256) => Ok(xml_response(
            StatusCode::OK,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <ServerSideEncryptionConfiguration><Rule>\
             <ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm>\
             </ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"
                .to_string(),
        )),
        None => Err(S3Error::no_encryption_config()),
    }
}

async fn upload_part(
    svc: &S3Service,
    upload_id: &str,
    q: &[(String, String)],
    body: Body,
) -> S3Result<Response> {
    let part_number = qget(q, "partNumber")
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|&n| n >= 1)
        .ok_or_else(|| S3Error::invalid_argument("invalid partNumber"))?;
    let bytes = axum::body::to_bytes(body, MAX_BODY)
        .await
        .map_err(|e| S3Error::invalid_argument(format!("reading body: {e}")))?;

    let etag = svc
        .upload_part(upload_id.to_string(), part_number, bytes)
        .await?;
    let mut h = HeaderMap::new();
    h.insert(header::ETAG, hv(&format!("\"{etag}\"")));
    Ok((StatusCode::OK, h).into_response())
}

async fn complete_multipart(
    svc: &S3Service,
    bucket: &str,
    key: &str,
    upload_id: &str,
    body: Body,
) -> S3Result<Response> {
    let bytes = axum::body::to_bytes(body, MAX_BODY)
        .await
        .map_err(|e| S3Error::invalid_argument(format!("reading body: {e}")))?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| S3Error::invalid_argument("body is not valid utf-8"))?;
    let parts = xml::parse_complete_parts(text);

    let etag = svc
        .complete_multipart(
            bucket.to_string(),
            key.to_string(),
            upload_id.to_string(),
            parts,
            now_secs(),
        )
        .await?;
    let body = xml::complete_multipart_result(bucket, key, &etag);
    Ok(xml_response(StatusCode::OK, body))
}

async fn get_object(
    svc: &S3Service,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
    head_only: bool,
) -> S3Result<Response> {
    if head_only {
        let m = svc.head_object(bucket.to_string(), key.to_string()).await?;
        let mut h = object_headers(&m.etag, m.size, m.created_at);
        h.insert(header::CONTENT_LENGTH, hv(&m.size.to_string()));
        if m.encrypted {
            h.insert("x-amz-server-side-encryption", hv("AES256"));
        }
        return Ok((StatusCode::OK, h).into_response());
    }

    let range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let partial = range.is_some();
    let obj = svc
        .get_object(bucket.to_string(), key.to_string(), range)
        .await?;

    let mut h = object_headers(&obj.etag, obj.size, obj.created_at);
    h.insert(header::CONTENT_LENGTH, hv(&obj.data.len().to_string()));
    if obj.encrypted {
        h.insert("x-amz-server-side-encryption", hv("AES256"));
    }
    let status = match (&obj.content_range, partial) {
        (Some(cr), _) => {
            h.insert(header::CONTENT_RANGE, hv(cr));
            StatusCode::PARTIAL_CONTENT
        }
        _ => StatusCode::OK,
    };
    Ok((status, h, obj.data).into_response())
}

async fn delete_object(
    svc: &S3Service,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
) -> S3Result<Response> {
    let cond = put_condition(headers)?;
    svc.delete_object(bucket.to_string(), key.to_string(), cond)
        .await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Build the conditional-write precondition from `If-None-Match` / `If-Match`.
fn put_condition(headers: &HeaderMap) -> S3Result<PutCondition> {
    if let Some(v) = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
    {
        if v.trim() == "*" {
            return Ok(PutCondition::IfNoneMatch);
        }
        return Err(S3Error::invalid_argument(
            "only 'If-None-Match: *' is supported",
        ));
    }
    if let Some(v) = headers.get(header::IF_MATCH).and_then(|v| v.to_str().ok()) {
        return Ok(PutCondition::IfMatch(ETag(unquote(v).to_string())));
    }
    Ok(PutCondition::None)
}

/// Common object response headers (ETag, Last-Modified, Accept-Ranges, type).
fn object_headers(etag: &str, _size: u64, created_at: u64) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(header::ETAG, hv(&format!("\"{etag}\"")));
    h.insert(header::LAST_MODIFIED, hv(&xml::http_date(created_at)));
    h.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    h
}

fn xml_response(status: StatusCode, body: String) -> Response {
    (status, [(header::CONTENT_TYPE, "application/xml")], body).into_response()
}

fn json_response(status: StatusCode, body: String) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}

/// Split a request path into a (decoded bucket, decoded key) pair.
fn split_path(path: &str) -> (String, String) {
    let trimmed = path.strip_prefix('/').unwrap_or(path);
    match trimmed.split_once('/') {
        Some((b, k)) => (pct(b), pct(k)),
        None => (pct(trimmed), String::new()),
    }
}

fn parse_query(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (pct(k), pct(v))
        })
        .collect()
}

fn qget<'a>(q: &'a [(String, String)], name: &str) -> Option<&'a str> {
    q.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
}

fn qhas(q: &[(String, String)], name: &str) -> bool {
    q.iter().any(|(k, _)| k == name)
}

fn pct(s: &str) -> String {
    percent_decode_str(s).decode_utf8_lossy().into_owned()
}

fn unquote(s: &str) -> &str {
    s.trim().trim_matches('"')
}

fn hv(s: &str) -> HeaderValue {
    HeaderValue::from_str(s).unwrap_or_else(|_| HeaderValue::from_static(""))
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
