//! End-to-end tests: drive the real router (over real redb + local-FS stores)
//! with SigV4-signed requests via `tower::oneshot`.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode, Uri};
use axum::Router;
use bytes::Bytes;
use tempfile::TempDir;
use tower::ServiceExt;

use soma_backend::{BackendConfig, LocalFsBackend, StorageBackend};
use soma_meta::{MetadataStore, RedbMetaStore};

use crate::sigv4::{self, sha256_hex, AuthHeader};
use crate::{router, Credentials, S3Service};

const AK: &str = "AK";
const SK: &str = "SK";

fn make_app() -> (Router, TempDir) {
    let dir = TempDir::new().unwrap();
    let meta: Arc<dyn MetadataStore> =
        Arc::new(RedbMetaStore::open(dir.path().join("meta.redb")).unwrap());
    let backend: Arc<dyn StorageBackend> =
        Arc::new(LocalFsBackend::open(dir.path(), BackendConfig::default()).unwrap());
    let svc = S3Service::new(meta, backend, Credentials::single(AK, SK));
    (router(svc), dir)
}

fn auth_header(
    method: &str,
    uri: &Uri,
    headers: &HeaderMap,
    payload_hash: &str,
    secret: &str,
) -> String {
    let auth = AuthHeader {
        access_key: AK.to_string(),
        date: "20240101".to_string(),
        region: "us-east-1".to_string(),
        service: "s3".to_string(),
        signed_headers: vec![
            "host".to_string(),
            "x-amz-content-sha256".to_string(),
            "x-amz-date".to_string(),
        ],
        signature: String::new(),
    };
    let sig = sigv4::signature(
        method,
        uri.path(),
        uri.query().unwrap_or(""),
        headers,
        "20240101T000000Z",
        payload_hash,
        &auth,
        secret,
    );
    format!(
        "AWS4-HMAC-SHA256 Credential={AK}/20240101/us-east-1/s3/aws4_request, \
         SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={sig}"
    )
}

/// Send a signed request and return (status, headers, body).
async fn signed(
    app: &Router,
    method: &str,
    uri: &str,
    body: &[u8],
    extra: &[(&str, &str)],
) -> (StatusCode, HeaderMap, Bytes) {
    send(app, method, uri, body, extra, SK, true).await
}

async fn send(
    app: &Router,
    method: &str,
    uri: &str,
    body: &[u8],
    extra: &[(&str, &str)],
    secret: &str,
    with_auth: bool,
) -> (StatusCode, HeaderMap, Bytes) {
    let parsed: Uri = uri.parse().unwrap();
    let payload = sha256_hex(body);

    let mut signed_headers = HeaderMap::new();
    signed_headers.insert("host", HeaderValue::from_static("localhost:9000"));
    signed_headers.insert("x-amz-date", HeaderValue::from_static("20240101T000000Z"));
    signed_headers.insert(
        "x-amz-content-sha256",
        HeaderValue::from_str(&payload).unwrap(),
    );

    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("host", "localhost:9000")
        .header("x-amz-date", "20240101T000000Z")
        .header("x-amz-content-sha256", &payload);
    if with_auth {
        let authz = auth_header(method, &parsed, &signed_headers, &payload, secret);
        builder = builder.header("authorization", authz);
    }
    for (k, v) in extra {
        builder = builder.header(*k, *v);
    }

    let request = builder.body(Body::from(body.to_vec())).unwrap();
    let resp = app.clone().oneshot(request).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, headers, bytes)
}

fn body_str(b: &Bytes) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// Extract the text inside `<tag>...</tag>` from an XML body.
fn xml_tag(xml: &str, tag: &str) -> String {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let i = xml.find(&open).unwrap() + open.len();
    let j = xml[i..].find(&close).unwrap() + i;
    xml[i..j].to_string()
}

#[tokio::test]
async fn multipart_upload_roundtrip() {
    let (app, _dir) = make_app();
    signed(&app, "PUT", "/b", b"", &[]).await;

    // Initiate.
    let (status, _, body) = signed(&app, "POST", "/b/big.bin?uploads", b"", &[]).await;
    assert_eq!(status, StatusCode::OK);
    let upload_id = xml_tag(&body_str(&body), "UploadId");
    assert!(upload_id.starts_with("soma-"));

    // Upload two parts.
    let part1 = vec![b'a'; 100];
    let part2 = vec![b'b'; 50];
    let (s1, h1, _) = signed(
        &app,
        "PUT",
        &format!("/b/big.bin?partNumber=1&uploadId={upload_id}"),
        &part1,
        &[],
    )
    .await;
    assert_eq!(s1, StatusCode::OK);
    let etag1 = h1.get("etag").unwrap().to_str().unwrap().to_string();
    let (s2, h2, _) = signed(
        &app,
        "PUT",
        &format!("/b/big.bin?partNumber=2&uploadId={upload_id}"),
        &part2,
        &[],
    )
    .await;
    assert_eq!(s2, StatusCode::OK);
    let etag2 = h2.get("etag").unwrap().to_str().unwrap().to_string();

    // Complete.
    let complete_body = format!(
        "<CompleteMultipartUpload>\
         <Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part>\
         </CompleteMultipartUpload>"
    );
    let (sc, _, cbody) = signed(
        &app,
        "POST",
        &format!("/b/big.bin?uploadId={upload_id}"),
        complete_body.as_bytes(),
        &[],
    )
    .await;
    assert_eq!(sc, StatusCode::OK);
    // Multipart ETag carries the part count as a `-N` suffix (XML-escaped quotes
    // surround it in the response body).
    assert!(body_str(&cbody).contains("-2&quot;"));

    // The assembled object reads back as part1 ++ part2.
    let (sg, _, gbody) = signed(&app, "GET", "/b/big.bin", b"", &[]).await;
    assert_eq!(sg, StatusCode::OK);
    let mut expected = part1.clone();
    expected.extend_from_slice(&part2);
    assert_eq!(gbody.as_ref(), expected.as_slice());
}

#[tokio::test]
async fn multipart_abort_then_complete_fails() {
    let (app, _dir) = make_app();
    signed(&app, "PUT", "/b", b"", &[]).await;
    let (_, _, body) = signed(&app, "POST", "/b/k?uploads", b"", &[]).await;
    let upload_id = xml_tag(&body_str(&body), "UploadId");
    signed(
        &app,
        "PUT",
        &format!("/b/k?partNumber=1&uploadId={upload_id}"),
        b"data",
        &[],
    )
    .await;

    // Abort.
    let (sa, _, _) = signed(
        &app,
        "DELETE",
        &format!("/b/k?uploadId={upload_id}"),
        b"",
        &[],
    )
    .await;
    assert_eq!(sa, StatusCode::NO_CONTENT);

    // Completing the aborted upload now fails.
    let cbody = "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"x\"</ETag></Part></CompleteMultipartUpload>";
    let (sc, _, cb) = signed(
        &app,
        "POST",
        &format!("/b/k?uploadId={upload_id}"),
        cbody.as_bytes(),
        &[],
    )
    .await;
    assert_eq!(sc, StatusCode::NOT_FOUND);
    assert!(body_str(&cb).contains("NoSuchUpload"));
}

#[tokio::test]
async fn upload_part_to_unknown_upload_fails() {
    let (app, _dir) = make_app();
    signed(&app, "PUT", "/b", b"", &[]).await;
    let (status, _, body) = signed(
        &app,
        "PUT",
        "/b/k?partNumber=1&uploadId=soma-deadbeef",
        b"data",
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body_str(&body).contains("NoSuchUpload"));
}

#[tokio::test]
async fn full_object_lifecycle() {
    let (app, _dir) = make_app();

    // Create bucket.
    let (status, _, _) = signed(&app, "PUT", "/mybucket", b"", &[]).await;
    assert_eq!(status, StatusCode::OK);

    // Put object.
    let payload = b"hello soma object storage";
    let (status, headers, _) = signed(&app, "PUT", "/mybucket/greeting.txt", payload, &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("etag").is_some());

    // Get object.
    let (status, headers, body) = signed(&app, "GET", "/mybucket/greeting.txt", b"", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), payload);
    assert_eq!(
        headers.get("content-length").unwrap(),
        &payload.len().to_string()
    );

    // Head object.
    let (status, headers, body) = signed(&app, "HEAD", "/mybucket/greeting.txt", b"", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty());
    assert!(headers.get("last-modified").is_some());

    // List objects.
    let (status, _, body) = signed(&app, "GET", "/mybucket?list-type=2", b"", &[]).await;
    assert_eq!(status, StatusCode::OK);
    let xml = body_str(&body);
    assert!(xml.contains("<Key>greeting.txt</Key>"));

    // List buckets.
    let (status, _, body) = signed(&app, "GET", "/", b"", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body_str(&body).contains("<Name>mybucket</Name>"));

    // Delete object.
    let (status, _, _) = signed(&app, "DELETE", "/mybucket/greeting.txt", b"", &[]).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Now missing.
    let (status, _, _) = signed(&app, "GET", "/mybucket/greeting.txt", b"", &[]).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn auth_failures() {
    let (app, _dir) = make_app();

    // Missing Authorization header → 403.
    let (status, _, _) = send(&app, "GET", "/", b"", &[], SK, false).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Wrong secret → signature mismatch → 403.
    let (status, _, body) = send(&app, "GET", "/", b"", &[], "WRONG", true).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body_str(&body).contains("SignatureDoesNotMatch"));
}

#[tokio::test]
async fn conditional_put_if_none_match() {
    let (app, _dir) = make_app();
    signed(&app, "PUT", "/b", b"", &[]).await;

    // First create with If-None-Match: * succeeds.
    let (status, _, _) = signed(&app, "PUT", "/b/k", b"v1", &[("if-none-match", "*")]).await;
    assert_eq!(status, StatusCode::OK);

    // Second fails with 412.
    let (status, _, body) = signed(&app, "PUT", "/b/k", b"v2", &[("if-none-match", "*")]).await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED);
    assert!(body_str(&body).contains("PreconditionFailed"));

    // Original is intact.
    let (_, _, body) = signed(&app, "GET", "/b/k", b"", &[]).await;
    assert_eq!(body.as_ref(), b"v1");
}

#[tokio::test]
async fn conditional_put_if_match() {
    let (app, _dir) = make_app();
    signed(&app, "PUT", "/b", b"", &[]).await;
    let (_, headers, _) = signed(&app, "PUT", "/b/k", b"v1", &[]).await;
    let etag = headers.get("etag").unwrap().to_str().unwrap().to_string();

    // Wrong etag → 412.
    let (status, _, _) = signed(&app, "PUT", "/b/k", b"v2", &[("if-match", "\"deadbeef\"")]).await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED);

    // Matching etag → 200.
    let (status, _, _) = signed(&app, "PUT", "/b/k", b"v2", &[("if-match", etag.as_str())]).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn range_get() {
    let (app, _dir) = make_app();
    signed(&app, "PUT", "/b", b"", &[]).await;
    signed(&app, "PUT", "/b/data", b"0123456789", &[]).await;

    let (status, headers, body) =
        signed(&app, "GET", "/b/data", b"", &[("range", "bytes=2-5")]).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body.as_ref(), b"2345");
    assert_eq!(headers.get("content-range").unwrap(), "bytes 2-5/10");
}

#[tokio::test]
async fn list_with_delimiter() {
    let (app, _dir) = make_app();
    signed(&app, "PUT", "/b", b"", &[]).await;
    for key in ["docs/a", "docs/b", "root"] {
        signed(&app, "PUT", &format!("/b/{key}"), b"x", &[]).await;
    }
    let (status, _, body) = signed(&app, "GET", "/b?list-type=2&delimiter=%2F", b"", &[]).await;
    assert_eq!(status, StatusCode::OK);
    let xml = body_str(&body);
    assert!(xml.contains("<Prefix>docs/</Prefix>") || xml.contains("docs/"));
    assert!(xml.contains("<CommonPrefixes>"));
    assert!(xml.contains("<Key>root</Key>"));
}

#[tokio::test]
async fn get_missing_bucket_and_key() {
    let (app, _dir) = make_app();
    let (status, _, _) = signed(&app, "GET", "/nope/key", b"", &[]).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn delete_non_empty_bucket_conflicts() {
    let (app, _dir) = make_app();
    signed(&app, "PUT", "/b", b"", &[]).await;
    signed(&app, "PUT", "/b/k", b"x", &[]).await;
    let (status, _, body) = signed(&app, "DELETE", "/b", b"", &[]).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(body_str(&body).contains("BucketNotEmpty"));
}
