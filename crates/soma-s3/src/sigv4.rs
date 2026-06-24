//! AWS Signature Version 4 verification (the S3 auth scheme).
//!
//! We recompute the request signature and compare it to the one the client sent
//! in the `Authorization` header. The payload hash is taken verbatim from the
//! `x-amz-content-sha256` header (as AWS specifies) — so verification needs only
//! the request line and headers, not the body.
//!
//! Scope for M0: the standard header-based `AWS4-HMAC-SHA256` scheme with a
//! single signed payload hash or `UNSIGNED-PAYLOAD`. Chunked
//! (`STREAMING-…`) uploads and presigned-URL query auth are rejected by the
//! caller, not handled here.

use axum::http::HeaderMap;
use hmac::{Hmac, Mac};
use percent_encoding::{percent_decode_str, percent_encode, AsciiSet, NON_ALPHANUMERIC};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// RFC 3986 unreserved characters are left as-is; everything else is
/// percent-encoded (used for the canonical query string).
const AWS_ENCODE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// Why SigV4 verification failed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AuthError {
    /// No `Authorization` header was present.
    #[error("missing Authorization header")]
    Missing,
    /// The `Authorization` header was not a well-formed `AWS4-HMAC-SHA256`.
    #[error("malformed Authorization header")]
    Malformed,
    /// The access key id is not known.
    #[error("unknown access key")]
    UnknownAccessKey,
    /// The recomputed signature did not match.
    #[error("signature mismatch")]
    SignatureMismatch,
}

/// The parsed pieces of an `AWS4-HMAC-SHA256` `Authorization` header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthHeader {
    /// Access key id from the credential.
    pub access_key: String,
    /// `yyyymmdd` date from the credential scope.
    pub date: String,
    /// Region from the credential scope.
    pub region: String,
    /// Service from the credential scope (`s3`).
    pub service: String,
    /// Lower-cased signed header names, in the order signed.
    pub signed_headers: Vec<String>,
    /// The hex signature the client computed.
    pub signature: String,
}

impl AuthHeader {
    /// The credential scope string `date/region/service/aws4_request`.
    fn scope(&self) -> String {
        format!(
            "{}/{}/{}/aws4_request",
            self.date, self.region, self.service
        )
    }
}

/// Parse an `Authorization` header value.
pub fn parse_authorization(value: &str) -> Result<AuthHeader, AuthError> {
    let rest = value
        .strip_prefix("AWS4-HMAC-SHA256 ")
        .ok_or(AuthError::Malformed)?;

    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;
    for part in rest.split(',') {
        let part = part.trim();
        if let Some(v) = part.strip_prefix("Credential=") {
            credential = Some(v);
        } else if let Some(v) = part.strip_prefix("SignedHeaders=") {
            signed_headers = Some(v);
        } else if let Some(v) = part.strip_prefix("Signature=") {
            signature = Some(v);
        }
    }

    let credential = credential.ok_or(AuthError::Malformed)?;
    let signed_headers = signed_headers.ok_or(AuthError::Malformed)?;
    let signature = signature.ok_or(AuthError::Malformed)?;

    // Credential = access_key/date/region/service/aws4_request
    let scope: Vec<&str> = credential.split('/').collect();
    if scope.len() != 5 || scope[4] != "aws4_request" {
        return Err(AuthError::Malformed);
    }

    Ok(AuthHeader {
        access_key: scope[0].to_string(),
        date: scope[1].to_string(),
        region: scope[2].to_string(),
        service: scope[3].to_string(),
        signed_headers: signed_headers.split(';').map(|s| s.to_string()).collect(),
        signature: signature.to_string(),
    })
}

/// Recompute the signature for a request and compare it to the client's.
#[allow(clippy::too_many_arguments)]
pub fn verify(
    method: &str,
    path: &str,
    query: &str,
    headers: &HeaderMap,
    amz_date: &str,
    payload_hash: &str,
    auth: &AuthHeader,
    secret: &str,
) -> Result<(), AuthError> {
    let expected = signature(
        method,
        path,
        query,
        headers,
        amz_date,
        payload_hash,
        auth,
        secret,
    );
    if constant_time_eq(expected.as_bytes(), auth.signature.as_bytes()) {
        Ok(())
    } else {
        Err(AuthError::SignatureMismatch)
    }
}

/// Compute the hex signature for a request (the core of [`verify`]; also used by
/// tests to sign requests).
#[allow(clippy::too_many_arguments)]
pub fn signature(
    method: &str,
    path: &str,
    query: &str,
    headers: &HeaderMap,
    amz_date: &str,
    payload_hash: &str,
    auth: &AuthHeader,
    secret: &str,
) -> String {
    let canonical = canonical_request(
        method,
        path,
        query,
        headers,
        payload_hash,
        &auth.signed_headers,
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        auth.scope(),
        sha256_hex(canonical.as_bytes())
    );
    let key = signing_key(secret, &auth.date, &auth.region, &auth.service);
    hex::encode(hmac(&key, string_to_sign.as_bytes()))
}

/// Build the SigV4 canonical request string.
fn canonical_request(
    method: &str,
    path: &str,
    query: &str,
    headers: &HeaderMap,
    payload_hash: &str,
    signed_headers: &[String],
) -> String {
    let canonical_uri = if path.is_empty() { "/" } else { path };
    let canonical_query = canonical_query_string(query);

    let mut header_lines = String::new();
    for name in signed_headers {
        let value = headers
            .get(name.as_str())
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        header_lines.push_str(name);
        header_lines.push(':');
        header_lines.push_str(value.trim());
        header_lines.push('\n');
    }
    let signed_headers_str = signed_headers.join(";");

    format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{header_lines}\n{signed_headers_str}\n{payload_hash}"
    )
}

/// Build the canonical query string: each key/value decoded then re-encoded with
/// AWS rules, sorted by key then value.
fn canonical_query_string(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| {
            let mut it = kv.splitn(2, '=');
            let k = it.next().unwrap_or("");
            let v = it.next().unwrap_or("");
            (aws_encode(&decode(k)), aws_encode(&decode(v)))
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn decode(s: &str) -> String {
    percent_decode_str(s).decode_utf8_lossy().into_owned()
}

fn aws_encode(s: &str) -> String {
    percent_encode(s.as_bytes(), AWS_ENCODE).to_string()
}

fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    let k_secret = format!("AWS4{secret}");
    let k_date = hmac(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, service.as_bytes());
    hmac(&k_service, b"aws4_request")
}

// HMAC accepts a key of any length, so `new_from_slice` is infallible here.
#[allow(clippy::expect_used)]
fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// Hex-encoded SHA-256 of `data`.
pub fn sha256_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use axum::http::HeaderValue;

    // AWS SigV4 reference test-suite vector `get-vanilla`:
    // GET "/" with Host + X-Amz-Date, empty payload, well-known example key.
    #[test]
    fn aws_reference_vector_get_vanilla() {
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("example.amazonaws.com"));
        headers.insert("x-amz-date", HeaderValue::from_static("20150830T123600Z"));

        let auth = AuthHeader {
            access_key: "AKIDEXAMPLE".into(),
            date: "20150830".into(),
            region: "us-east-1".into(),
            service: "service".into(),
            signed_headers: vec!["host".into(), "x-amz-date".into()],
            signature: String::new(),
        };
        let empty_hash = sha256_hex(b"");
        let sig = signature(
            "GET",
            "/",
            "",
            &headers,
            "20150830T123600Z",
            &empty_hash,
            &auth,
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        );
        assert_eq!(
            sig,
            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }

    #[test]
    fn sign_then_verify_roundtrips() {
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("localhost:9000"));
        headers.insert("x-amz-date", HeaderValue::from_static("20240101T000000Z"));
        headers.insert(
            "x-amz-content-sha256",
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );

        let mut auth = AuthHeader {
            access_key: "soma".into(),
            date: "20240101".into(),
            region: "us-east-1".into(),
            service: "s3".into(),
            signed_headers: vec![
                "host".into(),
                "x-amz-content-sha256".into(),
                "x-amz-date".into(),
            ],
            signature: String::new(),
        };
        let secret = "secretkey";
        auth.signature = signature(
            "PUT",
            "/bucket/key.txt",
            "",
            &headers,
            "20240101T000000Z",
            "UNSIGNED-PAYLOAD",
            &auth,
            secret,
        );
        assert!(verify(
            "PUT",
            "/bucket/key.txt",
            "",
            &headers,
            "20240101T000000Z",
            "UNSIGNED-PAYLOAD",
            &auth,
            secret,
        )
        .is_ok());

        // A wrong secret must fail.
        assert_eq!(
            verify(
                "PUT",
                "/bucket/key.txt",
                "",
                &headers,
                "20240101T000000Z",
                "UNSIGNED-PAYLOAD",
                &auth,
                "wrong",
            ),
            Err(AuthError::SignatureMismatch)
        );
    }

    #[test]
    fn canonical_query_is_sorted_and_encoded() {
        assert_eq!(canonical_query_string(""), "");
        assert_eq!(
            canonical_query_string("prefix=a/b&list-type=2"),
            "list-type=2&prefix=a%2Fb"
        );
    }

    #[test]
    fn parse_authorization_extracts_fields() {
        let h = "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request, \
                 SignedHeaders=host;x-amz-date, Signature=abc123";
        let a = parse_authorization(h).unwrap();
        assert_eq!(a.access_key, "AKID");
        assert_eq!(a.region, "us-east-1");
        assert_eq!(a.service, "s3");
        assert_eq!(a.signed_headers, vec!["host", "x-amz-date"]);
        assert_eq!(a.signature, "abc123");
    }

    #[test]
    fn parse_rejects_garbage() {
        assert_eq!(parse_authorization("Basic xyz"), Err(AuthError::Malformed));
    }
}
