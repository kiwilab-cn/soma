//! AWS SigV4 **signing** for the gateway fallback path — the mirror image of the
//! gateway's verifier (`soma-s3`). It must canonicalize identically: the path is
//! used verbatim as the canonical URI (not re-encoded), and the query is decoded,
//! AWS-encoded, and sorted.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// The headers a signed GET must carry.
#[derive(Debug, Clone)]
pub struct Signed {
    /// `Authorization` header value.
    pub authorization: String,
    /// `x-amz-date` header value (`yyyymmddThhmmssZ`).
    pub amz_date: String,
    /// `x-amz-content-sha256` header value (SHA-256 of the empty body).
    pub content_sha256: String,
}

/// Sign a GET request. `host` is the `Host` header the HTTP client will send
/// (authority of the URL); `path` is the request path verbatim (already
/// percent-encoded as it will appear on the wire); `query` is the raw query string.
pub fn sign_get(
    host: &str,
    path: &str,
    query: &str,
    access_key: &str,
    secret: &str,
    region: &str,
    now_unix: u64,
) -> Signed {
    let (amz_date, datestamp) = amz_timestamp(now_unix);
    let payload_hash = sha256_hex(b""); // GET has an empty body
    let canonical_uri = if path.is_empty() { "/" } else { path };
    let canonical_query = canonical_query_string(query);

    // Signed headers, in sorted order (host < x-amz-content-sha256 < x-amz-date).
    let signed: [(&str, &str); 3] = [
        ("host", host),
        ("x-amz-content-sha256", &payload_hash),
        ("x-amz-date", &amz_date),
    ];

    let signature = compute_signature(
        "GET",
        canonical_uri,
        &canonical_query,
        &signed,
        &payload_hash,
        &amz_date,
        &datestamp,
        region,
        "s3",
        secret,
    );

    let names = signed
        .iter()
        .map(|(n, _)| *n)
        .collect::<Vec<_>>()
        .join(";");
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{datestamp}/{region}/s3/aws4_request, \
         SignedHeaders={names}, Signature={signature}"
    );
    Signed {
        authorization,
        amz_date,
        content_sha256: payload_hash,
    }
}

/// Compute the hex SigV4 signature from already-canonical pieces (mirrors the
/// verifier's `signature`).
#[allow(clippy::too_many_arguments)]
fn compute_signature(
    method: &str,
    canonical_uri: &str,
    canonical_query: &str,
    signed: &[(&str, &str)],
    payload_hash: &str,
    amz_date: &str,
    datestamp: &str,
    region: &str,
    service: &str,
    secret: &str,
) -> String {
    let mut header_lines = String::new();
    for (name, value) in signed {
        header_lines.push_str(name);
        header_lines.push(':');
        header_lines.push_str(value.trim());
        header_lines.push('\n');
    }
    let names = signed
        .iter()
        .map(|(n, _)| *n)
        .collect::<Vec<_>>()
        .join(";");
    let canonical_request = format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{header_lines}\n{names}\n{payload_hash}"
    );
    let scope = format!("{datestamp}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let key = signing_key(secret, datestamp, region, service);
    hex::encode(hmac(&key, string_to_sign.as_bytes()))
}

/// Canonical query string: decode each key/value, AWS-encode, sort by key then
/// value (mirrors the verifier).
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
            (aws_encode(&pct_decode(k)), aws_encode(&pct_decode(v)))
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Percent-encode an object key for use in a request path: everything except the
/// RFC 3986 unreserved set and `/` (which stays a path separator).
pub fn encode_path_segment(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for &b in key.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~' | b'/') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// AWS-encode a string: unreserved chars stay, everything else is `%XX`.
fn aws_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Minimal percent-decoding (`%XX` and `+` → space) for canonicalizing a query.
fn pct_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                if let (Some(h), Some(l)) = (hi, lo) {
                    out.push((h * 16 + l) as u8);
                    i += 3;
                    continue;
                }
                out.push(b'%');
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    let k_secret = format!("AWS4{secret}");
    let k_date = hmac(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, service.as_bytes());
    hmac(&k_service, b"aws4_request")
}

// HMAC accepts a key of any length, so construction is infallible here.
#[allow(clippy::expect_used)]
fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn sha256_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

/// Format unix seconds as the SigV4 `(amz_date, datestamp)` pair, in UTC. The
/// gateway does not check the date window, but a real timestamp keeps requests
/// correct against any S3-compatible endpoint.
fn amz_timestamp(unix_secs: u64) -> (String, String) {
    let days = (unix_secs / 86_400) as i64;
    let sod = unix_secs % 86_400;
    let (y, m, d) = civil_from_days(days);
    let (hh, mm, ss) = (sod / 3600, (sod % 3600) / 60, sod % 60);
    (
        format!("{y:04}{m:02}{d:02}T{hh:02}{mm:02}{ss:02}Z"),
        format!("{y:04}{m:02}{d:02}"),
    )
}

/// Civil date `(year, month, day)` from days since the Unix epoch (Howard
/// Hinnant's algorithm).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    // AWS SigV4 reference vector `get-vanilla` — the same one the gateway's verifier
    // is tested against, proving our canonicalization matches.
    #[test]
    fn aws_reference_vector_get_vanilla() {
        let payload_hash = sha256_hex(b"");
        let signed = [
            ("host", "example.amazonaws.com"),
            ("x-amz-date", "20150830T123600Z"),
        ];
        let sig = compute_signature(
            "GET",
            "/",
            "",
            &signed,
            &payload_hash,
            "20150830T123600Z",
            "20150830",
            "us-east-1",
            "service",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        );
        assert_eq!(
            sig,
            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }

    #[test]
    fn timestamp_formats_utc() {
        // 1440938160 = 2015-08-30T12:36:00Z (the reference vector's instant).
        assert_eq!(
            amz_timestamp(1_440_938_160),
            ("20150830T123600Z".to_string(), "20150830".to_string())
        );
        // Unix epoch.
        assert_eq!(
            amz_timestamp(0),
            ("19700101T000000Z".to_string(), "19700101".to_string())
        );
    }

    #[test]
    fn canonical_query_matches_verifier() {
        assert_eq!(canonical_query_string(""), "");
        assert_eq!(canonical_query_string("location"), "location=");
        assert_eq!(
            canonical_query_string("prefix=a/b&list-type=2"),
            "list-type=2&prefix=a%2Fb"
        );
    }

    #[test]
    fn sign_get_sets_three_headers() {
        let s = sign_get(
            "127.0.0.1:9000",
            "/bucket/key",
            "location",
            "AK",
            "SK",
            "us-east-1",
            1_700_000_000,
        );
        assert!(s.authorization.starts_with("AWS4-HMAC-SHA256 Credential=AK/"));
        assert!(s
            .authorization
            .contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"));
        assert_eq!(s.content_sha256, sha256_hex(b""));
    }
}
