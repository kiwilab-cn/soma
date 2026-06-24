//! Minimal S3 XML response rendering. S3's response bodies are simple enough to
//! format directly, avoiding an XML serialization dependency.

use soma_meta::{BucketMeta, ObjectEntry};

/// XML-escape a string for use in element text or attributes.
pub fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

/// Render a `ListAllMyBucketsResult` document.
pub fn list_all_buckets(buckets: &[BucketMeta], created_at: u64) -> String {
    let mut s = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Owner><ID>soma</ID><DisplayName>soma</DisplayName></Owner><Buckets>",
    );
    let ts = iso8601(created_at);
    for b in buckets {
        s.push_str(&format!(
            "<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>",
            escape(&b.name),
            ts
        ));
    }
    s.push_str("</Buckets></ListAllMyBucketsResult>");
    s
}

/// Arguments for rendering a `ListBucketResult` (ListObjectsV2).
pub struct ListObjectsXml<'a> {
    /// Bucket name.
    pub bucket: &'a str,
    /// Requested prefix.
    pub prefix: &'a str,
    /// Requested delimiter, if any.
    pub delimiter: Option<&'a str>,
    /// Effective max-keys echoed back.
    pub max_keys: usize,
    /// Whether the listing was truncated.
    pub is_truncated: bool,
    /// Base64 continuation token for the next page, if truncated.
    pub next_token: Option<&'a str>,
    /// The objects in this page.
    pub objects: &'a [ObjectEntry],
    /// The rolled-up common prefixes.
    pub common_prefixes: &'a [String],
}

/// Render a `ListBucketResult` document (ListObjectsV2).
pub fn list_objects_v2(args: &ListObjectsXml<'_>) -> String {
    let key_count = args.objects.len() + args.common_prefixes.len();
    let mut s = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );
    s.push_str(&format!("<Name>{}</Name>", escape(args.bucket)));
    s.push_str(&format!("<Prefix>{}</Prefix>", escape(args.prefix)));
    if let Some(d) = args.delimiter {
        s.push_str(&format!("<Delimiter>{}</Delimiter>", escape(d)));
    }
    s.push_str(&format!("<MaxKeys>{}</MaxKeys>", args.max_keys));
    s.push_str(&format!("<KeyCount>{key_count}</KeyCount>"));
    s.push_str(&format!("<IsTruncated>{}</IsTruncated>", args.is_truncated));
    if let Some(tok) = args.next_token {
        s.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            escape(tok)
        ));
    }
    for o in args.objects {
        s.push_str(&format!(
            "<Contents><Key>{}</Key><LastModified>{}</LastModified>\
             <ETag>&quot;{}&quot;</ETag><Size>{}</Size>\
             <StorageClass>STANDARD</StorageClass></Contents>",
            escape(&o.key),
            iso8601(o.created_at),
            escape(&o.etag.0),
            o.size
        ));
    }
    for cp in args.common_prefixes {
        s.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            escape(cp)
        ));
    }
    s.push_str("</ListBucketResult>");
    s
}

/// Format a unix timestamp (seconds) as an ISO-8601 UTC instant, e.g.
/// `2024-01-01T00:00:00.000Z`. Uses Howard Hinnant's civil-from-days algorithm
/// so we need no date dependency.
pub fn iso8601(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (hour, min, sec) = (rem / 3600, (rem % 3600) / 60, rem % 60);

    let z = days + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };

    format!("{year:04}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}.000Z")
}

/// Format a unix timestamp as an RFC 1123 / RFC 7231 HTTP date, e.g.
/// `Mon, 01 Jan 2024 00:00:00 GMT` (for the `Last-Modified` header).
pub fn http_date(secs: u64) -> String {
    const WDAY: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    const MON: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (hour, min, sec) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // 1970-01-01 was a Thursday → index 0 in WDAY.
    let wday = WDAY[(days.rem_euclid(7)) as usize];

    let z = days + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    let mon = MON[(m - 1) as usize];

    format!("{wday}, {d:02} {mon} {year:04} {hour:02}:{min:02}:{sec:02} GMT")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn http_date_known() {
        assert_eq!(http_date(0), "Thu, 01 Jan 1970 00:00:00 GMT");
        assert_eq!(http_date(1_704_067_200), "Mon, 01 Jan 2024 00:00:00 GMT");
    }

    #[test]
    fn escapes_xml_specials() {
        assert_eq!(escape("a<b>&\"'"), "a&lt;b&gt;&amp;&quot;&apos;");
    }

    #[test]
    fn iso8601_epoch_and_known_date() {
        assert_eq!(iso8601(0), "1970-01-01T00:00:00.000Z");
        // 2024-01-01T00:00:00Z = 1704067200
        assert_eq!(iso8601(1_704_067_200), "2024-01-01T00:00:00.000Z");
    }
}
