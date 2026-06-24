//! Integration tests for `RedbMetaStore` against a real temp database.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use soma_meta::{
    BucketOpts, ETag, ListRequest, MetadataStore, ObjectPut, PutCondition, Quota, RedbMetaStore,
    TenantUsage, Version,
};
use tempfile::TempDir;

fn store(dir: &TempDir) -> RedbMetaStore {
    RedbMetaStore::open(dir.path().join("meta.redb")).unwrap()
}

fn put(object_id: u64, _offset: u64, size: u32, etag: &str) -> ObjectPut {
    ObjectPut {
        object_id,
        size: size as u64,
        etag: ETag(etag.to_string()),
        created_at: 0,
        tenant: String::new(),
        quota: Quota::default(),
    }
}

/// Like [`put`], but charged to `tenant` under `quota`.
fn put_for(object_id: u64, size: u64, etag: &str, tenant: &str, quota: Quota) -> ObjectPut {
    ObjectPut {
        object_id,
        size,
        etag: ETag(etag.to_string()),
        created_at: 0,
        tenant: tenant.to_string(),
        quota,
    }
}

#[test]
fn bucket_lifecycle() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    m.create_bucket("b1", BucketOpts::default()).unwrap();
    assert!(m.get_bucket("b1").unwrap().is_some());
    // Duplicate create errors.
    assert!(m.create_bucket("b1", BucketOpts::default()).is_err());
    m.create_bucket("b2", BucketOpts { versioning: true })
        .unwrap();

    let names: Vec<_> = m
        .list_buckets()
        .unwrap()
        .into_iter()
        .map(|b| b.name)
        .collect();
    assert_eq!(names, vec!["b1".to_string(), "b2".to_string()]);

    m.delete_bucket("b1").unwrap();
    assert!(m.get_bucket("b1").unwrap().is_none());
    // Deleting a missing bucket errors.
    assert!(m.delete_bucket("b1").is_err());
}

#[test]
fn delete_non_empty_bucket_errors() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    m.create_bucket("b", BucketOpts::default()).unwrap();
    m.put_object("b", "k", put(1, 0, 3, "e1"), PutCondition::None)
        .unwrap();
    assert!(m.delete_bucket("b").is_err());
}

#[test]
fn put_get_and_version_increment() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    m.create_bucket("b", BucketOpts::default()).unwrap();

    let v1 = m
        .put_object("b", "k", put(1, 0, 3, "e1"), PutCondition::None)
        .unwrap();
    assert_eq!(v1, Version(1));
    let got = m.get_object("b", "k").unwrap().unwrap();
    assert_eq!(got.object_id, 1);
    assert_eq!(got.etag, ETag("e1".to_string()));
    assert_eq!(got.version, Version(1));

    let v2 = m
        .put_object("b", "k", put(2, 40, 5, "e2"), PutCondition::None)
        .unwrap();
    assert_eq!(v2, Version(2));
    assert_eq!(
        m.get_object("b", "k").unwrap().unwrap().etag,
        ETag("e2".to_string())
    );
}

#[test]
fn put_to_missing_bucket_errors() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    assert!(m
        .put_object("nope", "k", put(1, 0, 3, "e"), PutCondition::None)
        .is_err());
}

#[test]
fn if_none_match_creates_only_when_absent() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    m.create_bucket("b", BucketOpts::default()).unwrap();

    // First create succeeds.
    m.put_object("b", "k", put(1, 0, 3, "e1"), PutCondition::IfNoneMatch)
        .unwrap();
    // Second create with the same precondition fails (object now exists).
    assert!(m
        .put_object("b", "k", put(2, 40, 3, "e2"), PutCondition::IfNoneMatch)
        .is_err());
    // The original value is untouched.
    assert_eq!(
        m.get_object("b", "k").unwrap().unwrap().etag,
        ETag("e1".to_string())
    );
}

#[test]
fn if_match_overwrites_only_on_matching_etag() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    m.create_bucket("b", BucketOpts::default()).unwrap();
    m.put_object("b", "k", put(1, 0, 3, "e1"), PutCondition::None)
        .unwrap();

    // Wrong etag is rejected.
    assert!(m
        .put_object(
            "b",
            "k",
            put(2, 40, 3, "e2"),
            PutCondition::IfMatch(ETag("wrong".into()))
        )
        .is_err());
    // Matching etag succeeds.
    let v = m
        .put_object(
            "b",
            "k",
            put(2, 40, 3, "e2"),
            PutCondition::IfMatch(ETag("e1".into())),
        )
        .unwrap();
    assert_eq!(v, Version(2));

    // If-Match on a missing object fails.
    assert!(m
        .put_object(
            "b",
            "missing",
            put(3, 80, 3, "e3"),
            PutCondition::IfMatch(ETag("x".into()))
        )
        .is_err());
}

#[test]
fn delete_is_idempotent_and_respects_conditions() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    m.create_bucket("b", BucketOpts::default()).unwrap();
    m.put_object("b", "k", put(1, 0, 3, "e1"), PutCondition::None)
        .unwrap();

    // If-Match with wrong etag refuses.
    assert!(m
        .delete_object("b", "k", PutCondition::IfMatch(ETag("no".into())))
        .is_err());
    // Correct etag deletes.
    m.delete_object("b", "k", PutCondition::IfMatch(ETag("e1".into())))
        .unwrap();
    assert!(m.get_object("b", "k").unwrap().is_none());
    // Deleting again (absent) is a no-op success.
    m.delete_object("b", "k", PutCondition::None).unwrap();
}

#[test]
fn next_object_id_is_monotonic() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    let ids: Vec<_> = (0..5).map(|_| m.next_object_id().unwrap()).collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);
}

#[test]
fn persistence_across_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let m = store(&dir);
        m.create_bucket("b", BucketOpts::default()).unwrap();
        m.put_object("b", "k", put(1, 0, 4, "etag"), PutCondition::None)
            .unwrap();
        let _ = m.next_object_id().unwrap(); // bump the counter
    }
    let m = store(&dir);
    assert!(m.get_bucket("b").unwrap().is_some());
    assert_eq!(
        m.get_object("b", "k").unwrap().unwrap().etag,
        ETag("etag".into())
    );
    // Counter survived (was 1 after the bump; next is 2).
    assert_eq!(m.next_object_id().unwrap(), 2);
}

#[test]
fn list_prefix_and_pagination() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    m.create_bucket("b", BucketOpts::default()).unwrap();
    for i in 0..10 {
        let key = format!("file-{i:02}");
        m.put_object(
            "b",
            &key,
            put(i + 1, i * 64, 3, &format!("e{i}")),
            PutCondition::None,
        )
        .unwrap();
    }
    // Also an unrelated key that the prefix must exclude.
    m.put_object("b", "other", put(100, 999, 3, "eo"), PutCondition::None)
        .unwrap();

    // Page 1: max 4 of the "file-" prefix.
    let req = ListRequest {
        prefix: "file-".into(),
        delimiter: None,
        continuation_token: None,
        max_keys: 4,
    };
    let page1 = m.list_objects("b", &req).unwrap();
    assert_eq!(page1.objects.len(), 4);
    assert!(page1.is_truncated);
    assert_eq!(page1.objects[0].key, "file-00");
    assert_eq!(page1.objects[3].key, "file-03");

    // Page 2 via the continuation token.
    let req2 = ListRequest {
        continuation_token: page1.next_continuation_token,
        ..req.clone()
    };
    let page2 = m.list_objects("b", &req2).unwrap();
    assert_eq!(page2.objects[0].key, "file-04");

    // Full listing returns exactly the 10 prefixed keys, sorted.
    let all = m
        .list_objects(
            "b",
            &ListRequest {
                prefix: "file-".into(),
                max_keys: 1000,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(all.objects.len(), 10);
    assert!(!all.is_truncated);
    let keys: Vec<_> = all.objects.iter().map(|o| o.key.clone()).collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
}

#[test]
fn list_with_delimiter_rolls_up_common_prefixes() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    m.create_bucket("b", BucketOpts::default()).unwrap();
    for key in ["a/1", "a/2", "a/3", "b/1", "top"] {
        m.put_object("b", key, put(1, 0, 1, "e"), PutCondition::None)
            .unwrap();
    }
    let req = ListRequest {
        prefix: String::new(),
        delimiter: Some("/".into()),
        continuation_token: None,
        max_keys: 1000,
    };
    let res = m.list_objects("b", &req).unwrap();
    // "a/" and "b/" roll up; "top" is a leaf.
    assert_eq!(
        res.common_prefixes,
        vec!["a/".to_string(), "b/".to_string()]
    );
    assert_eq!(res.objects.len(), 1);
    assert_eq!(res.objects[0].key, "top");
}

#[test]
fn list_delimiter_paginates_without_duplicating_prefixes() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    m.create_bucket("b", BucketOpts::default()).unwrap();
    for key in ["a/1", "a/2", "b/1", "c/1", "d/1"] {
        m.put_object("b", key, put(1, 0, 1, "e"), PutCondition::None)
            .unwrap();
    }
    // Page through with max_keys=2; collect all common prefixes across pages.
    let mut token = None;
    let mut prefixes = Vec::new();
    loop {
        let req = ListRequest {
            prefix: String::new(),
            delimiter: Some("/".into()),
            continuation_token: token,
            max_keys: 2,
        };
        let res = m.list_objects("b", &req).unwrap();
        prefixes.extend(res.common_prefixes);
        if !res.is_truncated {
            break;
        }
        token = res.next_continuation_token;
    }
    assert_eq!(
        prefixes,
        vec![
            "a/".to_string(),
            "b/".to_string(),
            "c/".to_string(),
            "d/".to_string()
        ]
    );
}

// --- multi-tenant quotas (M4c) ---------------------------------------------

const T: &str = "tenant-a";

fn bucket(m: &RedbMetaStore) {
    m.create_bucket("b", BucketOpts::default()).unwrap();
}

#[test]
fn byte_quota_is_enforced_atomically() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    bucket(&m);
    let q = Quota {
        max_bytes: 100,
        max_objects: 0,
    };

    // Two puts up to the limit succeed.
    m.put_object("b", "k1", put_for(1, 60, "e1", T, q), PutCondition::None)
        .unwrap();
    m.put_object("b", "k2", put_for(2, 40, "e2", T, q), PutCondition::None)
        .unwrap();
    assert_eq!(
        m.tenant_usage(T).unwrap(),
        TenantUsage {
            bytes: 100,
            objects: 2
        }
    );

    // The next byte over the limit is rejected, and usage is unchanged (atomic).
    let err = m
        .put_object("b", "k3", put_for(3, 1, "e3", T, q), PutCondition::None)
        .unwrap_err();
    assert!(matches!(err, soma_meta::Error::QuotaExceeded(_)));
    assert_eq!(
        m.tenant_usage(T).unwrap(),
        TenantUsage {
            bytes: 100,
            objects: 2
        }
    );
    assert!(m.get_object("b", "k3").unwrap().is_none()); // nothing committed
}

#[test]
fn object_count_quota_is_enforced() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    bucket(&m);
    let q = Quota {
        max_bytes: 0,
        max_objects: 2,
    };
    m.put_object("b", "k1", put_for(1, 1, "e1", T, q), PutCondition::None)
        .unwrap();
    m.put_object("b", "k2", put_for(2, 1, "e2", T, q), PutCondition::None)
        .unwrap();
    assert!(m
        .put_object("b", "k3", put_for(3, 1, "e3", T, q), PutCondition::None)
        .is_err());
}

#[test]
fn overwrite_refunds_the_previous_version() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    bucket(&m);
    let q = Quota {
        max_bytes: 100,
        max_objects: 0,
    };
    m.put_object("b", "k", put_for(1, 80, "e1", T, q), PutCondition::None)
        .unwrap();
    // Overwriting the same key with a larger object: the old 80 bytes are
    // refunded, so 90 fits under the 100-byte limit (90, not 170).
    m.put_object("b", "k", put_for(2, 90, "e2", T, q), PutCondition::None)
        .unwrap();
    assert_eq!(
        m.tenant_usage(T).unwrap(),
        TenantUsage {
            bytes: 90,
            objects: 1
        }
    );
}

#[test]
fn delete_refunds_quota() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    bucket(&m);
    let q = Quota {
        max_bytes: 100,
        max_objects: 0,
    };
    m.put_object("b", "k", put_for(1, 70, "e1", T, q), PutCondition::None)
        .unwrap();
    m.delete_object("b", "k", PutCondition::None).unwrap();
    assert_eq!(m.tenant_usage(T).unwrap(), TenantUsage::default());
    // The freed space is reusable.
    m.put_object("b", "k2", put_for(2, 90, "e2", T, q), PutCondition::None)
        .unwrap();
    assert_eq!(
        m.tenant_usage(T).unwrap(),
        TenantUsage {
            bytes: 90,
            objects: 1
        }
    );
}

#[test]
fn untenanted_puts_skip_accounting() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    bucket(&m);
    m.put_object("b", "k", put(1, 0, 1000, "e1"), PutCondition::None)
        .unwrap();
    assert_eq!(m.tenant_usage("").unwrap(), TenantUsage::default());
}
