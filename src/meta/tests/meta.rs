//! Integration tests for `RedbMetaStore` against a real temp database.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use soma_meta::{
    BucketOpts, BucketUsage, ETag, ListRequest, MetadataStore, ObjectPut, ObjectPutItem,
    PutCondition, Quota, RedbMetaStore, Version,
};
use tempfile::TempDir;

fn item(bucket: &str, key: &str, put: ObjectPut, cond: PutCondition) -> ObjectPutItem {
    ObjectPutItem {
        bucket: bucket.to_string(),
        key: key.to_string(),
        put,
        cond,
    }
}

fn store(dir: &TempDir) -> RedbMetaStore {
    RedbMetaStore::open(dir.path().join("meta.redb")).unwrap()
}

fn put(object_id: u64, _offset: u64, size: u32, etag: &str) -> ObjectPut {
    put_sized(object_id, size as u64, etag)
}

/// An [`ObjectPut`] of an explicit byte size (quota is per-bucket, not per-put).
fn put_sized(object_id: u64, size: u64, etag: &str) -> ObjectPut {
    ObjectPut {
        object_id,
        size,
        etag: ETag(etag.to_string()),
        created_at: 0,
        encrypted: false,
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
    m.create_bucket(
        "b2",
        BucketOpts {
            versioning: true,
            ..Default::default()
        },
    )
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
fn bucket_ownership_and_policy() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);

    // create→own: the owner is recorded from BucketOpts.
    m.create_bucket(
        "owned",
        BucketOpts {
            owner: "AK".to_string(),
            ..Default::default()
        },
    )
    .unwrap();
    let b = m.get_bucket("owned").unwrap().unwrap();
    assert_eq!(b.owner, "AK");
    assert!(b.can_read("AK") && b.can_write("AK"));
    assert!(!b.can_read("BK") && !b.can_write("BK")); // private, owner-only

    // Make it public-read with an extra reader; writes stay owner-only.
    m.set_bucket_policy("owned", "AK", true, vec!["CK".to_string()])
        .unwrap();
    let b = m.get_bucket("owned").unwrap().unwrap();
    assert!(b.can_read("BK")); // public_read
    assert!(b.can_read("CK")); // explicit reader
    assert!(!b.can_write("BK") && !b.can_write("CK")); // still owner-only
    assert!(b.can_write("AK"));

    // Unowned bucket is open to everyone (back-compat / single-tenant default).
    m.create_bucket("open", BucketOpts::default()).unwrap();
    let o = m.get_bucket("open").unwrap().unwrap();
    assert!(o.can_read("anyone") && o.can_write("anyone"));

    // Policy on a missing bucket errors.
    assert!(m.set_bucket_policy("ghost", "X", false, vec![]).is_err());
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
    let allocated;
    {
        let m = store(&dir);
        m.create_bucket("b", BucketOpts::default()).unwrap();
        m.put_object("b", "k", put(1, 0, 4, "etag"), PutCondition::None)
            .unwrap();
        allocated = m.next_object_id().unwrap(); // bump the counter
    }
    let m = store(&dir);
    assert!(m.get_bucket("b").unwrap().is_some());
    assert_eq!(
        m.get_object("b", "k").unwrap().unwrap().etag,
        ETag("etag".into())
    );
    // The id high-water survived the restart: the next id strictly exceeds any
    // previously allocated one. The hi-lo allocator may skip the tail of the
    // pre-restart block (gaps are fine — ids only need to be unique + increasing).
    assert!(m.next_object_id().unwrap() > allocated);
}

#[test]
fn object_ids_unique_and_increasing_across_block_boundary() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    // Allocate well past one hi-lo block (1024) to exercise a refill mid-run.
    let ids: Vec<u64> = (0..2500).map(|_| m.next_object_id().unwrap()).collect();
    // Strictly increasing and contiguous within a single process (no restart).
    assert_eq!(ids.first(), Some(&1));
    for w in ids.windows(2) {
        assert_eq!(w[1], w[0] + 1, "ids contiguous within a run");
    }
}

#[test]
fn object_ids_are_unique_under_concurrency() {
    use std::collections::HashSet;
    use std::sync::Arc;

    let dir = TempDir::new().unwrap();
    let m = Arc::new(store(&dir));
    let mut handles = Vec::new();
    for _ in 0..8 {
        let m = m.clone();
        handles.push(std::thread::spawn(move || {
            (0..500).map(|_| m.next_object_id().unwrap()).collect::<Vec<_>>()
        }));
    }
    let mut all = HashSet::new();
    for h in handles {
        for id in h.join().unwrap() {
            assert!(all.insert(id), "duplicate id {id} handed out");
        }
    }
    assert_eq!(all.len(), 8 * 500);
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

// --- per-bucket quotas -----------------------------------------------------

/// Create bucket "b" with the given quota applied.
fn bucket_with_quota(m: &RedbMetaStore, quota: Quota) {
    m.create_bucket("b", BucketOpts::default()).unwrap();
    m.set_bucket_quota("b", quota).unwrap();
}

#[test]
fn byte_quota_is_enforced_atomically() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    bucket_with_quota(
        &m,
        Quota {
            max_bytes: 100,
            max_objects: 0,
        },
    );

    // Two puts up to the limit succeed.
    m.put_object("b", "k1", put_sized(1, 60, "e1"), PutCondition::None)
        .unwrap();
    m.put_object("b", "k2", put_sized(2, 40, "e2"), PutCondition::None)
        .unwrap();
    assert_eq!(
        m.bucket_usage("b").unwrap(),
        BucketUsage {
            bytes: 100,
            objects: 2
        }
    );

    // The next byte over the limit is rejected, and usage is unchanged (atomic).
    let err = m
        .put_object("b", "k3", put_sized(3, 1, "e3"), PutCondition::None)
        .unwrap_err();
    assert!(matches!(err, soma_meta::Error::QuotaExceeded(_)));
    assert_eq!(
        m.bucket_usage("b").unwrap(),
        BucketUsage {
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
    bucket_with_quota(
        &m,
        Quota {
            max_bytes: 0,
            max_objects: 2,
        },
    );
    m.put_object("b", "k1", put_sized(1, 1, "e1"), PutCondition::None)
        .unwrap();
    m.put_object("b", "k2", put_sized(2, 1, "e2"), PutCondition::None)
        .unwrap();
    assert!(m
        .put_object("b", "k3", put_sized(3, 1, "e3"), PutCondition::None)
        .is_err());
}

#[test]
fn overwrite_refunds_the_previous_version() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    bucket_with_quota(
        &m,
        Quota {
            max_bytes: 100,
            max_objects: 0,
        },
    );
    m.put_object("b", "k", put_sized(1, 80, "e1"), PutCondition::None)
        .unwrap();
    // Overwriting the same key with a larger object: the old 80 bytes are
    // refunded, so 90 fits under the 100-byte limit (90, not 170).
    m.put_object("b", "k", put_sized(2, 90, "e2"), PutCondition::None)
        .unwrap();
    assert_eq!(
        m.bucket_usage("b").unwrap(),
        BucketUsage {
            bytes: 90,
            objects: 1
        }
    );
}

#[test]
fn delete_refunds_quota() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    bucket_with_quota(
        &m,
        Quota {
            max_bytes: 100,
            max_objects: 0,
        },
    );
    m.put_object("b", "k", put_sized(1, 70, "e1"), PutCondition::None)
        .unwrap();
    m.delete_object("b", "k", PutCondition::None).unwrap();
    assert_eq!(m.bucket_usage("b").unwrap(), BucketUsage::default());
    // The freed space is reusable.
    m.put_object("b", "k2", put_sized(2, 90, "e2"), PutCondition::None)
        .unwrap();
    assert_eq!(
        m.bucket_usage("b").unwrap(),
        BucketUsage {
            bytes: 90,
            objects: 1
        }
    );
}

#[test]
fn unconfigured_bucket_skips_accounting() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    // No quota set on the bucket → unlimited, and usage accounting is skipped.
    m.create_bucket("b", BucketOpts::default()).unwrap();
    m.put_object("b", "k", put(1, 0, 1000, "e1"), PutCondition::None)
        .unwrap();
    assert_eq!(m.bucket_usage("b").unwrap(), BucketUsage::default());
}

#[test]
fn quota_can_be_changed_after_creation() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    m.create_bucket("b", BucketOpts::default()).unwrap();
    // Initially unlimited: a large object is accepted.
    m.put_object("b", "k1", put_sized(1, 1000, "e1"), PutCondition::None)
        .unwrap();
    // Tighten the quota below current usage: further growth is rejected, but the
    // already-stored object remains.
    m.set_bucket_quota(
        "b",
        Quota {
            max_bytes: 1000,
            max_objects: 0,
        },
    )
    .unwrap();
    assert!(m
        .put_object("b", "k2", put_sized(2, 1, "e2"), PutCondition::None)
        .is_err());
    assert!(m.get_object("b", "k1").unwrap().is_some());
}

// --- cluster membership + placement groups (M3a) ---------------------------

use soma_meta::{NodeState, NodeTopology, PgPlacement};

fn topo(zone: &str, host: &str) -> NodeTopology {
    NodeTopology {
        zone: zone.to_string(),
        host: host.to_string(),
    }
}

#[test]
fn register_heartbeat_and_list_members() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);

    m.register_node("node-a", "http://a:9200", topo("az-1", "host-a"), 100)
        .unwrap();
    m.register_node("node-b", "http://b:9200", NodeTopology::default(), 100)
        .unwrap();

    let members = m.list_members().unwrap();
    assert_eq!(members.len(), 2);
    let a = members.iter().find(|n| n.node_id == "node-a").unwrap();
    assert_eq!(a.endpoint, "http://a:9200");
    assert_eq!(a.state, NodeState::Active);
    assert_eq!(a.last_heartbeat, 100);
    assert_eq!(a.generation, 1);
    // Topology is recorded for data-locality scheduling.
    assert_eq!(a.zone, "az-1");
    assert_eq!(a.host, "host-a");

    // Heartbeat advances the clock without changing identity.
    m.heartbeat("node-a", 175).unwrap();
    let a = m
        .list_members()
        .unwrap()
        .into_iter()
        .find(|n| n.node_id == "node-a")
        .unwrap();
    assert_eq!(a.last_heartbeat, 175);
    assert_eq!(a.generation, 1);

    // Re-registering (e.g. after a restart) bumps the generation.
    m.register_node("node-a", "http://a:9200", topo("az-1", "host-a"), 200)
        .unwrap();
    let a = m
        .list_members()
        .unwrap()
        .into_iter()
        .find(|n| n.node_id == "node-a")
        .unwrap();
    assert_eq!(a.generation, 2);
}

#[test]
fn heartbeat_for_unknown_node_errors() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    let err = m.heartbeat("ghost", 1).unwrap_err();
    assert!(matches!(err, soma_meta::Error::UnknownNode(_)));
}

#[test]
fn pg_table_seeds_once_and_reads_back() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);

    let entries: Vec<(u32, PgPlacement)> = (0..4u32)
        .map(|pg| {
            (
                pg,
                PgPlacement {
                    node_ids: vec![format!("n{}", pg % 3), format!("n{}", (pg + 1) % 3)],
                    target: Vec::new(),
                    generation: 1,
                },
            )
        })
        .collect();

    // First seed writes the table.
    assert!(m.seed_pg_table(&entries).unwrap());
    let table = m.list_pg_table().unwrap();
    assert_eq!(table.len(), 4);
    assert_eq!(table[0].0, 0);
    assert_eq!(
        table[0].1.node_ids,
        vec!["n0".to_string(), "n1".to_string()]
    );

    // A second seed is a no-op (table already populated) — leaves it unchanged.
    let other: Vec<(u32, PgPlacement)> = vec![(
        0,
        PgPlacement {
            node_ids: vec!["different".to_string()],
            target: Vec::new(),
            generation: 9,
        },
    )];
    assert!(!m.seed_pg_table(&other).unwrap());
    assert_eq!(m.list_pg_table().unwrap().len(), 4);
    assert_eq!(
        m.list_pg_table().unwrap()[0].1.node_ids,
        vec!["n0".to_string(), "n1".to_string()]
    );
}

#[test]
fn membership_and_pg_table_persist_across_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let m = store(&dir);
        m.register_node("n1", "http://n1:9200", topo("az-9", "host-n1"), 50)
            .unwrap();
        m.seed_pg_table(&[(
            0,
            PgPlacement {
                node_ids: vec!["n1".to_string()],
                target: Vec::new(),
                generation: 1,
            },
        )])
        .unwrap();
    }
    let m = store(&dir);
    assert_eq!(m.list_members().unwrap().len(), 1);
    assert_eq!(m.list_pg_table().unwrap().len(), 1);
}

#[test]
fn pg_migration_begin_and_finalize() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    m.seed_pg_table(&[(
        0,
        PgPlacement {
            node_ids: vec!["n0".to_string(), "n1".to_string()],
            target: Vec::new(),
            generation: 1,
        },
    )])
    .unwrap();

    // Begin: records the target and bumps the generation.
    m.begin_migration(0, vec!["n1".to_string(), "n2".to_string()])
        .unwrap();
    let p = m.pg_placement(0).unwrap().unwrap();
    assert!(p.is_migrating());
    assert_eq!(p.target, vec!["n1".to_string(), "n2".to_string()]);
    assert_eq!(p.node_ids, vec!["n0".to_string(), "n1".to_string()]); // acting unchanged
    assert_eq!(p.generation, 2);

    // Finalize: target becomes acting, migration clears.
    m.finalize_migration(0).unwrap();
    let p = m.pg_placement(0).unwrap().unwrap();
    assert!(!p.is_migrating());
    assert_eq!(p.node_ids, vec!["n1".to_string(), "n2".to_string()]);
    assert_eq!(p.generation, 3);

    // Finalize again is a no-op (not migrating).
    m.finalize_migration(0).unwrap();
    assert_eq!(m.pg_placement(0).unwrap().unwrap().generation, 3);
}

#[test]
fn list_object_ids_returns_committed_objects() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    m.create_bucket("b", BucketOpts::default()).unwrap();
    m.put_object("b", "k1", put(10, 0, 3, "e1"), PutCondition::None)
        .unwrap();
    m.put_object("b", "k2", put(20, 0, 3, "e2"), PutCondition::None)
        .unwrap();
    let mut ids = m.list_object_ids().unwrap();
    ids.sort();
    assert_eq!(ids, vec![10, 20]);
}

// --- orphan garbage tracking (hardening) -----------------------------------

#[test]
fn overwrite_and_delete_record_garbage() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    m.create_bucket("b", BucketOpts::default()).unwrap();

    // First write: no prior version → no garbage.
    m.put_object("b", "k", put(1, 0, 3, "e1"), PutCondition::None)
        .unwrap();
    assert!(m.list_garbage(100).unwrap().is_empty());

    // Overwrite: the old id (1) becomes garbage.
    m.put_object("b", "k", put(2, 0, 3, "e2"), PutCondition::None)
        .unwrap();
    assert_eq!(m.list_garbage(100).unwrap(), vec![1]);

    // Delete: the current id (2) becomes garbage too.
    m.delete_object("b", "k", PutCondition::None).unwrap();
    let mut g = m.list_garbage(100).unwrap();
    g.sort();
    assert_eq!(g, vec![1, 2]);

    // Reclaiming clears them.
    m.remove_garbage(&[1]).unwrap();
    assert_eq!(m.list_garbage(100).unwrap(), vec![2]);
}

#[test]
fn mark_garbage_and_limit() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    m.mark_garbage(&[10, 20, 30]).unwrap();
    assert_eq!(m.list_garbage(2).unwrap().len(), 2); // limited
    assert_eq!(m.list_garbage(100).unwrap().len(), 3);
    m.remove_garbage(&[10, 20, 30]).unwrap();
    assert!(m.list_garbage(100).unwrap().is_empty());
}

#[test]
fn bucket_default_encryption_set_get_clear() {
    use soma_meta::SseAlgorithm;
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    m.create_bucket("b", BucketOpts::default()).unwrap();
    assert!(m.get_bucket("b").unwrap().unwrap().default_sse.is_none()); // default off

    m.set_bucket_encryption("b", Some(SseAlgorithm::Aes256))
        .unwrap();
    assert_eq!(
        m.get_bucket("b").unwrap().unwrap().default_sse,
        Some(SseAlgorithm::Aes256)
    );

    m.set_bucket_encryption("b", None).unwrap();
    assert!(m.get_bucket("b").unwrap().unwrap().default_sse.is_none());

    // Missing bucket errors.
    assert!(matches!(
        m.set_bucket_encryption("nope", Some(SseAlgorithm::Aes256)),
        Err(soma_meta::Error::NoSuchBucket(_))
    ));
}

#[test]
fn put_object_batch_mixed_outcomes_and_durable() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    m.create_bucket("b1", BucketOpts::default()).unwrap();

    // One transaction, independent per-item outcomes:
    //  0: fresh key "a"                         -> Ok(v1)
    //  1: key "a" again, If-None-Match          -> Err (item 0 is visible in-txn)
    //  2: key "b" in a missing bucket           -> Err(NoSuchBucket)
    //  3: fresh key "c"                         -> Ok(v1)
    let results = m.put_object_batch(vec![
        item("b1", "a", put(1, 0, 10, "a1"), PutCondition::None),
        item("b1", "a", put(2, 0, 10, "a2"), PutCondition::IfNoneMatch),
        item("nope", "b", put(3, 0, 10, "b1"), PutCondition::None),
        item("b1", "c", put(4, 0, 10, "c1"), PutCondition::None),
    ]);

    assert_eq!(results.len(), 4);
    assert_eq!(*results[0].as_ref().unwrap(), Version(1));
    assert!(results[1].is_err(), "intra-batch If-None-Match must see item 0");
    assert!(results[2].is_err(), "missing bucket must fail just its item");
    assert_eq!(*results[3].as_ref().unwrap(), Version(1));

    // A neighbour's failure must not have rolled back the successes — and they
    // survive a reopen (the batch's single commit was durable).
    drop(m);
    let m = store(&dir);
    assert_eq!(m.get_object("b1", "a").unwrap().unwrap().object_id, 1);
    assert_eq!(m.get_object("b1", "c").unwrap().unwrap().object_id, 4);
    assert!(m.get_object("b1", "b").unwrap().is_none());
}

#[test]
fn put_object_batch_quota_accrues_within_batch() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);
    m.create_bucket("b1", BucketOpts::default()).unwrap();
    m.set_bucket_quota(
        "b1",
        Quota {
            max_bytes: 0,
            max_objects: 2,
        },
    )
    .unwrap();

    // Three fresh objects, limit of 2: usage accumulates across the batch, so the
    // third is rejected while the first two commit.
    let results = m.put_object_batch(vec![
        item("b1", "k1", put(1, 0, 5, "e1"), PutCondition::None),
        item("b1", "k2", put(2, 0, 5, "e2"), PutCondition::None),
        item("b1", "k3", put(3, 0, 5, "e3"), PutCondition::None),
    ]);
    assert!(results[0].is_ok());
    assert!(results[1].is_ok());
    assert!(results[2].is_err(), "third object exceeds the 2-object quota");

    let usage = m.bucket_usage("b1").unwrap();
    assert_eq!(usage, BucketUsage { bytes: 10, objects: 2 });
    assert!(m.get_object("b1", "k3").unwrap().is_none());
}

#[test]
fn reserve_object_ids_blocks_are_disjoint() {
    let dir = TempDir::new().unwrap();
    let m = store(&dir);

    let (s1, l1) = m.reserve_object_ids(10).unwrap();
    assert_eq!(l1, 10);
    let (s2, l2) = m.reserve_object_ids(5).unwrap();
    assert_eq!(l2, 5);
    assert!(s2 >= s1 + l1, "reserved blocks must not overlap");

    // The in-process allocator shares the same counter — a direct next_object_id
    // never collides with a reserved block.
    let single = m.next_object_id().unwrap();
    assert!(single >= s2 + l2, "single id must not fall inside a reserved block");

    // count is clamped to at least 1.
    let (_, l3) = m.reserve_object_ids(0).unwrap();
    assert_eq!(l3, 1);
}
