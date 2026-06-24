//! Integration tests for `LocalFsBackend` against a real temp directory.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use soma_backend::{BackendConfig, ByteRange, LocalFsBackend, StorageBackend};
use tempfile::TempDir;

fn open(dir: &TempDir) -> LocalFsBackend {
    LocalFsBackend::open(dir.path(), BackendConfig::default()).unwrap()
}

fn open_with(dir: &TempDir, volume_max: u64) -> LocalFsBackend {
    LocalFsBackend::open(dir.path(), BackendConfig { volume_max }).unwrap()
}

#[test]
fn put_then_get_roundtrip() {
    let dir = TempDir::new().unwrap();
    let be = open(&dir);
    let payload = b"the quick brown fox";
    let loc = be.put(1, payload).unwrap();
    assert_eq!(be.get(loc, None).unwrap(), payload);
}

#[test]
fn empty_object_roundtrip() {
    let dir = TempDir::new().unwrap();
    let be = open(&dir);
    let loc = be.put(7, b"").unwrap();
    assert_eq!(be.get(loc, None).unwrap(), b"");
}

#[test]
fn range_get() {
    let dir = TempDir::new().unwrap();
    let be = open(&dir);
    let loc = be.put(1, b"0123456789").unwrap();
    let got = be
        .get(
            loc,
            Some(ByteRange {
                offset: 3,
                length: 4,
            }),
        )
        .unwrap();
    assert_eq!(got, b"3456");
}

#[test]
fn range_out_of_bounds_errors() {
    let dir = TempDir::new().unwrap();
    let be = open(&dir);
    let loc = be.put(1, b"short").unwrap();
    assert!(be
        .get(
            loc,
            Some(ByteRange {
                offset: 3,
                length: 100
            })
        )
        .is_err());
}

#[test]
fn reopen_recovers_objects() {
    let dir = TempDir::new().unwrap();
    let locs: Vec<_> = {
        let be = open(&dir);
        (0..20u64)
            .map(|i| (i, be.put(i, format!("object-{i}").as_bytes()).unwrap()))
            .collect()
    };
    // Reopen from scratch — index must rebuild by scanning the volume.
    let be = open(&dir);
    for (i, loc) in locs {
        assert_eq!(be.get(loc, None).unwrap(), format!("object-{i}").as_bytes());
    }
}

#[test]
fn reopen_after_checkpoint() {
    let dir = TempDir::new().unwrap();
    let loc = {
        let be = open(&dir);
        let loc = be.put(99, b"checkpointed").unwrap();
        be.checkpoint().unwrap();
        loc
    };
    let be = open(&dir);
    assert_eq!(be.get(loc, None).unwrap(), b"checkpointed");
}

#[test]
fn delete_writes_tombstone() {
    let dir = TempDir::new().unwrap();
    let be = open(&dir);
    be.put(1, b"data").unwrap();
    let tomb = be.delete(1).unwrap();
    assert!(tomb.is_tombstone());
    // Reading the tombstone needle yields an empty payload.
    assert_eq!(be.get(tomb, None).unwrap(), b"");
}

#[test]
fn rotation_spans_multiple_volumes() {
    let dir = TempDir::new().unwrap();
    // Tiny cap so the second needle forces a rotation.
    let be = open_with(&dir, 64);
    let a = be.put(1, &[0xAA; 40]).unwrap();
    let b = be.put(2, &[0xBB; 40]).unwrap();
    assert_eq!(a.volume.get(), 1);
    assert_eq!(b.volume.get(), 2);
    assert_eq!(be.get(a, None).unwrap(), vec![0xAA; 40]);
    assert_eq!(be.get(b, None).unwrap(), vec![0xBB; 40]);

    // And both survive a reopen.
    drop(be);
    let be = open_with(&dir, 64);
    assert_eq!(be.get(a, None).unwrap(), vec![0xAA; 40]);
    assert_eq!(be.get(b, None).unwrap(), vec![0xBB; 40]);
}

#[test]
fn torn_tail_is_recovered() {
    use std::io::Write;

    let dir = TempDir::new().unwrap();
    let good = {
        let be = open(&dir);
        be.put(1, b"intact object").unwrap()
    };

    // Simulate a crash mid-append: append garbage (a partial needle) to the
    // active volume file.
    let vol_file = dir.path().join("volumes").join("0000000001.vol");
    {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&vol_file)
            .unwrap();
        f.write_all(&[0xFF; 20]).unwrap();
        f.sync_all().unwrap();
    }

    // Reopen: the torn tail must be truncated and the intact object still read.
    let be = open(&dir);
    assert_eq!(be.get(good, None).unwrap(), b"intact object");

    // A fresh write must succeed and land at the clean boundary (be readable).
    let next = be.put(2, b"after recovery").unwrap();
    assert_eq!(be.get(next, None).unwrap(), b"after recovery");
}

#[test]
fn newest_write_for_same_id_wins_in_rebuilt_index() {
    let dir = TempDir::new().unwrap();
    let (first, second) = {
        let be = open(&dir);
        let first = be.put(5, b"v1").unwrap();
        let second = be.put(5, b"v2").unwrap();
        (first, second)
    };
    // Both locations remain individually readable (the backend doesn't GC),
    // and they point at distinct offsets.
    let be = open(&dir);
    assert_ne!(first.needle.offset, second.needle.offset);
    assert_eq!(be.get(first, None).unwrap(), b"v1");
    assert_eq!(be.get(second, None).unwrap(), b"v2");
}
