//! Integration tests for `LocalFsBackend` against a real temp directory.
//!
//! The backend is addressed by `object_id`; the physical byte location is an
//! internal detail resolved by the node-local index.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use soma_backend::{BackendConfig, ByteRange, LocalFsBackend, StorageBackend};
use tempfile::TempDir;

fn open(dir: &TempDir) -> LocalFsBackend {
    LocalFsBackend::open(dir.path(), BackendConfig::default()).unwrap()
}

fn open_with(dir: &TempDir, volume_max: u64) -> LocalFsBackend {
    LocalFsBackend::open(dir.path(), BackendConfig { volume_max }).unwrap()
}

fn vol_count(dir: &TempDir) -> usize {
    std::fs::read_dir(dir.path().join("volumes"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("vol"))
        .count()
}

#[test]
fn put_then_get_roundtrip() {
    let dir = TempDir::new().unwrap();
    let be = open(&dir);
    let payload = b"the quick brown fox";
    be.put(1, payload).unwrap();
    assert_eq!(be.get(1, None).unwrap(), payload);
}

#[test]
fn empty_object_roundtrip() {
    let dir = TempDir::new().unwrap();
    let be = open(&dir);
    be.put(7, b"").unwrap();
    assert_eq!(be.get(7, None).unwrap(), b"");
}

#[test]
fn missing_object_errors() {
    let dir = TempDir::new().unwrap();
    let be = open(&dir);
    assert!(be.get(42, None).is_err());
}

#[test]
fn range_get() {
    let dir = TempDir::new().unwrap();
    let be = open(&dir);
    be.put(1, b"0123456789").unwrap();
    let got = be
        .get(
            1,
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
    be.put(1, b"short").unwrap();
    assert!(be
        .get(
            1,
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
    {
        let be = open(&dir);
        for i in 0..20u64 {
            be.put(i, format!("object-{i}").as_bytes()).unwrap();
        }
    }
    // Reopen from scratch — the id index must rebuild by scanning the volume.
    let be = open(&dir);
    for i in 0..20u64 {
        assert_eq!(be.get(i, None).unwrap(), format!("object-{i}").as_bytes());
    }
}

#[test]
fn reopen_after_checkpoint() {
    let dir = TempDir::new().unwrap();
    {
        let be = open(&dir);
        be.put(99, b"checkpointed").unwrap();
        be.checkpoint().unwrap();
    }
    let be = open(&dir);
    assert_eq!(be.get(99, None).unwrap(), b"checkpointed");
}

#[test]
fn delete_makes_object_unreadable() {
    let dir = TempDir::new().unwrap();
    let be = open(&dir);
    be.put(1, b"data").unwrap();
    assert_eq!(be.get(1, None).unwrap(), b"data");
    be.delete(1).unwrap();
    assert!(be.get(1, None).is_err());

    // The tombstone survives a reopen.
    drop(be);
    let be = open(&dir);
    assert!(be.get(1, None).is_err());
}

#[test]
fn rotation_spans_multiple_volumes() {
    let dir = TempDir::new().unwrap();
    // Tiny cap so the second needle forces a rotation.
    let be = open_with(&dir, 64);
    be.put(1, &[0xAA; 40]).unwrap();
    be.put(2, &[0xBB; 40]).unwrap();
    assert_eq!(vol_count(&dir), 2);
    assert_eq!(be.get(1, None).unwrap(), vec![0xAA; 40]);
    assert_eq!(be.get(2, None).unwrap(), vec![0xBB; 40]);

    // And both survive a reopen (id index spans volumes).
    drop(be);
    let be = open_with(&dir, 64);
    assert_eq!(be.get(1, None).unwrap(), vec![0xAA; 40]);
    assert_eq!(be.get(2, None).unwrap(), vec![0xBB; 40]);
}

#[test]
fn torn_tail_is_recovered() {
    use std::io::Write;

    let dir = TempDir::new().unwrap();
    {
        let be = open(&dir);
        be.put(1, b"intact object").unwrap();
    }

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
    assert_eq!(be.get(1, None).unwrap(), b"intact object");

    // A fresh write succeeds and lands at the clean boundary (be readable).
    be.put(2, b"after recovery").unwrap();
    assert_eq!(be.get(2, None).unwrap(), b"after recovery");
}

#[test]
fn newest_write_for_same_id_wins() {
    let dir = TempDir::new().unwrap();
    {
        let be = open(&dir);
        be.put(5, b"v1").unwrap();
        be.put(5, b"v2").unwrap(); // same id -> newest wins
        assert_eq!(be.get(5, None).unwrap(), b"v2");
    }
    // The rebuilt id index also resolves to the newest write.
    let be = open(&dir);
    assert_eq!(be.get(5, None).unwrap(), b"v2");
}
