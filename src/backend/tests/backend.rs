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
fn scrub_detects_payload_corruption() {
    let dir = TempDir::new().unwrap();
    {
        let be = open(&dir);
        be.put(1, b"aaaa").unwrap();
        be.put(2, b"bbbb").unwrap();
        be.sync().unwrap();
    }

    // Corrupt object 2's first payload byte directly in the volume file.
    // Needle 1: 32B header + 4B data + 4B pad = 40B; needle 2 header at 40, data
    // begins at 40 + 32 = 72.
    let vol = dir.path().join("volumes").join("0000000001.vol");
    let mut bytes = std::fs::read(&vol).unwrap();
    bytes[72] ^= 0xFF;
    std::fs::write(&vol, &bytes).unwrap();

    let be = open(&dir);
    let report = be.scrub().unwrap();
    assert_eq!(report.checked, 2);
    assert_eq!(report.corrupt, vec![2u64]);
    // The corrupted object also fails to read; the intact one is fine.
    assert!(be.get(2, None).is_err());
    assert_eq!(be.get(1, None).unwrap(), b"aaaa");
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

// --- compaction (space reclamation) ----------------------------------------

fn total_vol_bytes(dir: &TempDir) -> u64 {
    std::fs::read_dir(dir.path().join("volumes"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("vol"))
        .map(|e| e.metadata().unwrap().len())
        .sum()
}

#[test]
fn compaction_reclaims_deleted_and_keeps_live() {
    let dir = TempDir::new().unwrap();
    // Tiny volume_max + sizable payloads → each object lands in its own volume,
    // so deleted objects sit in sealed (compactable) volumes.
    let be = open_with(&dir, 64);
    let payload = |i: u64| [i as u8; 200];
    for i in 1..=6 {
        be.put(i, &payload(i)).unwrap();
    }
    for i in [1u64, 2, 3] {
        be.delete(i).unwrap();
    }

    let before = total_vol_bytes(&dir);
    let report = be.compact(0.0).unwrap();
    let after = total_vol_bytes(&dir);

    assert!(
        report.bytes_reclaimed > 0,
        "should reclaim deleted needle space"
    );
    assert!(after < before, "volumes should shrink: {after} < {before}");
    for i in [1u64, 2, 3] {
        assert!(matches!(
            be.get(i, None),
            Err(soma_backend::Error::ObjectNotFound(_))
        ));
    }
    for i in [4u64, 5, 6] {
        assert_eq!(be.get(i, None).unwrap(), payload(i));
    }
}

#[test]
fn compaction_reclaims_superseded_same_id() {
    let dir = TempDir::new().unwrap();
    let be = open_with(&dir, 64);
    be.put(1, &[9u8; 200]).unwrap(); // sealed
    be.put(2, &[8u8; 200]).unwrap();
    be.put(1, &[7u8; 200]).unwrap(); // re-put id 1 → the first needle is dead

    let before = total_vol_bytes(&dir);
    let report = be.compact(0.0).unwrap();
    let after = total_vol_bytes(&dir);

    assert!(report.bytes_reclaimed > 0 && after < before);
    assert_eq!(be.get(1, None).unwrap(), [7u8; 200]); // newest wins
    assert_eq!(be.get(2, None).unwrap(), [8u8; 200]);
}

#[test]
fn compaction_is_noop_without_garbage() {
    let dir = TempDir::new().unwrap();
    let be = open_with(&dir, 64);
    for i in 1..=4 {
        be.put(i, &[i as u8; 200]).unwrap();
    }
    let report = be.compact(0.0).unwrap();
    assert_eq!(report.bytes_reclaimed, 0);
    assert_eq!(report.volumes_compacted, 0);
    for i in 1..=4 {
        assert_eq!(be.get(i, None).unwrap(), [i as u8; 200]);
    }
}

#[test]
fn compacted_volumes_survive_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let be = open_with(&dir, 64);
        for i in 1..=6 {
            be.put(i, &[i as u8; 200]).unwrap();
        }
        for i in [1u64, 2, 3] {
            be.delete(i).unwrap();
        }
        be.compact(0.0).unwrap();
    }
    // Reopen: the rebuild from compacted volumes preserves the live/deleted state.
    let be = open(&dir);
    for i in [1u64, 2, 3] {
        assert!(be.get(i, None).is_err());
    }
    for i in [4u64, 5, 6] {
        assert_eq!(be.get(i, None).unwrap(), [i as u8; 200]);
    }
    // And the backend keeps working after compaction + reopen.
    be.put(99, b"after compaction").unwrap();
    assert_eq!(be.get(99, None).unwrap(), b"after compaction");
}
