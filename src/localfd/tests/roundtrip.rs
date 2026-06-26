//! End-to-end: store objects in a real backend, serve the local socket, and read
//! them back through the passed descriptor — proving the bytes never cross the
//! socket (only the fd does) and the CRC verifies.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use soma_backend::{BackendConfig, LocalFsBackend, LocalReader, StorageBackend};
use soma_localfd::{serve_local_reads, Error, LocalClient, LocalRead};
use tempfile::TempDir;

/// Read the payload framed by a `LocalRead` straight from the passed descriptor —
/// the bytes come from the volume file the fd references, never over the socket.
fn pread_payload(read: &LocalRead) -> Vec<u8> {
    let mut buf = vec![0u8; read.len as usize];
    let n = nix::sys::uio::pread(&read.fd, &mut buf, read.payload_offset as i64).unwrap();
    assert_eq!(n, buf.len());
    buf
}

#[test]
fn fd_passing_roundtrip_and_crc() {
    let dir = TempDir::new().unwrap();
    let backend = Arc::new(LocalFsBackend::open(dir.path(), BackendConfig::default()).unwrap());

    // Store a couple of objects of different sizes.
    let payload_a = b"the quick brown fox jumps over the lazy dog".to_vec();
    let payload_b = vec![0xABu8; 100_000];
    backend.put(1, &payload_a).unwrap();
    backend.put(2, &payload_b).unwrap();

    let reader: Arc<dyn LocalReader> = backend.clone();
    let server = serve_local_reads(dir.path().join("storage.sock"), reader).unwrap();

    let mut client = LocalClient::connect(server.path()).unwrap();

    // Object 1: read via the passed fd, verify bytes + CRC.
    let r1 = client.read_fd(1).unwrap();
    assert_eq!(r1.len as usize, payload_a.len());
    let got_a = pread_payload(&r1);
    assert_eq!(got_a, payload_a);
    assert_eq!(crc32c::crc32c(&got_a), r1.crc, "payload CRC must match");

    // Object 2 (large): same connection (keep-alive), verify.
    let r2 = client.read_fd(2).unwrap();
    assert_eq!(r2.len as usize, payload_b.len());
    let got_b = pread_payload(&r2);
    assert_eq!(got_b, payload_b);
    assert_eq!(crc32c::crc32c(&got_b), r2.crc);

    // A sub-range read is just a slice of the mapped payload region.
    let sub = &pread_payload(&r1)[4..9];
    assert_eq!(sub, b"quick");

    // Missing object → NotFound, connection still usable afterwards.
    assert!(matches!(client.read_fd(999), Err(Error::NotFound)));
    let r1_again = client.read_fd(1).unwrap();
    assert_eq!(r1_again.len as usize, payload_a.len());

    drop(server);
}

#[test]
fn deleted_object_is_not_found() {
    let dir = TempDir::new().unwrap();
    let backend = Arc::new(LocalFsBackend::open(dir.path(), BackendConfig::default()).unwrap());
    backend.put(7, b"ephemeral").unwrap();
    backend.delete(7).unwrap();

    let reader: Arc<dyn LocalReader> = backend.clone();
    let server = serve_local_reads(dir.path().join("s.sock"), reader).unwrap();
    let mut client = LocalClient::connect(server.path()).unwrap();
    assert!(matches!(client.read_fd(7), Err(Error::NotFound)));
}

#[test]
fn oversized_needle_in_its_own_volume() {
    let dir = TempDir::new().unwrap();
    // A tiny volume_max so a larger object becomes an oversized needle that gets its
    // own volume (the "an empty volume always accepts at least one needle" path).
    let backend = Arc::new(
        LocalFsBackend::open(dir.path(), BackendConfig { volume_max: 256 * 1024, ..Default::default() }).unwrap(),
    );
    let big = vec![0x5Au8; 1024 * 1024]; // 1 MiB > 256 KiB volume_max
    backend.put(1, &big).unwrap();

    let reader: Arc<dyn LocalReader> = backend;
    let server = serve_local_reads(dir.path().join("s.sock"), reader).unwrap();
    let mut client = LocalClient::connect(server.path()).unwrap();

    let r = client.read_fd(1).unwrap();
    assert_eq!(r.len as usize, big.len());
    let got = pread_payload(&r);
    assert_eq!(got, big);
    assert_eq!(crc32c::crc32c(&got), r.crc);
}
