//! Envelope encryption at rest (M4a).
//!
//! [`EncryptingBackend`] is a transparent [`StorageBackend`] decorator that seals
//! object bytes before they reach the inner backend — so storage nodes, and the
//! replication / erasure-coding layer below, only ever see ciphertext.
//!
//! **Envelope scheme.** Each object gets a fresh random 256-bit *data encryption
//! key* (DEK), wrapped by the *master key* (KEK) and stored alongside the payload
//! so the bytes are self-describing — no per-object key database is needed.
//!
//! **Chunked AEAD for seekable range reads.** The payload is split into fixed-size
//! chunks, each sealed *independently* with AES-256-GCM (its own nonce + tag), so
//! a range read fetches and decrypts only the chunks covering the requested window
//! — not the whole object — while every chunk is still authenticated:
//!
//! ```text
//! frame = [ version:1 ][ kek_nonce:12 ][ wrapped_dek:48 ][ chunk_size:4 ][ orig_len:8 ]
//!         [ chunk_0+tag ][ chunk_1+tag ] ...
//! ```
//!
//! A read reads just the header (for the DEK + layout), then the ciphertext bytes
//! of the covering chunks (one ranged fetch from the inner backend), decrypts them,
//! and slices. GCM authentication means a wrong master key or tampered bytes
//! surface as an [`Error::Crypto`] rather than silently wrong data.

use std::sync::Arc;

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
use rand::rngs::OsRng;
use rand::RngCore;
use soma_core::ObjectId;

use crate::error::{Error, Result};
use crate::{ByteRange, StorageBackend};

/// Frame format version (the first stored byte). v2 is the chunked layout.
const VERSION: u8 = 2;
/// AES-GCM nonce length.
const NONCE: usize = 12;
/// Data encryption key length (AES-256).
const DEK: usize = 32;
/// AES-GCM authentication tag length.
const TAG: usize = 16;
/// A DEK wrapped by the KEK: AES-256-GCM ciphertext (32) + tag (16).
const WRAPPED_DEK: usize = DEK + TAG;
/// Default plaintext chunk size (each chunk is sealed independently for seekable
/// range reads).
const DEFAULT_CHUNK: usize = 64 * 1024;
/// Self-describing header: `version(1) | kek_nonce(12) | wrapped_dek(48) |
/// chunk_size(4) | orig_len(8)`, followed by the per-chunk ciphertexts.
const HEADER: usize = 1 + NONCE + WRAPPED_DEK + 4 + 8;

/// Source of the master key (KEK) that wraps per-object data keys.
///
/// A trait so an external KMS (AWS KMS / Vault) can replace the in-process static
/// key later without touching [`EncryptingBackend`].
pub trait KeyProvider: Send + Sync {
    /// The 256-bit master key used to wrap and unwrap per-object data keys.
    fn master_key(&self) -> &[u8; 32];
}

/// A [`KeyProvider`] holding a single master key from config / a Kubernetes Secret.
pub struct StaticKeyProvider {
    key: [u8; 32],
}

impl StaticKeyProvider {
    /// Build from a raw 32-byte master key.
    pub fn new(key: [u8; 32]) -> Self {
        Self { key }
    }

    /// Decode a base64 (standard, padded) 32-byte master key.
    pub fn from_base64(encoded: &str) -> Result<Self> {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded.trim())
            .map_err(|_| Error::Crypto("master key is not valid base64"))?;
        let key: [u8; 32] = bytes
            .try_into()
            .map_err(|_| Error::Crypto("master key must decode to 32 bytes"))?;
        Ok(Self { key })
    }
}

impl KeyProvider for StaticKeyProvider {
    fn master_key(&self) -> &[u8; 32] {
        &self.key
    }
}

/// A [`StorageBackend`] decorator that encrypts payloads at rest (see module docs).
pub struct EncryptingBackend {
    inner: Arc<dyn StorageBackend>,
    kek: Aes256Gcm,
    chunk_size: usize,
}

impl EncryptingBackend {
    /// Wrap `inner`, sealing every payload under the master key from `keys`.
    pub fn new(inner: Arc<dyn StorageBackend>, keys: &dyn KeyProvider) -> Self {
        let kek = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(keys.master_key()));
        Self {
            inner,
            kek,
            chunk_size: DEFAULT_CHUNK,
        }
    }

    /// Override the chunk size (smaller = finer range granularity, more tag
    /// overhead). Mainly for tests.
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size.max(1);
        self
    }

    /// The nonce for chunk `i` — deterministic but unique within an object (the
    /// per-object DEK is random, so `(DEK, nonce)` never repeats).
    fn chunk_nonce(i: u64) -> [u8; NONCE] {
        let mut n = [0u8; NONCE];
        n[NONCE - 8..].copy_from_slice(&i.to_be_bytes());
        n
    }

    /// Seal `data` into a chunked, self-describing frame.
    fn seal(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut dek = [0u8; DEK];
        OsRng.fill_bytes(&mut dek);
        let mut kek_nonce = [0u8; NONCE];
        OsRng.fill_bytes(&mut kek_nonce);
        let wrapped = self
            .kek
            .encrypt(Nonce::from_slice(&kek_nonce), dek.as_slice())
            .map_err(|_| Error::Crypto("key wrap failed"))?;
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&dek));

        let mut frame = Vec::with_capacity(HEADER + data.len() + TAG);
        frame.push(VERSION);
        frame.extend_from_slice(&kek_nonce);
        frame.extend_from_slice(&wrapped);
        frame.extend_from_slice(&(self.chunk_size as u32).to_be_bytes());
        frame.extend_from_slice(&(data.len() as u64).to_be_bytes());
        for (i, chunk) in data.chunks(self.chunk_size).enumerate() {
            let ct = cipher
                .encrypt(Nonce::from_slice(&Self::chunk_nonce(i as u64)), chunk)
                .map_err(|_| Error::Crypto("payload encryption failed"))?;
            frame.extend_from_slice(&ct);
        }
        Ok(frame)
    }

    /// Parse a frame header, unwrapping the DEK → `(cipher, chunk_size, orig_len)`.
    fn open_header(&self, header: &[u8]) -> Result<(Aes256Gcm, usize, u64)> {
        if header.len() < HEADER || header[0] != VERSION {
            return Err(Error::Crypto("malformed or unknown ciphertext frame"));
        }
        let kek_nonce = &header[1..1 + NONCE];
        let wrapped = &header[1 + NONCE..1 + NONCE + WRAPPED_DEK];
        let cs = 1 + NONCE + WRAPPED_DEK;
        let chunk_size = u32::from_be_bytes(
            header[cs..cs + 4]
                .try_into()
                .map_err(|_| Error::Crypto("bad header"))?,
        ) as usize;
        let orig_len = u64::from_be_bytes(
            header[cs + 4..cs + 12]
                .try_into()
                .map_err(|_| Error::Crypto("bad header"))?,
        );
        if chunk_size == 0 {
            return Err(Error::Crypto("invalid chunk size"));
        }
        let dek = self
            .kek
            .decrypt(Nonce::from_slice(kek_nonce), wrapped)
            .map_err(|_| Error::Crypto("key unwrap failed (wrong master key?)"))?;
        if dek.len() != DEK {
            return Err(Error::Crypto("unwrapped data key has the wrong length"));
        }
        Ok((
            Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&dek)),
            chunk_size,
            orig_len,
        ))
    }

    /// Frame byte offset where chunk `i`'s ciphertext starts.
    fn chunk_ct_offset(chunk_size: usize, i: usize) -> usize {
        HEADER + i * (chunk_size + TAG)
    }

    /// Ciphertext length of chunk `i` (the last chunk is shorter).
    fn chunk_ct_len(chunk_size: usize, orig_len: u64, i: usize) -> usize {
        let pt_start = i as u64 * chunk_size as u64;
        let pt = orig_len.saturating_sub(pt_start).min(chunk_size as u64) as usize;
        pt + TAG
    }

    /// Decrypt consecutive chunks (starting at chunk index `first`) from `ct`,
    /// returning their concatenated plaintext.
    fn decrypt_chunks(
        cipher: &Aes256Gcm,
        chunk_size: usize,
        orig_len: u64,
        first: usize,
        ct: &[u8],
    ) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(ct.len());
        let mut pos = 0usize;
        let mut i = first;
        while pos < ct.len() && (i as u64 * chunk_size as u64) < orig_len {
            let this = Self::chunk_ct_len(chunk_size, orig_len, i);
            if pos + this > ct.len() {
                return Err(Error::Crypto("truncated ciphertext chunk"));
            }
            let dec = cipher
                .decrypt(
                    Nonce::from_slice(&Self::chunk_nonce(i as u64)),
                    &ct[pos..pos + this],
                )
                .map_err(|_| Error::Crypto("payload authentication failed"))?;
            out.extend_from_slice(&dec);
            pos += this;
            i += 1;
        }
        Ok(out)
    }
}

impl StorageBackend for EncryptingBackend {
    fn put(&self, object_id: ObjectId, data: &[u8]) -> Result<()> {
        let frame = self.seal(data)?;
        self.inner.put(object_id, &frame)
    }

    fn get(&self, object_id: ObjectId, range: Option<ByteRange>) -> Result<Vec<u8>> {
        match range {
            None => {
                let frame = self.inner.get(object_id, None)?;
                let (cipher, chunk_size, orig_len) = self.open_header(&frame)?;
                Self::decrypt_chunks(&cipher, chunk_size, orig_len, 0, &frame[HEADER..])
            }
            // Seekable: read just the header (for the DEK + layout) and the chunks
            // covering the requested window, decrypt them, and slice.
            Some(r) => {
                let header = self.inner.get(
                    object_id,
                    Some(ByteRange {
                        offset: 0,
                        length: HEADER as u64,
                    }),
                )?;
                let (cipher, chunk_size, orig_len) = self.open_header(&header)?;
                let end = r
                    .offset
                    .checked_add(r.length)
                    .filter(|&e| e <= orig_len)
                    .ok_or(Error::BadRange {
                        offset: r.offset,
                        len: r.length,
                        size: orig_len.min(u32::MAX as u64) as u32,
                    })?;
                if r.length == 0 {
                    return Ok(Vec::new());
                }
                let first = (r.offset / chunk_size as u64) as usize;
                let last = ((end - 1) / chunk_size as u64) as usize;
                let ct_start = Self::chunk_ct_offset(chunk_size, first);
                let ct_end = Self::chunk_ct_offset(chunk_size, last)
                    + Self::chunk_ct_len(chunk_size, orig_len, last);
                let ct = self.inner.get(
                    object_id,
                    Some(ByteRange {
                        offset: ct_start as u64,
                        length: (ct_end - ct_start) as u64,
                    }),
                )?;
                let span = Self::decrypt_chunks(&cipher, chunk_size, orig_len, first, &ct)?;
                let span_start = first as u64 * chunk_size as u64;
                let lo = (r.offset - span_start) as usize;
                let hi = (end - span_start) as usize;
                if hi > span.len() {
                    return Err(Error::Crypto("decrypted span shorter than expected"));
                }
                Ok(span[lo..hi].to_vec())
            }
        }
    }

    fn delete(&self, object_id: ObjectId) -> Result<()> {
        self.inner.delete(object_id)
    }

    fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    fn checkpoint(&self) -> Result<()> {
        self.inner.checkpoint()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use parking_lot::Mutex;
    use std::collections::HashMap;

    /// A minimal in-memory backend, so tests can inspect the bytes at rest and
    /// count how many bytes a read pulled from it (to prove seekability).
    #[derive(Default)]
    struct MemBackend {
        store: Mutex<HashMap<ObjectId, Vec<u8>>>,
        bytes_read: std::sync::atomic::AtomicU64,
    }

    impl StorageBackend for MemBackend {
        fn put(&self, object_id: ObjectId, data: &[u8]) -> Result<()> {
            self.store.lock().insert(object_id, data.to_vec());
            Ok(())
        }
        fn get(&self, object_id: ObjectId, range: Option<ByteRange>) -> Result<Vec<u8>> {
            let data = self
                .store
                .lock()
                .get(&object_id)
                .cloned()
                .ok_or(Error::ObjectNotFound(object_id))?;
            let out = match range {
                None => data,
                Some(r) => data[r.offset as usize..(r.offset + r.length) as usize].to_vec(),
            };
            self.bytes_read
                .fetch_add(out.len() as u64, std::sync::atomic::Ordering::Relaxed);
            Ok(out)
        }
        fn delete(&self, object_id: ObjectId) -> Result<()> {
            self.store.lock().remove(&object_id);
            Ok(())
        }
        fn sync(&self) -> Result<()> {
            Ok(())
        }
        fn checkpoint(&self) -> Result<()> {
            Ok(())
        }
    }

    fn backend(key: [u8; 32]) -> (Arc<MemBackend>, EncryptingBackend) {
        let mem = Arc::new(MemBackend::default());
        let enc = EncryptingBackend::new(mem.clone(), &StaticKeyProvider::new(key));
        (mem, enc)
    }

    #[test]
    fn roundtrip_and_ciphertext_at_rest() {
        let (mem, enc) = backend([7u8; 32]);
        let plain = b"the quick brown fox";
        enc.put(1, plain).unwrap();

        // What hit the inner backend is a ciphertext frame, never the plaintext.
        let stored = mem.store.lock().get(&1).cloned().unwrap();
        assert_eq!(stored.len(), HEADER + plain.len() + 16); // + GCM tag
        assert_eq!(stored[0], VERSION);
        assert!(!stored.windows(plain.len()).any(|w| w == plain));

        assert_eq!(enc.get(1, None).unwrap(), plain);
    }

    #[test]
    fn range_read_after_decrypt() {
        let (_mem, enc) = backend([3u8; 32]);
        enc.put(1, b"0123456789").unwrap();
        let part = enc
            .get(
                1,
                Some(ByteRange {
                    offset: 2,
                    length: 4,
                }),
            )
            .unwrap();
        assert_eq!(part, b"2345");
    }

    #[test]
    fn out_of_bounds_range_is_rejected() {
        let (_mem, enc) = backend([3u8; 32]);
        enc.put(1, b"short").unwrap();
        let err = enc.get(
            1,
            Some(ByteRange {
                offset: 3,
                length: 10,
            }),
        );
        assert!(matches!(err, Err(Error::BadRange { .. })));
    }

    #[test]
    fn wrong_master_key_cannot_decrypt() {
        let mem = Arc::new(MemBackend::default());
        let writer = EncryptingBackend::new(mem.clone(), &StaticKeyProvider::new([1u8; 32]));
        writer.put(1, b"secret").unwrap();

        let reader = EncryptingBackend::new(mem.clone(), &StaticKeyProvider::new([2u8; 32]));
        assert!(matches!(reader.get(1, None), Err(Error::Crypto(_))));
    }

    #[test]
    fn tampered_ciphertext_is_rejected() {
        let (mem, enc) = backend([9u8; 32]);
        enc.put(1, b"important").unwrap();
        {
            let mut g = mem.store.lock();
            let v = g.get_mut(&1).unwrap();
            let last = v.len() - 1;
            v[last] ^= 0xff; // flip a byte in the ciphertext body
        }
        assert!(matches!(enc.get(1, None), Err(Error::Crypto(_))));
    }

    #[test]
    fn distinct_ciphertext_per_put() {
        // Same plaintext + same master key still yields different bytes at rest
        // (fresh per-object DEK and nonces).
        let (mem, enc) = backend([4u8; 32]);
        enc.put(1, b"same").unwrap();
        enc.put(2, b"same").unwrap();
        let a = mem.store.lock().get(&1).cloned().unwrap();
        let b = mem.store.lock().get(&2).cloned().unwrap();
        assert_ne!(a, b);
        assert_eq!(enc.get(1, None).unwrap(), enc.get(2, None).unwrap());
    }

    #[test]
    fn base64_key_validation() {
        use base64::Engine;
        let good = base64::engine::general_purpose::STANDARD.encode([5u8; 32]);
        assert!(StaticKeyProvider::from_base64(&good).is_ok());
        assert!(StaticKeyProvider::from_base64("not base64!!!").is_err());
        let short = base64::engine::general_purpose::STANDARD.encode([5u8; 16]);
        assert!(StaticKeyProvider::from_base64(&short).is_err());
    }

    fn chunked(key: [u8; 32], chunk: usize) -> (Arc<MemBackend>, EncryptingBackend) {
        let mem = Arc::new(MemBackend::default());
        let enc = EncryptingBackend::new(mem.clone(), &StaticKeyProvider::new(key))
            .with_chunk_size(chunk);
        (mem, enc)
    }

    #[test]
    fn chunked_roundtrip_various_sizes() {
        let (_mem, enc) = chunked([5u8; 32], 16);
        for size in [0usize, 1, 15, 16, 17, 31, 32, 33, 100, 1000] {
            let payload: Vec<u8> = (0..size).map(|i| (i * 31 + 7) as u8).collect();
            enc.put(size as u64, &payload).unwrap();
            assert_eq!(enc.get(size as u64, None).unwrap(), payload, "size {size}");
        }
    }

    #[test]
    fn range_reads_span_chunk_boundaries() {
        let (_mem, enc) = chunked([6u8; 32], 16);
        let payload: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        enc.put(1, &payload).unwrap();
        // Windows aligned to, inside, and straddling chunk boundaries.
        for (off, len) in [
            (0u64, 5u64),
            (10, 12),
            (16, 16),
            (15, 2),
            (300, 200),
            (999, 1),
            (0, 1000),
        ] {
            let got = enc
                .get(
                    1,
                    Some(ByteRange {
                        offset: off,
                        length: len,
                    }),
                )
                .unwrap();
            assert_eq!(
                got,
                &payload[off as usize..(off + len) as usize],
                "off {off} len {len}"
            );
        }
    }

    #[test]
    fn range_read_is_seekable_not_whole_object() {
        // 64 chunks of 64 bytes = ~4 KiB plaintext.
        let (mem, enc) = chunked([8u8; 32], 64);
        let payload: Vec<u8> = (0..4096u32).map(|i| (i % 256) as u8).collect();
        enc.put(1, &payload).unwrap();
        let stored = mem.store.lock().get(&1).cloned().unwrap().len() as u64;

        mem.bytes_read
            .store(0, std::sync::atomic::Ordering::Relaxed);
        let got = enc
            .get(
                1,
                Some(ByteRange {
                    offset: 2000,
                    length: 50,
                }),
            )
            .unwrap();
        assert_eq!(got, &payload[2000..2050]);
        // Only the header + the one covering chunk were read — far less than the
        // whole stored object.
        let read = mem.bytes_read.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            read < stored / 4,
            "range read pulled {read} of {stored} bytes"
        );
    }

    #[test]
    fn tampered_chunk_is_rejected() {
        let (mem, enc) = chunked([9u8; 32], 16);
        enc.put(1, &[3u8; 100]).unwrap();
        {
            let mut g = mem.store.lock();
            let v = g.get_mut(&1).unwrap();
            let last = v.len() - 1;
            v[last] ^= 0xff; // corrupt the final chunk's tag
        }
        assert!(matches!(enc.get(1, None), Err(Error::Crypto(_))));
    }
}
