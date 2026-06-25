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

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
use rand::rngs::OsRng;
use rand::RngCore;

use crate::error::{Error, Result};
use crate::ByteRange;

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

/// Chunked AEAD object crypto (see module docs). Holds the master key (KEK) and
/// the chunk size; used by the S3 layer to seal objects in encrypted buckets and
/// to decrypt them (range-aware) on read.
pub struct Crypto {
    kek: Aes256Gcm,
    chunk_size: usize,
}

impl Crypto {
    /// Build over the master key from `keys`, with the default chunk size.
    pub fn new(keys: &dyn KeyProvider) -> Self {
        let kek = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(keys.master_key()));
        Self {
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
    pub fn seal(&self, data: &[u8]) -> Result<Vec<u8>> {
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

    /// Decrypt a whole frame (the inner backend's full stored bytes).
    pub fn open_full(&self, frame: &[u8]) -> Result<Vec<u8>> {
        let (cipher, chunk_size, orig_len) = self.open_header(frame)?;
        Self::decrypt_chunks(&cipher, chunk_size, orig_len, 0, &frame[HEADER..])
    }

    /// Decrypt a byte range without fetching the whole object: read just the header
    /// (for the DEK + layout) and the chunks covering the window via `read(offset,
    /// len)` (which the caller backs with a ranged backend read), then slice.
    pub fn open_range<R>(&self, range: ByteRange, read: R) -> Result<Vec<u8>>
    where
        R: Fn(u64, u64) -> Result<Vec<u8>>,
    {
        let header = read(0, HEADER as u64)?;
        let (cipher, chunk_size, orig_len) = self.open_header(&header)?;
        let end = range
            .offset
            .checked_add(range.length)
            .filter(|&e| e <= orig_len)
            .ok_or(Error::BadRange {
                offset: range.offset,
                len: range.length,
                size: orig_len.min(u32::MAX as u64) as u32,
            })?;
        if range.length == 0 {
            return Ok(Vec::new());
        }
        let first = (range.offset / chunk_size as u64) as usize;
        let last = ((end - 1) / chunk_size as u64) as usize;
        let ct_start = Self::chunk_ct_offset(chunk_size, first);
        let ct_end = Self::chunk_ct_offset(chunk_size, last)
            + Self::chunk_ct_len(chunk_size, orig_len, last);
        let ct = read(ct_start as u64, (ct_end - ct_start) as u64)?;
        let span = Self::decrypt_chunks(&cipher, chunk_size, orig_len, first, &ct)?;
        let span_start = first as u64 * chunk_size as u64;
        let lo = (range.offset - span_start) as usize;
        let hi = (end - span_start) as usize;
        if hi > span.len() {
            return Err(Error::Crypto("decrypted span shorter than expected"));
        }
        Ok(span[lo..hi].to_vec())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    fn crypto(key: [u8; 32], chunk: usize) -> Crypto {
        Crypto::new(&StaticKeyProvider::new(key)).with_chunk_size(chunk)
    }

    /// A reader over an in-memory frame that counts the bytes pulled (to prove
    /// seekability), matching `Crypto::open_range`'s `read(offset, len)` contract.
    fn reader<'a>(
        frame: &'a [u8],
        counter: &'a std::sync::atomic::AtomicU64,
    ) -> impl Fn(u64, u64) -> Result<Vec<u8>> + 'a {
        move |off, len| {
            counter.fetch_add(len, std::sync::atomic::Ordering::Relaxed);
            Ok(frame[off as usize..(off + len) as usize].to_vec())
        }
    }

    #[test]
    fn roundtrip_and_self_describing_frame() {
        let c = crypto([7u8; 32], DEFAULT_CHUNK);
        let plain = b"the quick brown fox";
        let frame = c.seal(plain).unwrap();
        // The frame is a single-chunk ciphertext, never the plaintext.
        assert_eq!(frame.len(), HEADER + plain.len() + TAG);
        assert_eq!(frame[0], VERSION);
        assert!(!frame.windows(plain.len()).any(|w| w == plain));
        assert_eq!(c.open_full(&frame).unwrap(), plain);
    }

    #[test]
    fn chunked_roundtrip_various_sizes() {
        let c = crypto([5u8; 32], 16);
        for size in [0usize, 1, 15, 16, 17, 31, 32, 33, 100, 1000] {
            let payload: Vec<u8> = (0..size).map(|i| (i * 31 + 7) as u8).collect();
            let frame = c.seal(&payload).unwrap();
            assert_eq!(c.open_full(&frame).unwrap(), payload, "size {size}");
        }
    }

    #[test]
    fn range_reads_span_chunk_boundaries() {
        let c = crypto([6u8; 32], 16);
        let payload: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        let frame = c.seal(&payload).unwrap();
        let counter = std::sync::atomic::AtomicU64::new(0);
        for (off, len) in [
            (0u64, 5u64),
            (10, 12),
            (16, 16),
            (15, 2),
            (300, 200),
            (999, 1),
            (0, 1000),
        ] {
            let got = c
                .open_range(
                    ByteRange {
                        offset: off,
                        length: len,
                    },
                    reader(&frame, &counter),
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
        let c = crypto([8u8; 32], 64); // 64 chunks of 64 B = ~4 KiB
        let payload: Vec<u8> = (0..4096u32).map(|i| (i % 256) as u8).collect();
        let frame = c.seal(&payload).unwrap();
        let counter = std::sync::atomic::AtomicU64::new(0);
        let got = c
            .open_range(
                ByteRange {
                    offset: 2000,
                    length: 50,
                },
                reader(&frame, &counter),
            )
            .unwrap();
        assert_eq!(got, &payload[2000..2050]);
        let read = counter.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            read < frame.len() as u64 / 4,
            "range read pulled {read} of {} bytes",
            frame.len()
        );
    }

    #[test]
    fn out_of_bounds_range_is_rejected() {
        let c = crypto([3u8; 32], 16);
        let frame = c.seal(b"short").unwrap();
        let counter = std::sync::atomic::AtomicU64::new(0);
        let err = c.open_range(
            ByteRange {
                offset: 3,
                length: 10,
            },
            reader(&frame, &counter),
        );
        assert!(matches!(err, Err(Error::BadRange { .. })));
    }

    #[test]
    fn wrong_master_key_cannot_decrypt() {
        let writer = crypto([1u8; 32], 16);
        let frame = writer.seal(b"secret").unwrap();
        let reader_c = crypto([2u8; 32], 16);
        assert!(matches!(reader_c.open_full(&frame), Err(Error::Crypto(_))));
    }

    #[test]
    fn tampered_chunk_is_rejected() {
        let c = crypto([9u8; 32], 16);
        let mut frame = c.seal(&[3u8; 100]).unwrap();
        let last = frame.len() - 1;
        frame[last] ^= 0xff; // corrupt the final chunk's tag
        assert!(matches!(c.open_full(&frame), Err(Error::Crypto(_))));
    }

    #[test]
    fn distinct_ciphertext_per_seal() {
        // Same plaintext + master key → different frames (fresh DEK + nonces).
        let c = crypto([4u8; 32], 16);
        let a = c.seal(b"same").unwrap();
        let b = c.seal(b"same").unwrap();
        assert_ne!(a, b);
        assert_eq!(c.open_full(&a).unwrap(), c.open_full(&b).unwrap());
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
}
