//! Envelope encryption at rest (M4a).
//!
//! [`EncryptingBackend`] is a transparent [`StorageBackend`] decorator that seals
//! object bytes before they reach the inner backend — so storage nodes, and the
//! replication / erasure-coding layer below, only ever see ciphertext.
//!
//! **Envelope scheme.** Each object gets a fresh random 256-bit *data encryption
//! key* (DEK). The payload is sealed with AES-256-GCM under the DEK; the DEK is
//! then *wrapped* (encrypted) with the *master key* (KEK) and stored alongside the
//! ciphertext, so the stored bytes are self-describing — no per-object key
//! database is needed to decrypt:
//!
//! ```text
//! frame = [ version:1 ][ kek_nonce:12 ][ wrapped_dek:48 ][ data_nonce:12 ][ ciphertext+tag ]
//! ```
//!
//! Reads fetch the whole frame (AES-GCM is not seekable), unwrap the DEK with the
//! KEK, decrypt, then slice any requested range. GCM is authenticated, so a wrong
//! master key or tampered bytes surface as an [`Error::Crypto`] rather than
//! silently wrong data.

use std::sync::Arc;

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
use rand::rngs::OsRng;
use rand::RngCore;
use soma_core::ObjectId;

use crate::error::{Error, Result};
use crate::{ByteRange, StorageBackend};

/// Frame format version (the first stored byte), so the layout can evolve.
const VERSION: u8 = 1;
/// AES-GCM nonce length.
const NONCE: usize = 12;
/// Data encryption key length (AES-256).
const DEK: usize = 32;
/// A DEK wrapped by the KEK: AES-256-GCM ciphertext (32) + tag (16).
const WRAPPED_DEK: usize = DEK + 16;
/// Fixed self-describing header before the payload ciphertext.
const HEADER: usize = 1 + NONCE + WRAPPED_DEK + NONCE;

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
}

impl EncryptingBackend {
    /// Wrap `inner`, sealing every payload under the master key from `keys`.
    pub fn new(inner: Arc<dyn StorageBackend>, keys: &dyn KeyProvider) -> Self {
        let kek = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(keys.master_key()));
        Self { inner, kek }
    }

    /// Seal `plaintext` into a self-describing frame.
    fn seal(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut dek = [0u8; DEK];
        OsRng.fill_bytes(&mut dek);
        let mut data_nonce = [0u8; NONCE];
        OsRng.fill_bytes(&mut data_nonce);
        let mut kek_nonce = [0u8; NONCE];
        OsRng.fill_bytes(&mut kek_nonce);

        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&dek));
        let ciphertext = cipher
            .encrypt(Nonce::from_slice(&data_nonce), plaintext)
            .map_err(|_| Error::Crypto("payload encryption failed"))?;
        let wrapped = self
            .kek
            .encrypt(Nonce::from_slice(&kek_nonce), dek.as_slice())
            .map_err(|_| Error::Crypto("key wrap failed"))?;

        let mut frame = Vec::with_capacity(HEADER + ciphertext.len());
        frame.push(VERSION);
        frame.extend_from_slice(&kek_nonce);
        frame.extend_from_slice(&wrapped);
        frame.extend_from_slice(&data_nonce);
        frame.extend_from_slice(&ciphertext);
        Ok(frame)
    }

    /// Open a frame produced by [`Self::seal`], returning the plaintext.
    fn open(&self, frame: &[u8]) -> Result<Vec<u8>> {
        if frame.len() < HEADER || frame[0] != VERSION {
            return Err(Error::Crypto("malformed or unknown ciphertext frame"));
        }
        let kek_nonce = &frame[1..1 + NONCE];
        let wrapped = &frame[1 + NONCE..1 + NONCE + WRAPPED_DEK];
        let data_nonce = &frame[1 + NONCE + WRAPPED_DEK..HEADER];
        let ciphertext = &frame[HEADER..];

        let dek = self
            .kek
            .decrypt(Nonce::from_slice(kek_nonce), wrapped)
            .map_err(|_| Error::Crypto("key unwrap failed (wrong master key?)"))?;
        if dek.len() != DEK {
            return Err(Error::Crypto("unwrapped data key has the wrong length"));
        }
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&dek));
        cipher
            .decrypt(Nonce::from_slice(data_nonce), ciphertext)
            .map_err(|_| Error::Crypto("payload authentication failed"))
    }
}

impl StorageBackend for EncryptingBackend {
    fn put(&self, object_id: ObjectId, data: &[u8]) -> Result<()> {
        let frame = self.seal(data)?;
        self.inner.put(object_id, &frame)
    }

    fn get(&self, object_id: ObjectId, range: Option<ByteRange>) -> Result<Vec<u8>> {
        // GCM seals the whole payload, so a ranged read still fetches and decrypts
        // the full object, then slices.
        let frame = self.inner.get(object_id, None)?;
        let plain = self.open(&frame)?;
        match range {
            None => Ok(plain),
            Some(r) => {
                let end = r
                    .offset
                    .checked_add(r.length)
                    .filter(|&e| e <= plain.len() as u64)
                    .ok_or(Error::BadRange {
                        offset: r.offset,
                        len: r.length,
                        size: plain.len() as u32,
                    })?;
                Ok(plain[r.offset as usize..end as usize].to_vec())
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

    /// A minimal in-memory backend, so tests can inspect the bytes at rest.
    #[derive(Default)]
    struct MemBackend {
        store: Mutex<HashMap<ObjectId, Vec<u8>>>,
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
            match range {
                None => Ok(data),
                Some(r) => Ok(data[r.offset as usize..(r.offset + r.length) as usize].to_vec()),
            }
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
}
