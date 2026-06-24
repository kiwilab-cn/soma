//! Reed-Solomon `k+m` codec, shared by the erasure-coded backend and the
//! rebalance controller (which reconstructs shards for migrated/lost slots, M3d).
//!
//! Stripes are self-describing: the original length is prepended and the payload
//! zero-padded to `k` equal, even-length shards (`reed-solomon-simd` requires even
//! shard sizes). Reconstruction from any `k` survivors recomputes the exact same
//! `k+m` shards the original encode produced (the codec is deterministic), so a
//! rebuilt shard `i` is bit-identical to the original shard `i`.

use soma_backend::{Error, Result};

/// Bytes of big-endian length prefix prepended to the payload before encoding.
const LEN_PREFIX: usize = 8;

/// Split `data` into `k` data shards + `m` Reed-Solomon parity shards.
pub(crate) fn encode(data: &[u8], k: usize, m: usize) -> Result<Vec<Vec<u8>>> {
    let mut framed = Vec::with_capacity(LEN_PREFIX + data.len());
    framed.extend_from_slice(&(data.len() as u64).to_be_bytes());
    framed.extend_from_slice(data);

    // reed-solomon-simd needs equal, non-zero, even-length shards.
    let per_shard = framed.len().div_ceil(k).max(1);
    let shard_len = per_shard + (per_shard & 1);
    framed.resize(shard_len * k, 0);

    let mut shards: Vec<Vec<u8>> = (0..k)
        .map(|i| framed[i * shard_len..(i + 1) * shard_len].to_vec())
        .collect();

    if m > 0 {
        let parity = reed_solomon_simd::encode(k, m, &shards)
            .map_err(|_| Error::Erasure("reed-solomon encode failed"))?;
        shards.extend(parity);
    }
    Ok(shards)
}

/// Reconstruct the original bytes from any `k` surviving shards
/// `(shard_index, bytes)`.
pub(crate) fn reassemble(present: Vec<(usize, Vec<u8>)>, k: usize, m: usize) -> Result<Vec<u8>> {
    if present.len() < k {
        return Err(Error::Erasure("insufficient shards to reconstruct object"));
    }

    let mut data: Vec<Option<Vec<u8>>> = vec![None; k];
    let mut recovery: Vec<(usize, Vec<u8>)> = Vec::new();
    for (idx, shard) in present {
        if idx < k {
            data[idx] = Some(shard);
        } else {
            recovery.push((idx - k, shard));
        }
    }

    // Fill any missing data shards from parity.
    if data.iter().any(|s| s.is_none()) {
        let originals = data
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.as_ref().map(|v| (i, v.as_slice())));
        let recovered = reed_solomon_simd::decode(
            k,
            m,
            originals,
            recovery.iter().map(|(i, v)| (*i, v.as_slice())),
        )
        .map_err(|_| Error::Erasure("reed-solomon decode failed"))?;
        for (i, slot) in data.iter_mut().enumerate() {
            if slot.is_none() {
                *slot = Some(
                    recovered
                        .get(&i)
                        .cloned()
                        .ok_or(Error::Erasure("decode did not restore a data shard"))?,
                );
            }
        }
    }

    let mut framed = Vec::with_capacity(data.len());
    for slot in data {
        framed.extend_from_slice(&slot.ok_or(Error::Erasure("missing data shard"))?);
    }

    // Recover the true length from the prefix and truncate the padding.
    if framed.len() < LEN_PREFIX {
        return Err(Error::Erasure("stripe shorter than its length prefix"));
    }
    let mut len_bytes = [0u8; LEN_PREFIX];
    len_bytes.copy_from_slice(&framed[..LEN_PREFIX]);
    let orig_len = u64::from_be_bytes(len_bytes) as usize;
    let end = LEN_PREFIX
        .checked_add(orig_len)
        .filter(|&e| e <= framed.len())
        .ok_or(Error::Erasure("length prefix exceeds reconstructed data"))?;
    Ok(framed[LEN_PREFIX..end].to_vec())
}

/// Reconstruct the full `k+m` shard set from any `k` survivors — used to rebuild
/// a specific shard for a migrated/lost slot.
pub(crate) fn reconstruct_all_shards(
    present: Vec<(usize, Vec<u8>)>,
    k: usize,
    m: usize,
) -> Result<Vec<Vec<u8>>> {
    let data = reassemble(present, k, m)?;
    encode(&data, k, m)
}
