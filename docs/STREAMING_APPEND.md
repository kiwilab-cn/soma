# Soma Streaming & Append Design

> **Status: design / north-star (not yet implemented).** How soma can support
> streaming uploads, streaming downloads, bounded write memory, and **offset-based
> append** — all from one change: representing a large object as an ordered list of
> immutable **chunk needles** instead of a single needle. Mirrors AWS's
> multipart-as-parts model and S3 Express One Zone's append-by-write-offset.
> Parent: [`ARCHITECTURE.md`](./ARCHITECTURE.md),
> [`OBJECT_SIZING.md`](./OBJECT_SIZING.md), [`LOCALITY_DESIGN.md`](./LOCALITY_DESIGN.md).

## 1. Current limits

Today soma is **one object = one needle** (whole-object write, whole-object read, one
CRC over the whole payload, length+CRC known before the needle header is written):

- **PutObject buffers the whole body** in gateway memory (up to `max_request_body`);
  chunked/`STREAMING-…` upload signatures are rejected.
- **GetObject reads the whole object into memory** before responding — no streamed
  download.
- **Multipart upload assembles the whole object in memory** at `CompleteMultipartUpload`
  (the parts are concatenated into one needle), so it bounds upload *transfer* but not
  completion *memory*.
- **No append** — a needle is immutable and fixed-length; you cannot grow it.

The root cause is the same in all four: an object's bytes are one indivisible needle.

## 2. The unifying idea: chunked objects

Represent a large object as an **ordered list of chunk needles** plus a **chunk map**
in metadata:

```
object "k"  →  ObjectMeta { size, etag, … , chunks: [c0, c1, c2, …] }
                where ci = ChunkRef { needle_id, logical_offset, len, crc }
```

Each chunk is a first-class, **immutable** needle with its own `object_id`, placed and
replicated/EC'd independently (exactly like a multipart *part*, or an HDFS block). The
logical object is the concatenation of its chunks in order.

This is not new machinery — **multipart already produces part-needles**; soma just
reassembles them into one needle at completion today. **Keeping the chunk map instead
of reassembling** is the whole foundation. Small/medium objects stay **single-needle**
(an empty/implicit one-chunk map = today's fast path); chunking only kicks in above a
chunk threshold or for streaming/append.

Once an object is a chunk list, all four capabilities fall out:

| capability | with chunks |
| --- | --- |
| **streaming upload** | write each incoming chunk as a needle, accumulate `ChunkRef`s, commit the map at end — memory = one chunk |
| **streaming download** | stream chunk needles in order; a range read streams only the covering chunks |
| **bounded memory** | neither upload nor download holds the whole object |
| **append** | append a new immutable chunk needle + extend the map (volumes are already append-only) |

## 3. Metadata model

`ObjectMeta` gains a chunk list (or a sibling `CHUNKS` table keyed by
`(object_id, chunk_index)` to avoid bloating the common single-needle record). A
single-needle object has no chunk list (or a one-entry list pointing at itself), so
the hot path and on-disk layout for normal objects are unchanged.

Reads map a byte range to the covering chunks via the `logical_offset`/`len` fields
(a binary search over the chunk map). Placement, replication/EC, GC, and the locality
oracle all operate **per chunk needle** — so a large object naturally spreads across
nodes and is read in parallel.

## 4. Streaming download (the easy first step)

Independent of the chunk model: change `GetObject` to stream. For a single-needle
object, read the needle in windows via the backend's ranged `get(object_id, range)`
and emit them through an axum streaming response body — bounded read memory, no
storage change. For a chunked object, stream the covering chunks. This removes the
"whole object in memory on GET" risk immediately and is the recommended first
increment.

## 5. Chunked representation (the foundation)

Stop reassembling at `CompleteMultipartUpload`: persist the parts as the object's
chunk map (each part becomes a chunk; the part-needles are no longer garbage). Range
reads gain range→chunk mapping. Write memory at completion drops to ~zero (no
assembly). This is the load-bearing change the rest builds on.

## 6. Streaming upload

Accept the AWS chunked transfer encoding (`STREAMING-AWS4-HMAC-SHA256`: each wire
chunk carries a size + a chained signature). Decode incrementally at the S3 edge; as
bytes accumulate to a target chunk size, allocate a needle id, `put` that chunk,
record its `ChunkRef`, and continue. On completion, commit the object's chunk map.
This is effectively **internal auto-multipart** with bounded (one-chunk) memory.

## 7. Append — offset-based, mirroring S3 Express One Zone

AWS general-purpose S3 has no append; **S3 Express One Zone** added appendable objects
(Nov 2024) via `PutObject` with **`x-amz-write-offset-bytes: <N>`**, where `N` must
equal the object's current size. Soma should mirror this exact contract:

- An append `PutObject` carries `x-amz-write-offset-bytes: N`.
- The metadata transaction checks **`current_size == N`** (the offset is both the
  append position *and* a concurrency fence — like `If-Match`, but on size), then
  appends the body as a **new immutable chunk needle** and extends the chunk map.
- A concurrent appender holding a stale `N` fails the precondition (`412`) — no
  corruption, retry against the new size.

Why this fits soma perfectly:

- **Append-only is native.** A new chunk is appended to the active volume — exactly
  what volumes already do. No in-place mutation, no random write (which soma's
  immutable needles cannot do, and which S3 Express also forbids — append only at the
  end).
- **Locality is unaffected.** Every existing chunk needle stays immutable; only the
  chunk map grows. So the P2 property "a passed fd pins an immutable needle" still
  holds — append does **not** break short-circuit reads.
- **It is a CAS by construction**, reusing the conditional-write machinery
  (`docs/CONDITIONAL_WRITES.md`).

Limits to mirror: append only at `offset == current_size`; a bound on chunks per
object (the chunk map can't grow without limit); the object becomes multi-chunk.

## 8. Locality interaction

A multi-chunk object's chunks are separate needles, possibly on different volumes and
nodes. So the locality path generalizes: resolve the object → chunk map → per-chunk
`?location` → local fd (P2) or remote. `soma-object-store` / `soma-client` would gain
a "resolve chunk map" step before the per-chunk fd reads. **Single-chunk objects keep
the current one-needle, one-fd path** — so a consumer that sizes objects as one chunk
(see `OBJECT_SIZING.md`) is unaffected; chunking is only paid where it's used.

## 9. CRC, etag, integrity

Per-chunk CRC already exists (each part/needle has its own `data_crc`). A multi-chunk
object's etag uses the existing multipart `hash-N` form (a hash over the chunk
digests). Whole-object verification = verify each chunk's CRC; a streamed/range read
verifies the chunks it touches. This matches the localfd integrity model (the reader
verifies per needle).

## 10. Out of scope (and why)

- **Random write / overwrite-in-the-middle.** Soma's needles are immutable and
  append-only; S3 Express also forbids it. Append is end-only (`offset == size`).
- **Truncate / partial delete of an object.** Out of scope; delete is whole-object.

## 11. Phasing

1. **Streaming download** — ranged-read + streaming response body. Independent, removes
   the GET-into-memory risk. *Do this first.*
2. **Chunked representation** — keep the multipart chunk map instead of reassembling;
   range→chunk mapping. The foundation.
3. **Streaming upload** — decode chunked transfer, write chunk needles incrementally.
4. **Append** — `x-amz-write-offset-bytes` with the `size == offset` precondition,
   appending a chunk needle.
5. **Locality for chunked objects** — chunk-map resolution in the read clients.

## 12. Best-practice coordinates

| idea | precedent |
| --- | --- |
| large object = ordered list of immutable parts/blocks | S3 multipart, HDFS blocks, GFS chunks |
| keep the part manifest instead of reassembling | Iceberg/Delta data files, log-structured stores |
| append via write-offset that doubles as a fence | **S3 Express One Zone** (`x-amz-write-offset-bytes`), Azure Append Blob |
| streamed GET via ranged reads | every S3 implementation |

The combination is conventional and low-risk: soma already has the parts (multipart
part-needles) and the fence (conditional writes); this design connects them. Note the
scope — it is an **M-series feature**, not on the critical path for consumers whose
objects are immutable and single-chunk-sized; build it when a workload needs true
streaming, very large objects, or append.
