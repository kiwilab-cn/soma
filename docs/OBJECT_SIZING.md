# Soma Object Sizing — guidance for consumers storing large/immutable objects

> How big should an object be? Relevant to a compute/storage-separated consumer that
> stores immutable **segments** (or shards, blobs) on soma. The short answer: **one
> segment = one object = one needle, sized in the tens-to-low-hundreds of MB.**

## The limits that bound object size

- **Single-part PUT cap.** A single `PutObject` body is buffered up to
  `max_request_body` (config, default **5 GiB** = S3's single-PUT ceiling; env
  `SOMA_MAX_REQUEST_BODY`, Helm `gateway.maxRequestBody`). Objects larger than this
  must use **multipart** upload.
- **Memory, not just the cap.** A single-part PUT holds the whole body in gateway
  memory; and **multipart *completion* currently assembles the whole object in
  memory** (the parts are concatenated into one buffer before the needle is written).
  So multipart helps you *upload* a large object in chunks, but completing it still
  materializes the full object once. The practical ceiling is therefore
  **memory-bound** — many concurrent multi-GB objects will spike memory regardless of
  the PUT cap. (A future improvement — keeping the multipart chunk map instead of
  reassembling, plus streaming upload/download and append — is designed in
  [`STREAMING_APPEND.md`](./STREAMING_APPEND.md).)
- **Volume packing.** Objects are packed into append-only volume files of
  `storage.volume_max` (default 4 GiB). An object **larger than `volume_max`** still
  works — it gets its **own** volume as a single oversized needle — but a multi-GB
  volume file is unwieldy. Objects comfortably below `volume_max` pack normally.
- **Small-files pressure (the other end).** Object metadata grows with object
  **count** (~100–300 B per object), independent of object size. Swarms of *tiny*
  objects pressure the metadata store and add per-object overhead. (This is the
  classic small-files problem; soma's Haystack design mitigates the *storage* side
  but the *metadata* count still grows.)

## The sweet spot

For a consumer storing immutable segments:

1. **One segment = one object = one needle.** Do **not** pack multiple segments into
   one object. The 1:1 mapping keeps `?location` clean (the whole segment is one
   needle → one fd) and makes the local `mmap` cover exactly the segment's bytes.
2. **Size: tens to low-hundreds of MB per segment** (think Parquet file / row-group
   sizing). Big enough to amortize per-object metadata and give good scan locality;
   small enough that single-part PUT (or multipart completion) memory is comfortable
   and the object sits well under `volume_max`.
3. **Avoid both extremes:** multi-GB single objects (memory spikes on PUT and on
   multipart completion) and swarms of KB-sized objects (metadata count pressure +
   per-object overhead).

## Reads are size-agnostic

The zero-copy local read path (`?location` → fd → `mmap` a byte range) works for an
object of **any** size — the mapping is lazy, so a range read of a large segment
faults in only the touched pages, with no full-object copy. So "large" costs you on
the **write** path (memory), not the **read** path. Range reads (`get_range` /
`get_ranges`, via `SomaStore`) are the zero-copy hot path; full-object `get` returns
an owned buffer.

## Operator knob

Lower `max_request_body` (e.g. to `1GiB`) to **force large uploads through multipart**
and bound the memory a single PUT can consume on the gateway. It does not change what
clients can store (multipart still handles larger objects) — it caps per-request
buffering.
