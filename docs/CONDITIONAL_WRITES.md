# Soma Conditional Writes (CAS) — the optimistic-concurrency contract

> The guarantees soma offers for **compare-and-swap writes**, and the recipe a
> consumer uses to commit a manifest / pointer / catalog entry safely under
> concurrency. This is the fence that lets a single-writer-per-key design stay
> correct even when two writers briefly race.

## What soma provides

Soma supports S3 conditional `PutObject`:

| Header | Meaning | Fails with |
| --- | --- | --- |
| `If-None-Match: *` | write **only if the object is absent** (create-if-absent) | `412 PreconditionFailed` if it already exists |
| `If-Match: <etag>` | write **only if the current etag equals `<etag>`** (update-if-unchanged) | `412 PreconditionFailed` if the current etag differs or the object is absent |

The condition is evaluated **inside the metadata write transaction** — the same
transaction that commits the new version. That transaction is soma's linearization
point (redb serializes writers today; under the planned Raft meta the same check
moves into the state-machine `apply()`, unchanged in contract). So the
read-condition and the dependent write are **atomic and linearizable**: of any
number of concurrent conditional writers to one key, **exactly one commits**; every
other gets `412`. No torn or lost writes, no corruption — at worst a retry.

The **etag** is content-derived (MD5 hex for single-part objects, `hash-N` for
multipart). It is returned in the `PutObject` response and in `HEAD`/`GET`, and is
what you feed back into `If-Match`.

## The manifest-commit recipe (optimistic concurrency)

To advance a versioned pointer object (a manifest, a catalog root, a head pointer)
under concurrency:

```
loop:
  (cur_bytes, cur_etag) = GET key        # or "absent" on first write
  new_bytes = derive_next(cur_bytes)     # compute the new version
  try:
      if absent:  PUT key new_bytes  If-None-Match: *
      else:       PUT key new_bytes  If-Match: cur_etag
      return success                     # we won
  except 412 PreconditionFailed:
      continue                           # someone else committed; re-read and retry
```

Because the conditional check is atomic with the commit, this is a correct CAS loop:
the writer only succeeds if nothing changed the object since it read `cur_etag`. Two
writers that both observed version *V* both attempt `If-Match: etag(V)`; one wins and
produces *V+1*, the other sees `412` and retries against the now-current version.

## Using it through the `object_store` crate

A consumer on the `object_store` crate (the soma gateway via `AmazonS3`, or
`SomaStore`) gets this via `put_opts` once the client is built with conditional put
enabled — `AmazonS3Builder::…​.with_conditional_put(S3ConditionalPut::ETagMatch)`:

| intent | `PutMode` | header soma sees | error on conflict |
| --- | --- | --- | --- |
| create-if-absent | `PutMode::Create` | `If-None-Match: *` | `object_store::Error::AlreadyExists` |
| update-if-unchanged | `PutMode::Update(UpdateVersion { e_tag: Some(etag), .. })` | `If-Match: <etag>` | `object_store::Error::Precondition` |

`put_opts` returns the new `e_tag`; carry it into the next `Update`. This is exactly
the loop above, with the crate mapping `412` to `AlreadyExists` / `Precondition`.

End-to-end tests: `object_store_conditional_create` (create-if-absent) and
`object_store_conditional_update_cas` (the full read→update→stale-loser→retry cycle)
in `src/s3/tests/integration.rs`.

## Scope & caveats

- **Linearizability** holds as long as soma's metadata is linearizable — true today
  (single meta node) and preserved under the planned Raft meta (the check runs in
  `apply()`).
- The etag is **content-derived**, so two writes of identical bytes share an etag. A
  manifest commit always changes content (a new version), so this is a non-issue for
  the recipe; just don't rely on the etag changing when the bytes don't.
- Conditional writes apply to the **object's current version** only; soma keeps the
  current version (no version history in M0), which is what a head-pointer commit
  needs.
