//! `RedbMetaStore`: the M0 metadata store backed by an embedded redb database.
//!
//! Three tables:
//! - `buckets`: bucket name (`&str`) → CBOR/postcard [`BucketMeta`].
//! - `objects`: composite key (`&[u8]`, see [`composite_key`]) → [`ObjectMeta`].
//! - `seq`: counter name (`&str`) → `u64` (monotonic id allocation).
//!
//! Conditional writes are evaluated inside a single redb write transaction, which
//! is the linearization point: redb serializes writers, so an `If-Match` /
//! `If-None-Match` check and the dependent write are atomic.

use std::ops::Bound;
use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition};
use soma_core::ObjectId;

use crate::error::{Error, Result};
use crate::types::{
    BucketMeta, BucketOpts, ListRequest, ListResult, ObjectEntry, ObjectMeta, ObjectPut,
    PutCondition, Version,
};
use crate::MetadataStore;

const BUCKETS: TableDefinition<&str, &[u8]> = TableDefinition::new("buckets");
const OBJECTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("objects");
const SEQ: TableDefinition<&str, u64> = TableDefinition::new("seq");

/// Counter name for the monotonic object-id sequence.
const OBJECT_ID_SEQ: &str = "object_id";

/// Embedded metadata store.
pub struct RedbMetaStore {
    db: Database,
}

impl RedbMetaStore {
    /// Open (creating if absent) a metadata store at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::create(path.as_ref())?;
        // Materialize the tables so reads on a fresh database don't fail.
        let w = db.begin_write()?;
        {
            w.open_table(BUCKETS)?;
            w.open_table(OBJECTS)?;
            w.open_table(SEQ)?;
        }
        w.commit()?;
        Ok(Self { db })
    }
}

impl MetadataStore for RedbMetaStore {
    fn create_bucket(&self, name: &str, opts: BucketOpts) -> Result<()> {
        validate_bucket_name(name)?;
        let w = self.db.begin_write()?;
        {
            let mut t = w.open_table(BUCKETS)?;
            if t.get(name)?.is_some() {
                return Err(Error::BucketAlreadyExists(name.to_string()));
            }
            let meta = BucketMeta {
                name: name.to_string(),
                versioning: opts.versioning,
            };
            t.insert(name, postcard::to_allocvec(&meta)?.as_slice())?;
        }
        w.commit()?;
        Ok(())
    }

    fn delete_bucket(&self, name: &str) -> Result<()> {
        let w = self.db.begin_write()?;
        {
            let mut buckets = w.open_table(BUCKETS)?;
            if buckets.get(name)?.is_none() {
                return Err(Error::NoSuchBucket(name.to_string()));
            }
            // Refuse if any object remains in the bucket.
            let objects = w.open_table(OBJECTS)?;
            let prefix = bucket_prefix(name);
            if range_has_any(&objects, &prefix)? {
                return Err(Error::BucketNotEmpty(name.to_string()));
            }
            buckets.remove(name)?;
        }
        w.commit()?;
        Ok(())
    }

    fn get_bucket(&self, name: &str) -> Result<Option<BucketMeta>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(BUCKETS)?;
        match t.get(name)? {
            Some(g) => Ok(Some(postcard::from_bytes(g.value())?)),
            None => Ok(None),
        }
    }

    fn list_buckets(&self) -> Result<Vec<BucketMeta>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(BUCKETS)?;
        let mut out = Vec::new();
        for item in t.iter()? {
            let (_, v) = item?;
            out.push(postcard::from_bytes(v.value())?);
        }
        Ok(out)
    }

    fn put_object(
        &self,
        bucket: &str,
        key: &str,
        put: ObjectPut,
        cond: PutCondition,
    ) -> Result<Version> {
        let w = self.db.begin_write()?;
        let new_version;
        {
            {
                let buckets = w.open_table(BUCKETS)?;
                if buckets.get(bucket)?.is_none() {
                    return Err(Error::NoSuchBucket(bucket.to_string()));
                }
            }
            let mut objects = w.open_table(OBJECTS)?;
            let ck = composite_key(bucket, key);
            let current: Option<ObjectMeta> = match objects.get(ck.as_slice())? {
                Some(g) => Some(postcard::from_bytes(g.value())?),
                None => None,
            };
            check_condition(&cond, current.as_ref())?;

            new_version = Version(current.as_ref().map_or(1, |c| c.version.0 + 1));
            let meta = ObjectMeta {
                object_id: put.object_id,
                location: put.location,
                size: put.size,
                etag: put.etag,
                version: new_version,
            };
            objects.insert(ck.as_slice(), postcard::to_allocvec(&meta)?.as_slice())?;
        }
        w.commit()?;
        Ok(new_version)
    }

    fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        let r = self.db.begin_read()?;
        let objects = r.open_table(OBJECTS)?;
        let ck = composite_key(bucket, key);
        match objects.get(ck.as_slice())? {
            Some(g) => Ok(Some(postcard::from_bytes(g.value())?)),
            None => Ok(None),
        }
    }

    fn delete_object(&self, bucket: &str, key: &str, cond: PutCondition) -> Result<()> {
        let w = self.db.begin_write()?;
        {
            let mut objects = w.open_table(OBJECTS)?;
            let ck = composite_key(bucket, key);
            let current: Option<ObjectMeta> = match objects.get(ck.as_slice())? {
                Some(g) => Some(postcard::from_bytes(g.value())?),
                None => None,
            };
            check_condition(&cond, current.as_ref())?;
            if current.is_some() {
                objects.remove(ck.as_slice())?;
            }
        }
        w.commit()?;
        Ok(())
    }

    fn list_objects(&self, bucket: &str, req: &ListRequest) -> Result<ListResult> {
        let max = if req.max_keys == 0 {
            1000
        } else {
            req.max_keys.min(1000)
        };

        let r = self.db.begin_read()?;
        {
            let buckets = r.open_table(BUCKETS)?;
            if buckets.get(bucket)?.is_none() {
                return Err(Error::NoSuchBucket(bucket.to_string()));
            }
        }
        let objects = r.open_table(OBJECTS)?;

        let scan_prefix = composite_key(bucket, &req.prefix);
        let end_key = prefix_end(&scan_prefix);
        let start_owned: Vec<u8> = match &req.continuation_token {
            Some(tok) => tok.clone(),
            None => scan_prefix.clone(),
        };
        let lower = Bound::Included(start_owned.as_slice());
        let upper = match &end_key {
            Some(e) => Bound::Excluded(e.as_slice()),
            None => Bound::Unbounded,
        };

        let key_offset = 1 + bucket.len();
        let mut result = ListResult::default();
        let mut last_emitted_cp: Option<String> = None;
        let mut count = 0usize;

        for item in objects.range::<&[u8]>((lower, upper))? {
            let (k, v) = item?;
            let ck = k.value();
            let obj_key = std::str::from_utf8(&ck[key_offset..]).map_err(|_| Error::NonUtf8Key)?;

            // Delimiter roll-up into common prefixes.
            if let Some(delim) = req.delimiter.as_deref().filter(|d| !d.is_empty()) {
                let after = &obj_key[req.prefix.len()..];
                if let Some(pos) = after.find(delim) {
                    let cp = obj_key[..req.prefix.len() + pos + delim.len()].to_string();
                    if last_emitted_cp.as_deref() == Some(cp.as_str()) {
                        continue; // already rolled up this group
                    }
                    if count >= max {
                        result.is_truncated = true;
                        result.next_continuation_token = Some(ck.to_vec());
                        break;
                    }
                    last_emitted_cp = Some(cp.clone());
                    result.common_prefixes.push(cp);
                    count += 1;
                    continue;
                }
            }

            // A leaf object.
            if count >= max {
                result.is_truncated = true;
                result.next_continuation_token = Some(ck.to_vec());
                break;
            }
            let meta: ObjectMeta = postcard::from_bytes(v.value())?;
            result.objects.push(ObjectEntry {
                key: obj_key.to_string(),
                size: meta.size,
                etag: meta.etag,
                version: meta.version,
            });
            count += 1;
        }

        Ok(result)
    }

    fn next_object_id(&self) -> Result<ObjectId> {
        let w = self.db.begin_write()?;
        let id;
        {
            let mut t = w.open_table(SEQ)?;
            let current = t.get(OBJECT_ID_SEQ)?.map_or(0, |g| g.value());
            id = current + 1;
            t.insert(OBJECT_ID_SEQ, id)?;
        }
        w.commit()?;
        Ok(id)
    }
}

/// Evaluate a conditional-write precondition against the current object (if any).
fn check_condition(cond: &PutCondition, current: Option<&ObjectMeta>) -> Result<()> {
    match cond {
        PutCondition::None => Ok(()),
        PutCondition::IfNoneMatch => {
            if current.is_some() {
                Err(Error::PreconditionFailed)
            } else {
                Ok(())
            }
        }
        PutCondition::IfMatch(etag) => match current {
            Some(c) if &c.etag == etag => Ok(()),
            _ => Err(Error::PreconditionFailed),
        },
    }
}

/// The composite object-table key: `[bucket_len: u8][bucket][object_key]`.
///
/// Length-prefixing the bucket keeps all of a bucket's keys contiguous and sorted
/// by object key, so prefix scans and listings work with a single range query.
fn composite_key(bucket: &str, key: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + bucket.len() + key.len());
    v.push(bucket.len() as u8);
    v.extend_from_slice(bucket.as_bytes());
    v.extend_from_slice(key.as_bytes());
    v
}

/// The shared key prefix for every object in a bucket.
fn bucket_prefix(bucket: &str) -> Vec<u8> {
    composite_key(bucket, "")
}

/// The smallest key strictly greater than every key beginning with `prefix`, or
/// `None` if `prefix` is empty or all `0xFF` (i.e. unbounded above).
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.last_mut() {
        if *last < 0xFF {
            *last += 1;
            return Some(end);
        }
        end.pop();
    }
    None
}

/// Whether any key beginning with `prefix` exists in the table.
fn range_has_any(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    prefix: &[u8],
) -> Result<bool> {
    let end = prefix_end(prefix);
    let lower = Bound::Included(prefix);
    let upper = match &end {
        Some(e) => Bound::Excluded(e.as_slice()),
        None => Bound::Unbounded,
    };
    Ok(table.range::<&[u8]>((lower, upper))?.next().is_some())
}

/// Validate a bucket name. M0 keeps this permissive: non-empty and short enough
/// to length-prefix in a single byte.
fn validate_bucket_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 255 {
        return Err(Error::InvalidBucketName(name.to_string()));
    }
    Ok(())
}
