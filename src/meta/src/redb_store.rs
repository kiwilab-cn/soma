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
    BucketMeta, BucketOpts, ListRequest, ListResult, NodeInfo, NodeState, ObjectEntry, ObjectMeta,
    ObjectPut, PgPlacement, PutCondition, TenantUsage, Version,
};
use crate::MetadataStore;

const BUCKETS: TableDefinition<&str, &[u8]> = TableDefinition::new("buckets");
const OBJECTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("objects");
const SEQ: TableDefinition<&str, u64> = TableDefinition::new("seq");
const USAGE: TableDefinition<&str, &[u8]> = TableDefinition::new("tenant_usage");
const MEMBERS: TableDefinition<&str, &[u8]> = TableDefinition::new("members");
const PG_TABLE: TableDefinition<u32, &[u8]> = TableDefinition::new("pg_table");

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
            w.open_table(USAGE)?;
            w.open_table(MEMBERS)?;
            w.open_table(PG_TABLE)?;
        }
        w.commit()?;
        Ok(Self { db })
    }

    // --- rebalance controller support (M3b) --------------------------------
    //
    // These are inherent (not on the `MetadataStore` trait): only the controller,
    // which runs in-process with the concrete store, drives migration. Gateways
    // observe migration purely through `list_pg_table` (the `target` field).

    /// Mark a placement group as migrating to `target` (bumping its generation).
    /// No-op if the PG is absent (the table is seeded before the controller runs).
    pub fn begin_migration(&self, pg: u32, target: Vec<String>) -> Result<()> {
        let w = self.db.begin_write()?;
        {
            let mut t = w.open_table(PG_TABLE)?;
            let raw = match t.get(pg)? {
                Some(g) => g.value().to_vec(),
                None => return Ok(()),
            };
            let mut placement: PgPlacement = postcard::from_bytes(&raw)?;
            placement.target = target;
            placement.generation += 1;
            t.insert(pg, postcard::to_allocvec(&placement)?.as_slice())?;
        }
        w.commit()?;
        Ok(())
    }

    /// Finalize a migration: the target set becomes the acting set, clearing the
    /// migration. No-op if the PG is not migrating.
    pub fn finalize_migration(&self, pg: u32) -> Result<()> {
        let w = self.db.begin_write()?;
        {
            let mut t = w.open_table(PG_TABLE)?;
            let raw = match t.get(pg)? {
                Some(g) => g.value().to_vec(),
                None => return Ok(()),
            };
            let mut placement: PgPlacement = postcard::from_bytes(&raw)?;
            if !placement.target.is_empty() {
                placement.node_ids = std::mem::take(&mut placement.target);
                placement.generation += 1;
                t.insert(pg, postcard::to_allocvec(&placement)?.as_slice())?;
            }
        }
        w.commit()?;
        Ok(())
    }

    /// One placement group's placement, if present.
    pub fn pg_placement(&self, pg: u32) -> Result<Option<PgPlacement>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(PG_TABLE)?;
        match t.get(pg)? {
            Some(g) => Ok(Some(postcard::from_bytes(g.value())?)),
            None => Ok(None),
        }
    }

    /// The object ids of every live object (for the mover to enumerate a PG's
    /// objects: an object belongs to `pg = H(object_id) % pg_count`).
    pub fn list_object_ids(&self) -> Result<Vec<ObjectId>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(OBJECTS)?;
        let mut out = Vec::new();
        for item in t.iter()? {
            let (_, v) = item?;
            let m: ObjectMeta = postcard::from_bytes(v.value())?;
            out.push(m.object_id);
        }
        Ok(out)
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

            // Quota accounting, atomic with the object write. Only when the put
            // carries a tenant; overwriting refunds the prior version's owner
            // first, so usage tracks the live (current) object set.
            if !put.tenant.is_empty() {
                let mut usage = w.open_table(USAGE)?;
                if let Some(c) = current.as_ref().filter(|c| !c.tenant.is_empty()) {
                    let mut prev = read_usage(&usage, &c.tenant)?;
                    prev.bytes = prev.bytes.saturating_sub(c.size);
                    prev.objects = prev.objects.saturating_sub(1);
                    write_usage(&mut usage, &c.tenant, prev)?;
                }
                let mut u = read_usage(&usage, &put.tenant)?;
                u.bytes += put.size;
                u.objects += 1;
                let q = put.quota;
                if (q.max_bytes > 0 && u.bytes > q.max_bytes)
                    || (q.max_objects > 0 && u.objects > q.max_objects)
                {
                    // Returning here drops the write transaction — nothing commits.
                    return Err(Error::QuotaExceeded(format!(
                        "tenant {} would use {} bytes / {} objects (limits {} / {})",
                        put.tenant, u.bytes, u.objects, q.max_bytes, q.max_objects
                    )));
                }
                write_usage(&mut usage, &put.tenant, u)?;
            }

            new_version = Version(current.as_ref().map_or(1, |c| c.version.0 + 1));
            let meta = ObjectMeta {
                object_id: put.object_id,
                size: put.size,
                etag: put.etag,
                version: new_version,
                created_at: put.created_at,
                tenant: put.tenant,
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
            if let Some(c) = current {
                // Refund the owning tenant's usage before removing the object.
                if !c.tenant.is_empty() {
                    let mut usage = w.open_table(USAGE)?;
                    let mut u = read_usage(&usage, &c.tenant)?;
                    u.bytes = u.bytes.saturating_sub(c.size);
                    u.objects = u.objects.saturating_sub(1);
                    write_usage(&mut usage, &c.tenant, u)?;
                }
                objects.remove(ck.as_slice())?;
            }
        }
        w.commit()?;
        Ok(())
    }

    fn tenant_usage(&self, tenant: &str) -> Result<TenantUsage> {
        let r = self.db.begin_read()?;
        let usage = r.open_table(USAGE)?;
        read_usage(&usage, tenant)
    }

    fn register_node(&self, node_id: &str, endpoint: &str, now: u64) -> Result<()> {
        let w = self.db.begin_write()?;
        {
            let mut t = w.open_table(MEMBERS)?;
            let prev_gen = match t.get(node_id)? {
                Some(g) => postcard::from_bytes::<NodeInfo>(g.value())?.generation,
                None => 0,
            };
            let info = NodeInfo {
                node_id: node_id.to_string(),
                endpoint: endpoint.to_string(),
                state: NodeState::Active,
                last_heartbeat: now,
                generation: prev_gen + 1,
            };
            t.insert(node_id, postcard::to_allocvec(&info)?.as_slice())?;
        }
        w.commit()?;
        Ok(())
    }

    fn heartbeat(&self, node_id: &str, now: u64) -> Result<()> {
        let w = self.db.begin_write()?;
        {
            let mut t = w.open_table(MEMBERS)?;
            let raw = match t.get(node_id)? {
                Some(g) => g.value().to_vec(),
                None => return Err(Error::UnknownNode(node_id.to_string())),
            };
            let mut info: NodeInfo = postcard::from_bytes(&raw)?;
            info.last_heartbeat = now;
            if info.state == NodeState::Down {
                info.state = NodeState::Active;
            }
            t.insert(node_id, postcard::to_allocvec(&info)?.as_slice())?;
        }
        w.commit()?;
        Ok(())
    }

    fn set_node_state(&self, node_id: &str, state: NodeState) -> Result<()> {
        let w = self.db.begin_write()?;
        {
            let mut t = w.open_table(MEMBERS)?;
            let raw = match t.get(node_id)? {
                Some(g) => g.value().to_vec(),
                None => return Err(Error::UnknownNode(node_id.to_string())),
            };
            let mut info: NodeInfo = postcard::from_bytes(&raw)?;
            info.state = state;
            t.insert(node_id, postcard::to_allocvec(&info)?.as_slice())?;
        }
        w.commit()?;
        Ok(())
    }

    fn list_members(&self) -> Result<Vec<NodeInfo>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(MEMBERS)?;
        let mut out = Vec::new();
        for item in t.iter()? {
            let (_, v) = item?;
            out.push(postcard::from_bytes(v.value())?);
        }
        Ok(out)
    }

    fn seed_pg_table(&self, entries: &[(u32, PgPlacement)]) -> Result<bool> {
        let w = self.db.begin_write()?;
        let seeded;
        {
            let mut t = w.open_table(PG_TABLE)?;
            // Seed only when empty, atomically — concurrent gateways race-free.
            let already_populated = t.iter()?.next().is_some();
            if already_populated {
                seeded = false;
            } else {
                for (pg, placement) in entries {
                    t.insert(*pg, postcard::to_allocvec(placement)?.as_slice())?;
                }
                seeded = true;
            }
        }
        w.commit()?;
        Ok(seeded)
    }

    fn list_pg_table(&self) -> Result<Vec<(u32, PgPlacement)>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(PG_TABLE)?;
        let mut out = Vec::new();
        for item in t.iter()? {
            let (k, v) = item?;
            out.push((k.value(), postcard::from_bytes(v.value())?));
        }
        Ok(out)
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
                created_at: meta.created_at,
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

/// Read a tenant's usage row from the usage table (zero if absent).
fn read_usage(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    tenant: &str,
) -> Result<TenantUsage> {
    match table.get(tenant)? {
        Some(g) => Ok(postcard::from_bytes(g.value())?),
        None => Ok(TenantUsage::default()),
    }
}

/// Write a tenant's usage row.
fn write_usage(
    table: &mut redb::Table<&'static str, &'static [u8]>,
    tenant: &str,
    usage: TenantUsage,
) -> Result<()> {
    table.insert(tenant, postcard::to_allocvec(&usage)?.as_slice())?;
    Ok(())
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
