//! Metadata role: a gRPC server wrapping a local `MetadataStore`, and a client
//! implementing `MetadataStore` against it.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

use tokio::sync::{mpsc, oneshot};

use soma_core::ObjectId;
use soma_meta::{
    BucketMeta, BucketOpts, BucketUsage, Error, ListRequest, ListResult, MetadataStore, NodeInfo,
    NodeState, NodeTopology, ObjectMeta, ObjectPut, ObjectPutItem, PgPlacement, PutCondition, Quota,
    RateLimit, Result, Version,
};

use crate::bridge::Bridge;
use crate::pb;
use crate::wire::{MetaReply, MetaRequest, WireError};

const MAX_MSG: usize = 64 * 1024 * 1024;

/// Upper bound on object commits coalesced into a single metadata transaction.
/// Bounds the work (and memory) of one commit; under steady load the batch is
/// usually whatever arrived while the previous commit was in flight, well under
/// this cap.
const MAX_COMMIT_BATCH: usize = 512;

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

struct MetaService {
    store: Arc<dyn MetadataStore>,
    commits: CommitBatcher,
}

#[tonic::async_trait]
impl pb::meta_server::Meta for MetaService {
    async fn call(
        &self,
        request: Request<pb::Frame>,
    ) -> std::result::Result<Response<pb::Frame>, Status> {
        let payload = request.into_inner().payload;
        let req: MetaRequest =
            postcard::from_bytes(&payload).map_err(|e| Status::invalid_argument(e.to_string()))?;
        // Object commits flow through the batcher so concurrent PUTs coalesce
        // into one durable transaction (group commit). Everything else dispatches
        // straight to the store on a blocking thread.
        let result: std::result::Result<MetaReply, WireError> = match req {
            MetaRequest::PutObject {
                bucket,
                key,
                put,
                cond,
            } => self
                .commits
                .commit(ObjectPutItem {
                    bucket,
                    key,
                    put,
                    cond,
                })
                .await
                .map(MetaReply::Version)
                .map_err(|e| WireError {
                    kind: e.kind().to_string(),
                    message: e.to_string(),
                }),
            other => {
                let store = self.store.clone();
                tokio::task::spawn_blocking(move || dispatch(store.as_ref(), other))
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
            }
        };
        let bytes = postcard::to_allocvec(&result).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(pb::Frame { payload: bytes }))
    }
}

// ---------------------------------------------------------------------------
// Commit batcher (group commit for object metadata)
// ---------------------------------------------------------------------------

/// One queued object commit plus the channel its result returns on.
struct CommitJob {
    item: ObjectPutItem,
    resp: oneshot::Sender<Result<Version>>,
}

/// Coalesces concurrent object commits into single durable transactions.
///
/// A dedicated worker pulls the next job, then greedily drains everything else
/// already queued (up to [`MAX_COMMIT_BATCH`]) and applies the lot in one
/// [`MetadataStore::put_object_batch`]. Because the drain happens *after* the
/// previous batch's commit returns, the batch is naturally exactly the set of
/// requests that piled up during that fsync — large batches under load, a batch
/// of one when idle. No timer, so latency is never traded away artificially.
struct CommitBatcher {
    tx: mpsc::UnboundedSender<CommitJob>,
}

impl CommitBatcher {
    fn spawn(store: Arc<dyn MetadataStore>) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<CommitJob>();
        tokio::spawn(async move {
            while let Some(first) = rx.recv().await {
                let mut jobs = vec![first];
                while jobs.len() < MAX_COMMIT_BATCH {
                    match rx.try_recv() {
                        Ok(job) => jobs.push(job),
                        Err(_) => break,
                    }
                }
                let mut items = Vec::with_capacity(jobs.len());
                let mut resps = Vec::with_capacity(jobs.len());
                for job in jobs {
                    items.push(job.item);
                    resps.push(job.resp);
                }
                let store = store.clone();
                let outcome =
                    tokio::task::spawn_blocking(move || store.put_object_batch(items)).await;
                match outcome {
                    Ok(results) => {
                        for (resp, res) in resps.into_iter().zip(results) {
                            let _ = resp.send(res);
                        }
                    }
                    Err(e) => {
                        // The blocking task panicked/cancelled: nothing committed.
                        let msg = format!("commit batch task failed: {e}");
                        for resp in resps {
                            let _ = resp.send(Err(Error::Remote(msg.clone())));
                        }
                    }
                }
            }
        });
        Self { tx }
    }

    async fn commit(&self, item: ObjectPutItem) -> Result<Version> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(CommitJob {
                item,
                resp: resp_tx,
            })
            .map_err(|_| Error::Remote("commit batcher stopped".to_string()))?;
        resp_rx
            .await
            .map_err(|_| Error::Remote("commit batcher dropped response".to_string()))?
    }
}

fn dispatch(
    store: &dyn MetadataStore,
    req: MetaRequest,
) -> std::result::Result<MetaReply, WireError> {
    let reply: Result<MetaReply> = match req {
        MetaRequest::CreateBucket { name, opts } => {
            store.create_bucket(&name, opts).map(|()| MetaReply::Unit)
        }
        MetaRequest::DeleteBucket { name } => store.delete_bucket(&name).map(|()| MetaReply::Unit),
        MetaRequest::GetBucket { name } => store.get_bucket(&name).map(MetaReply::Bucket),
        MetaRequest::SetBucketEncryption { name, algo } => store
            .set_bucket_encryption(&name, algo)
            .map(|()| MetaReply::Unit),
        MetaRequest::SetBucketQuota { name, quota } => store
            .set_bucket_quota(&name, quota)
            .map(|()| MetaReply::Unit),
        MetaRequest::SetBucketRateLimit { name, limit } => store
            .set_bucket_rate_limit(&name, limit)
            .map(|()| MetaReply::Unit),
        MetaRequest::SetBucketPolicy {
            name,
            owner,
            public_read,
            readers,
        } => store
            .set_bucket_policy(&name, &owner, public_read, readers)
            .map(|()| MetaReply::Unit),
        MetaRequest::ListBuckets => store.list_buckets().map(MetaReply::Buckets),
        MetaRequest::PutObject {
            bucket,
            key,
            put,
            cond,
        } => store
            .put_object(&bucket, &key, put, cond)
            .map(MetaReply::Version),
        MetaRequest::GetObject { bucket, key } => {
            store.get_object(&bucket, &key).map(MetaReply::Object)
        }
        MetaRequest::DeleteObject { bucket, key, cond } => store
            .delete_object(&bucket, &key, cond)
            .map(|()| MetaReply::Unit),
        MetaRequest::ListObjects { bucket, req } => {
            store.list_objects(&bucket, &req).map(MetaReply::List)
        }
        MetaRequest::ReserveObjectIds { count } => store
            .reserve_object_ids(count)
            .map(|(start, len)| MetaReply::ObjectIdRange { start, len }),
        MetaRequest::BucketUsage { bucket } => store.bucket_usage(&bucket).map(MetaReply::Usage),
        MetaRequest::MarkGarbage { object_ids } => {
            store.mark_garbage(&object_ids).map(|()| MetaReply::Unit)
        }
        MetaRequest::RegisterNode {
            node_id,
            endpoint,
            zone,
            host,
            now,
        } => store
            .register_node(&node_id, &endpoint, NodeTopology { zone, host }, now)
            .map(|()| MetaReply::Unit),
        MetaRequest::Heartbeat { node_id, now } => {
            store.heartbeat(&node_id, now).map(|()| MetaReply::Unit)
        }
        MetaRequest::SetNodeState { node_id, state } => store
            .set_node_state(&node_id, state)
            .map(|()| MetaReply::Unit),
        MetaRequest::ListMembers => store.list_members().map(MetaReply::Members),
        MetaRequest::SeedPgTable { entries } => {
            store.seed_pg_table(&entries).map(MetaReply::Seeded)
        }
        MetaRequest::ListPgTable => store.list_pg_table().map(MetaReply::PgTable),
    };
    reply.map_err(|e| WireError {
        kind: e.kind().to_string(),
        message: e.to_string(),
    })
}

/// Serve the metadata `MetadataStore` over gRPC at `addr`.
pub async fn serve_meta(
    addr: SocketAddr,
    store: Arc<dyn MetadataStore>,
) -> std::result::Result<(), tonic::transport::Error> {
    let commits = CommitBatcher::spawn(store.clone());
    let svc = pb::meta_server::MetaServer::new(MetaService { store, commits })
        .max_decoding_message_size(MAX_MSG)
        .max_encoding_message_size(MAX_MSG);
    Server::builder().add_service(svc).serve(addr).await
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// How many object ids a gateway reserves per round trip, then hands out locally.
/// Larger = fewer reservation RPCs under load; a gateway restart abandons the
/// unused tail (gaps are harmless — ids only need to be unique and increasing).
const CLIENT_ID_BLOCK: u64 = 256;

/// A gateway-local cursor over a reserved, durable id block: ids in
/// `[next, end)` were handed to this client by the meta node and serve without a
/// round trip.
#[derive(Default)]
struct IdRange {
    next: u64,
    end: u64,
}

/// A `MetadataStore` implemented by RPC to a remote metadata node.
pub struct MetaClient {
    channel: Channel,
    bridge: Bridge,
    /// Gateway-side hi-lo cache: object ids are reserved a block at a time and
    /// served locally, so a steady stream of PUTs costs one reservation RPC per
    /// [`CLIENT_ID_BLOCK`] objects instead of one per object.
    id_cache: Mutex<IdRange>,
}

impl MetaClient {
    /// Connect (lazily) to a metadata node, e.g. `http://meta:9100`. The TCP
    /// connection is established on first use and reconnects automatically.
    pub async fn connect(
        endpoint: String,
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let channel = Channel::from_shared(endpoint)?.connect_lazy();
        Ok(Self {
            channel,
            bridge: Bridge::new(),
            id_cache: Mutex::new(IdRange::default()),
        })
    }

    fn call(&self, req: MetaRequest) -> Result<MetaReply> {
        let payload = postcard::to_allocvec(&req).map_err(|e| Error::Remote(e.to_string()))?;
        let channel = self.channel.clone();
        self.bridge
            .run(async move {
                let mut client = pb::meta_client::MetaClient::new(channel)
                    .max_decoding_message_size(MAX_MSG)
                    .max_encoding_message_size(MAX_MSG);
                let resp = client
                    .call(pb::Frame { payload })
                    .await
                    .map_err(|e| Error::Remote(e.to_string()))?;
                let reply: std::result::Result<MetaReply, WireError> =
                    postcard::from_bytes(&resp.into_inner().payload)
                        .map_err(|e| Error::Remote(e.to_string()))?;
                reply.map_err(|w| Error::from_remote(&w.kind, w.message))
            })
            .map_err(|_| Error::Remote("rpc bridge closed".to_string()))?
    }
}

fn unexpected() -> Error {
    Error::Remote("unexpected metadata reply".to_string())
}

impl MetadataStore for MetaClient {
    fn create_bucket(&self, name: &str, opts: BucketOpts) -> Result<()> {
        match self.call(MetaRequest::CreateBucket {
            name: name.to_string(),
            opts,
        })? {
            MetaReply::Unit => Ok(()),
            _ => Err(unexpected()),
        }
    }

    fn delete_bucket(&self, name: &str) -> Result<()> {
        match self.call(MetaRequest::DeleteBucket {
            name: name.to_string(),
        })? {
            MetaReply::Unit => Ok(()),
            _ => Err(unexpected()),
        }
    }

    fn get_bucket(&self, name: &str) -> Result<Option<BucketMeta>> {
        match self.call(MetaRequest::GetBucket {
            name: name.to_string(),
        })? {
            MetaReply::Bucket(b) => Ok(b),
            _ => Err(unexpected()),
        }
    }

    fn set_bucket_encryption(
        &self,
        name: &str,
        algo: Option<soma_meta::SseAlgorithm>,
    ) -> Result<()> {
        match self.call(MetaRequest::SetBucketEncryption {
            name: name.to_string(),
            algo,
        })? {
            MetaReply::Unit => Ok(()),
            _ => Err(unexpected()),
        }
    }

    fn set_bucket_quota(&self, name: &str, quota: Quota) -> Result<()> {
        match self.call(MetaRequest::SetBucketQuota {
            name: name.to_string(),
            quota,
        })? {
            MetaReply::Unit => Ok(()),
            _ => Err(unexpected()),
        }
    }

    fn set_bucket_rate_limit(&self, name: &str, limit: RateLimit) -> Result<()> {
        match self.call(MetaRequest::SetBucketRateLimit {
            name: name.to_string(),
            limit,
        })? {
            MetaReply::Unit => Ok(()),
            _ => Err(unexpected()),
        }
    }

    fn set_bucket_policy(
        &self,
        name: &str,
        owner: &str,
        public_read: bool,
        readers: Vec<String>,
    ) -> Result<()> {
        match self.call(MetaRequest::SetBucketPolicy {
            name: name.to_string(),
            owner: owner.to_string(),
            public_read,
            readers,
        })? {
            MetaReply::Unit => Ok(()),
            _ => Err(unexpected()),
        }
    }

    fn list_buckets(&self) -> Result<Vec<BucketMeta>> {
        match self.call(MetaRequest::ListBuckets)? {
            MetaReply::Buckets(b) => Ok(b),
            _ => Err(unexpected()),
        }
    }

    fn put_object(
        &self,
        bucket: &str,
        key: &str,
        put: ObjectPut,
        cond: PutCondition,
    ) -> Result<Version> {
        match self.call(MetaRequest::PutObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
            put,
            cond,
        })? {
            MetaReply::Version(v) => Ok(v),
            _ => Err(unexpected()),
        }
    }

    fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        match self.call(MetaRequest::GetObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })? {
            MetaReply::Object(o) => Ok(o),
            _ => Err(unexpected()),
        }
    }

    fn delete_object(&self, bucket: &str, key: &str, cond: PutCondition) -> Result<()> {
        match self.call(MetaRequest::DeleteObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
            cond,
        })? {
            MetaReply::Unit => Ok(()),
            _ => Err(unexpected()),
        }
    }

    fn list_objects(&self, bucket: &str, req: &ListRequest) -> Result<ListResult> {
        match self.call(MetaRequest::ListObjects {
            bucket: bucket.to_string(),
            req: req.clone(),
        })? {
            MetaReply::List(r) => Ok(r),
            _ => Err(unexpected()),
        }
    }

    fn next_object_id(&self) -> Result<ObjectId> {
        let mut cache = self
            .id_cache
            .lock()
            .map_err(|_| Error::Remote("id cache lock poisoned".to_string()))?;
        if cache.next >= cache.end {
            // Block drained: reserve a fresh one in a single round trip, then serve
            // the rest locally. (The node may return fewer than requested.)
            let (start, len) = self.reserve_object_ids(CLIENT_ID_BLOCK)?;
            cache.next = start;
            cache.end = start + len;
        }
        let id = cache.next;
        cache.next += 1;
        Ok(id)
    }

    fn reserve_object_ids(&self, count: u64) -> Result<(ObjectId, u64)> {
        match self.call(MetaRequest::ReserveObjectIds { count })? {
            MetaReply::ObjectIdRange { start, len } => Ok((start, len)),
            _ => Err(unexpected()),
        }
    }

    fn bucket_usage(&self, bucket: &str) -> Result<BucketUsage> {
        match self.call(MetaRequest::BucketUsage {
            bucket: bucket.to_string(),
        })? {
            MetaReply::Usage(u) => Ok(u),
            _ => Err(unexpected()),
        }
    }

    fn mark_garbage(&self, object_ids: &[ObjectId]) -> Result<()> {
        match self.call(MetaRequest::MarkGarbage {
            object_ids: object_ids.to_vec(),
        })? {
            MetaReply::Unit => Ok(()),
            _ => Err(unexpected()),
        }
    }

    fn register_node(
        &self,
        node_id: &str,
        endpoint: &str,
        topology: NodeTopology,
        now: u64,
    ) -> Result<()> {
        match self.call(MetaRequest::RegisterNode {
            node_id: node_id.to_string(),
            endpoint: endpoint.to_string(),
            zone: topology.zone,
            host: topology.host,
            now,
        })? {
            MetaReply::Unit => Ok(()),
            _ => Err(unexpected()),
        }
    }

    fn heartbeat(&self, node_id: &str, now: u64) -> Result<()> {
        match self.call(MetaRequest::Heartbeat {
            node_id: node_id.to_string(),
            now,
        })? {
            MetaReply::Unit => Ok(()),
            _ => Err(unexpected()),
        }
    }

    fn set_node_state(&self, node_id: &str, state: NodeState) -> Result<()> {
        match self.call(MetaRequest::SetNodeState {
            node_id: node_id.to_string(),
            state,
        })? {
            MetaReply::Unit => Ok(()),
            _ => Err(unexpected()),
        }
    }

    fn list_members(&self) -> Result<Vec<NodeInfo>> {
        match self.call(MetaRequest::ListMembers)? {
            MetaReply::Members(m) => Ok(m),
            _ => Err(unexpected()),
        }
    }

    fn seed_pg_table(&self, entries: &[(u32, PgPlacement)]) -> Result<bool> {
        match self.call(MetaRequest::SeedPgTable {
            entries: entries.to_vec(),
        })? {
            MetaReply::Seeded(b) => Ok(b),
            _ => Err(unexpected()),
        }
    }

    fn list_pg_table(&self) -> Result<Vec<(u32, PgPlacement)>> {
        match self.call(MetaRequest::ListPgTable)? {
            MetaReply::PgTable(t) => Ok(t),
            _ => Err(unexpected()),
        }
    }
}
