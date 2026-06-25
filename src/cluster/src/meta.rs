//! Metadata role: a gRPC server wrapping a local `MetadataStore`, and a client
//! implementing `MetadataStore` against it.

use std::net::SocketAddr;
use std::sync::Arc;

use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

use soma_core::ObjectId;
use soma_meta::{
    BucketMeta, BucketOpts, Error, ListRequest, ListResult, MetadataStore, NodeInfo, NodeState,
    ObjectMeta, ObjectPut, PgPlacement, PutCondition, Result, TenantUsage, Version,
};

use crate::bridge::Bridge;
use crate::pb;
use crate::wire::{MetaReply, MetaRequest, WireError};

const MAX_MSG: usize = 64 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

struct MetaService {
    store: Arc<dyn MetadataStore>,
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
        let store = self.store.clone();
        let result = tokio::task::spawn_blocking(move || dispatch(store.as_ref(), req))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let bytes = postcard::to_allocvec(&result).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(pb::Frame { payload: bytes }))
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
        MetaRequest::NextObjectId => store.next_object_id().map(MetaReply::ObjectId),
        MetaRequest::TenantUsage { tenant } => store.tenant_usage(&tenant).map(MetaReply::Usage),
        MetaRequest::MarkGarbage { object_ids } => {
            store.mark_garbage(&object_ids).map(|()| MetaReply::Unit)
        }
        MetaRequest::RegisterNode {
            node_id,
            endpoint,
            now,
        } => store
            .register_node(&node_id, &endpoint, now)
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
    let svc = pb::meta_server::MetaServer::new(MetaService { store })
        .max_decoding_message_size(MAX_MSG)
        .max_encoding_message_size(MAX_MSG);
    Server::builder().add_service(svc).serve(addr).await
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// A `MetadataStore` implemented by RPC to a remote metadata node.
pub struct MetaClient {
    channel: Channel,
    bridge: Bridge,
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
        match self.call(MetaRequest::NextObjectId)? {
            MetaReply::ObjectId(id) => Ok(id),
            _ => Err(unexpected()),
        }
    }

    fn tenant_usage(&self, tenant: &str) -> Result<TenantUsage> {
        match self.call(MetaRequest::TenantUsage {
            tenant: tenant.to_string(),
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

    fn register_node(&self, node_id: &str, endpoint: &str, now: u64) -> Result<()> {
        match self.call(MetaRequest::RegisterNode {
            node_id: node_id.to_string(),
            endpoint: endpoint.to_string(),
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
