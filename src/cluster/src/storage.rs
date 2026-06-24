//! Storage role: a gRPC server wrapping a local `StorageBackend`, and a client
//! implementing `StorageBackend` against it.

use std::net::SocketAddr;
use std::sync::Arc;

use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

use soma_backend::{ByteRange, Error, Result, StorageBackend};
use soma_core::{ObjectId, ObjectLocation};

use crate::bridge::Bridge;
use crate::pb;
use crate::wire::{StorageReply, StorageRequest, WireError};

const MAX_MSG: usize = 64 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

struct StorageService {
    backend: Arc<dyn StorageBackend>,
}

#[tonic::async_trait]
impl pb::storage_server::Storage for StorageService {
    async fn call(
        &self,
        request: Request<pb::Frame>,
    ) -> std::result::Result<Response<pb::Frame>, Status> {
        let payload = request.into_inner().payload;
        let req: StorageRequest =
            postcard::from_bytes(&payload).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let backend = self.backend.clone();
        let result = tokio::task::spawn_blocking(move || dispatch(backend.as_ref(), req))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let bytes = postcard::to_allocvec(&result).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(pb::Frame { payload: bytes }))
    }
}

fn dispatch(
    backend: &dyn StorageBackend,
    req: StorageRequest,
) -> std::result::Result<StorageReply, WireError> {
    let reply: Result<StorageReply> = match req {
        StorageRequest::Put { object_id, data } => {
            backend.put(object_id, &data).map(StorageReply::Location)
        }
        StorageRequest::Get { location, range } => backend
            .get(
                location,
                range.map(|(offset, length)| ByteRange { offset, length }),
            )
            .map(StorageReply::Data),
        StorageRequest::Delete { object_id } => {
            backend.delete(object_id).map(StorageReply::Location)
        }
        StorageRequest::Sync => backend.sync().map(|()| StorageReply::Unit),
        StorageRequest::Checkpoint => backend.checkpoint().map(|()| StorageReply::Unit),
    };
    reply.map_err(|e| WireError {
        kind: e.kind().to_string(),
        message: e.to_string(),
    })
}

/// Serve the storage `StorageBackend` over gRPC at `addr`.
pub async fn serve_storage(
    addr: SocketAddr,
    backend: Arc<dyn StorageBackend>,
) -> std::result::Result<(), tonic::transport::Error> {
    let svc = pb::storage_server::StorageServer::new(StorageService { backend })
        .max_decoding_message_size(MAX_MSG)
        .max_encoding_message_size(MAX_MSG);
    Server::builder().add_service(svc).serve(addr).await
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// A `StorageBackend` implemented by RPC to a remote storage node.
pub struct StorageClient {
    channel: Channel,
    bridge: Bridge,
}

impl StorageClient {
    /// Connect to a storage node, e.g. `http://storage:9200`.
    pub async fn connect(
        endpoint: String,
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let channel = Channel::from_shared(endpoint)?.connect().await?;
        Ok(Self {
            channel,
            bridge: Bridge::new(),
        })
    }

    fn call(&self, req: StorageRequest) -> Result<StorageReply> {
        let payload = postcard::to_allocvec(&req).map_err(|e| Error::Remote(e.to_string()))?;
        let channel = self.channel.clone();
        self.bridge
            .run(async move {
                let mut client = pb::storage_client::StorageClient::new(channel)
                    .max_decoding_message_size(MAX_MSG)
                    .max_encoding_message_size(MAX_MSG);
                let resp = client
                    .call(pb::Frame { payload })
                    .await
                    .map_err(|e| Error::Remote(e.to_string()))?;
                let reply: std::result::Result<StorageReply, WireError> =
                    postcard::from_bytes(&resp.into_inner().payload)
                        .map_err(|e| Error::Remote(e.to_string()))?;
                reply.map_err(|w| Error::from_remote(&w.kind, w.message))
            })
            .map_err(|_| Error::Remote("rpc bridge closed".to_string()))?
    }
}

fn unexpected() -> Error {
    Error::Remote("unexpected storage reply".to_string())
}

impl StorageBackend for StorageClient {
    fn put(&self, object_id: ObjectId, data: &[u8]) -> Result<ObjectLocation> {
        match self.call(StorageRequest::Put {
            object_id,
            data: data.to_vec(),
        })? {
            StorageReply::Location(loc) => Ok(loc),
            _ => Err(unexpected()),
        }
    }

    fn get(&self, loc: ObjectLocation, range: Option<ByteRange>) -> Result<Vec<u8>> {
        match self.call(StorageRequest::Get {
            location: loc,
            range: range.map(|r| (r.offset, r.length)),
        })? {
            StorageReply::Data(d) => Ok(d),
            _ => Err(unexpected()),
        }
    }

    fn delete(&self, object_id: ObjectId) -> Result<ObjectLocation> {
        match self.call(StorageRequest::Delete { object_id })? {
            StorageReply::Location(loc) => Ok(loc),
            _ => Err(unexpected()),
        }
    }

    fn sync(&self) -> Result<()> {
        match self.call(StorageRequest::Sync)? {
            StorageReply::Unit => Ok(()),
            _ => Err(unexpected()),
        }
    }

    fn checkpoint(&self) -> Result<()> {
        match self.call(StorageRequest::Checkpoint)? {
            StorageReply::Unit => Ok(()),
            _ => Err(unexpected()),
        }
    }
}
