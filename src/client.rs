use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::kv::Store;
use async_nats::jetstream::object_store::ObjectStore;
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio::sync::OnceCell;

use crate::handle::SandboxHandle;
use crate::placement::{self, NODE_ALIVE_MAX_AGE, PlacementError, PlacementInputs, PlacedNode};
use crate::protocol::{
    CancelExecRequest, CancelExecResponse, CreateSandboxResponse, CreateTaskSandboxRequest,
    CreateThreadSandboxRequest, DeleteSandboxRequest, DeleteSandboxResponse, ExecSandboxRequest,
    ExecSandboxResponse, NodeRecord, SandboxResponse, SessionRecord,
};
use crate::protocol::{FsBlobHandle, FsReadRequest, FsReadResponse, FsWriteRequest, FsWriteResponse};
use crate::{
    AFFINITY_BUCKET, EXEC_OUTPUTS_BUCKET, FS_INLINE_LIMIT, FS_PAYLOADS_BUCKET, NODES_BUCKET,
    SESSIONS_BUCKET, affinity, subjects,
};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const CREATE_SANDBOX_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Error)]
pub enum SandboxNatsClientError {
    #[error("nats request failed: {0}")]
    Nats(String),
    #[error("daemon error: {0}")]
    Daemon(String),
    #[error("malformed response: {0}")]
    Decode(String),
    #[error("placement: {0}")]
    Placement(#[from] PlacementError),
}

#[derive(Clone)]
pub struct SandboxNatsClient {
    nats: async_nats::Client,
    jetstream: jetstream::Context,
    request_timeout: Duration,
    buckets: Arc<BucketCache>,
}

#[derive(Default)]
struct BucketCache {
    sessions: OnceCell<Store>,
    nodes: OnceCell<Store>,
    affinity: OnceCell<Store>,
    exec_outputs: OnceCell<ObjectStore>,
    fs_payloads: OnceCell<ObjectStore>,
}

impl SandboxNatsClient {
    pub fn new(nats: async_nats::Client) -> Self {
        let jetstream = jetstream::new(nats.clone());
        Self {
            nats,
            jetstream,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            buckets: Arc::new(BucketCache::default()),
        }
    }

    async fn sessions_store(&self) -> Result<&Store, SandboxNatsClientError> {
        self.buckets
            .sessions
            .get_or_try_init(|| async {
                self.jetstream
                    .get_key_value(SESSIONS_BUCKET)
                    .await
                    .map_err(|err| SandboxNatsClientError::Nats(err.to_string()))
            })
            .await
    }

    async fn nodes_store(&self) -> Result<&Store, SandboxNatsClientError> {
        self.buckets
            .nodes
            .get_or_try_init(|| async {
                self.jetstream
                    .get_key_value(NODES_BUCKET)
                    .await
                    .map_err(|err| SandboxNatsClientError::Nats(err.to_string()))
            })
            .await
    }

    async fn affinity_store(&self) -> Result<&Store, SandboxNatsClientError> {
        self.buckets
            .affinity
            .get_or_try_init(|| async {
                self.jetstream
                    .get_key_value(AFFINITY_BUCKET)
                    .await
                    .map_err(|err| SandboxNatsClientError::Nats(err.to_string()))
            })
            .await
    }

    async fn exec_outputs_store(&self) -> Result<&ObjectStore, SandboxNatsClientError> {
        self.buckets
            .exec_outputs
            .get_or_try_init(|| async {
                self.jetstream
                    .get_object_store(EXEC_OUTPUTS_BUCKET)
                    .await
                    .map_err(|err| SandboxNatsClientError::Nats(err.to_string()))
            })
            .await
    }

    async fn fs_payloads_store(&self) -> Result<&ObjectStore, SandboxNatsClientError> {
        self.buckets
            .fs_payloads
            .get_or_try_init(|| async {
                self.jetstream
                    .get_object_store(FS_PAYLOADS_BUCKET)
                    .await
                    .map_err(|err| SandboxNatsClientError::Nats(err.to_string()))
            })
            .await
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub async fn warm(&self) -> Result<(), SandboxNatsClientError> {
        let _ = tokio::try_join!(
            self.sessions_store(),
            self.nodes_store(),
            self.affinity_store(),
            self.exec_outputs_store(),
            self.fs_payloads_store(),
        )?;
        Ok(())
    }

    pub async fn thread(
        &self,
        request: &CreateThreadSandboxRequest,
    ) -> Result<SandboxHandle, SandboxNatsClientError> {
        let sandbox_id = format!("thread-{}-{}", request.deployment_id, request.thread_id);
        if let Some(handle) = self.try_attach(&sandbox_id).await? {
            tracing::info!(
                target: "wacht_sandbox_client",
                sandbox_id = %sandbox_id,
                node_id = %handle.node_id(),
                "thread: attached existing session",
            );
            return Ok(handle);
        }
        let affinity_key = affinity::thread_key(&request.deployment_id, &request.thread_id);
        let placed = self
            .place_with_retry(Some(&affinity_key))
            .await?;
        tracing::info!(
            target: "wacht_sandbox_client",
            sandbox_id = %sandbox_id,
            node_id = %placed.node_id,
            "thread: placed; creating",
        );
        let response = self.create_thread(&placed.node_id, request).await?;
        Ok(self.handle(placed.node_id, response.sandbox_id))
    }

    pub async fn task(
        &self,
        request: &CreateTaskSandboxRequest,
    ) -> Result<SandboxHandle, SandboxNatsClientError> {
        let sandbox_id = format!(
            "task-{}-{}-{}",
            request.deployment_id, request.project_id, request.task_key
        );
        if let Some(handle) = self.try_attach(&sandbox_id).await? {
            return Ok(handle);
        }
        let affinity_key = affinity::task_key(
            &request.deployment_id,
            &request.project_id,
            &request.task_key,
        );
        let placed = self
            .place_with_retry(Some(&affinity_key))
            .await?;
        let response = self.create_task(&placed.node_id, request).await?;
        Ok(self.handle(placed.node_id, response.sandbox_id))
    }

    async fn try_attach(
        &self,
        sandbox_id: &str,
    ) -> Result<Option<SandboxHandle>, SandboxNatsClientError> {
        let session = match self.read_session(sandbox_id).await? {
            Some(session) => session,
            None => {
                tracing::info!(
                    target: "wacht_sandbox_client",
                    sandbox_id = %sandbox_id,
                    "try_attach: no session record",
                );
                return Ok(None);
            }
        };
        let alive = self.is_node_alive(&session.node_id).await?;
        tracing::info!(
            target: "wacht_sandbox_client",
            sandbox_id = %sandbox_id,
            node_id = %session.node_id,
            alive,
            "try_attach: session validation",
        );
        if !alive {
            return Ok(None);
        }
        Ok(Some(self.handle(session.node_id, session.sandbox_id)))
    }

    pub(crate) async fn forget_session(&self, sandbox_id: &str) -> Result<(), SandboxNatsClientError> {
        let store = self.sessions_store().await?;
        let _ = store.delete(sandbox_id).await;
        Ok(())
    }

    async fn read_session(
        &self,
        sandbox_id: &str,
    ) -> Result<Option<SessionRecord>, SandboxNatsClientError> {
        let store = self.sessions_store().await?;
        let Some(entry) = store
            .get(sandbox_id)
            .await
            .map_err(|err| SandboxNatsClientError::Nats(err.to_string()))?
        else {
            return Ok(None);
        };
        let session: SessionRecord = serde_json::from_slice(&entry)
            .map_err(|err| SandboxNatsClientError::Decode(format!("decode session: {err}")))?;
        Ok(Some(session))
    }

    async fn is_node_alive(&self, node_id: &str) -> Result<bool, SandboxNatsClientError> {
        let store = self.nodes_store().await?;
        let Some(entry) = store
            .get(format!("node/{node_id}"))
            .await
            .map_err(|err| SandboxNatsClientError::Nats(err.to_string()))?
        else {
            return Ok(false);
        };
        let node: NodeRecord = serde_json::from_slice(&entry)
            .map_err(|err| SandboxNatsClientError::Decode(format!("decode node: {err}")))?;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Ok(now_ms.saturating_sub(node.last_seen_ms) <= NODE_ALIVE_MAX_AGE.as_millis() as u64)
    }

    pub(crate) fn handle(&self, node_id: String, sandbox_id: String) -> SandboxHandle {
        SandboxHandle::new(self.clone(), node_id, sandbox_id)
    }

    pub(crate) async fn place(
        &self,
        affinity_key: Option<&str>,
    ) -> Result<PlacedNode, SandboxNatsClientError> {
        let nodes = self.nodes_store().await?;
        let affinity = self.affinity_store().await?;
        Ok(placement::pick_node_for_deployment(
            nodes,
            affinity,
            PlacementInputs { affinity_key },
        )
        .await?)
    }

    pub(crate) async fn place_with_retry(
        &self,
        affinity_key: Option<&str>,
    ) -> Result<PlacedNode, SandboxNatsClientError> {
        const ATTEMPTS: u32 = 6;
        let mut delay = std::time::Duration::from_millis(500);
        let mut last_err: Option<SandboxNatsClientError> = None;
        for attempt in 0..ATTEMPTS {
            match self.place(affinity_key).await {
                Ok(node) => return Ok(node),
                Err(SandboxNatsClientError::Placement(PlacementError::NoNodes)) => {
                    tracing::warn!(
                        target: "wacht_sandbox_client",
                        attempt = attempt + 1,
                        delay_ms = delay.as_millis() as u64,
                        "place: no live nodes — backing off",
                    );
                    last_err = Some(SandboxNatsClientError::Placement(PlacementError::NoNodes));
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(std::time::Duration::from_secs(4));
                }
                Err(e) => return Err(e),
            }
        }
        Err(last_err.unwrap_or(SandboxNatsClientError::Placement(PlacementError::NoNodes)))
    }

    pub(crate) async fn create_thread(
        &self,
        node_id: &str,
        request: &CreateThreadSandboxRequest,
    ) -> Result<CreateSandboxResponse, SandboxNatsClientError> {
        self.request_with_timeout(
            &subjects::thread_create(node_id),
            request,
            CREATE_SANDBOX_TIMEOUT,
        )
        .await
    }

    pub(crate) async fn create_task(
        &self,
        node_id: &str,
        request: &CreateTaskSandboxRequest,
    ) -> Result<CreateSandboxResponse, SandboxNatsClientError> {
        self.request_with_timeout(
            &subjects::task_create(node_id),
            request,
            CREATE_SANDBOX_TIMEOUT,
        )
        .await
    }

    pub(crate) async fn exec(
        &self,
        node_id: &str,
        request: &ExecSandboxRequest,
        request_timeout: Duration,
    ) -> Result<ExecSandboxResponse, SandboxNatsClientError> {
        self.request_with_timeout(&subjects::exec(node_id), request, request_timeout)
            .await
    }

    pub(crate) async fn cancel_exec(
        &self,
        node_id: &str,
        request: &CancelExecRequest,
    ) -> Result<CancelExecResponse, SandboxNatsClientError> {
        self.request(&subjects::exec_cancel(node_id), request).await
    }

    pub(crate) async fn delete(
        &self,
        node_id: &str,
        request: &DeleteSandboxRequest,
    ) -> Result<DeleteSandboxResponse, SandboxNatsClientError> {
        self.request(&subjects::delete(node_id), request).await
    }

    pub(crate) async fn fs_read(
        &self,
        node_id: &str,
        sandbox_id: &str,
        path: &str,
    ) -> Result<Vec<u8>, SandboxNatsClientError> {
        let response: FsReadResponse = self
            .request(
                &subjects::fs_read(node_id),
                &FsReadRequest {
                    sandbox_id: sandbox_id.to_string(),
                    path: path.to_string(),
                },
            )
            .await?;
        if response.in_object_store {
            let handle = response.handle.ok_or_else(|| {
                SandboxNatsClientError::Decode("fs_read: missing object store handle".into())
            })?;
            self.read_fs_object(&handle).await
        } else {
            Ok(response.inline)
        }
    }

    pub(crate) async fn fs_write(
        &self,
        node_id: &str,
        sandbox_id: &str,
        path: &str,
        content: &[u8],
    ) -> Result<FsWriteResponse, SandboxNatsClientError> {
        let request = if content.len() <= FS_INLINE_LIMIT {
            FsWriteRequest {
                sandbox_id: sandbox_id.to_string(),
                path: path.to_string(),
                size_bytes: content.len() as u64,
                in_object_store: false,
                inline: content.to_vec(),
                handle: None,
            }
        } else {
            let key = format!(
                "{sandbox_id}/write/{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0),
            );
            self.put_fs_object(&key, content).await?;
            FsWriteRequest {
                sandbox_id: sandbox_id.to_string(),
                path: path.to_string(),
                size_bytes: content.len() as u64,
                in_object_store: true,
                inline: Vec::new(),
                handle: Some(FsBlobHandle {
                    bucket: FS_PAYLOADS_BUCKET.to_string(),
                    key,
                }),
            }
        };
        self.request(&subjects::fs_write(node_id), &request).await
    }

    async fn read_fs_object(
        &self,
        handle: &FsBlobHandle,
    ) -> Result<Vec<u8>, SandboxNatsClientError> {
        let os = self.fs_payloads_store().await?;
        let mut obj = os
            .get(&handle.key)
            .await
            .map_err(|err| SandboxNatsClientError::Nats(format!("fs_read get {}: {err}", handle.key)))?;
        let mut buf = Vec::new();
        use tokio::io::AsyncReadExt;
        obj.read_to_end(&mut buf)
            .await
            .map_err(|err| SandboxNatsClientError::Nats(format!("fs_read body {}: {err}", handle.key)))?;
        Ok(buf)
    }

    async fn put_fs_object(
        &self,
        key: &str,
        content: &[u8],
    ) -> Result<(), SandboxNatsClientError> {
        let os = self.fs_payloads_store().await?;
        let mut cursor = std::io::Cursor::new(content.to_vec());
        os.put(key, &mut cursor)
            .await
            .map_err(|err| SandboxNatsClientError::Nats(format!("fs_write put {key}: {err}")))?;
        Ok(())
    }

    pub(crate) async fn read_exec_object(
        &self,
        prefix: &str,
        stream: &str,
    ) -> Result<Vec<u8>, SandboxNatsClientError> {
        let os = self.exec_outputs_store().await?;
        let key = format!("{prefix}/{stream}");
        let mut obj = os
            .get(&key)
            .await
            .map_err(|err| SandboxNatsClientError::Nats(format!("get {key}: {err}")))?;
        let mut buf = Vec::new();
        use tokio::io::AsyncReadExt;
        obj.read_to_end(&mut buf)
            .await
            .map_err(|err| SandboxNatsClientError::Nats(format!("read {key}: {err}")))?;
        Ok(buf)
    }

    async fn request<Req, Res>(
        &self,
        subject: &str,
        body: &Req,
    ) -> Result<Res, SandboxNatsClientError>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        self.request_with_timeout(subject, body, self.request_timeout)
            .await
    }

    async fn request_with_timeout<Req, Res>(
        &self,
        subject: &str,
        body: &Req,
        timeout: Duration,
    ) -> Result<Res, SandboxNatsClientError>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        let payload = serde_json::to_vec(body)
            .map_err(|err| SandboxNatsClientError::Decode(format!("encode request: {err}")))?;
        let started = std::time::Instant::now();
        tracing::info!(
            target: "wacht_sandbox_client",
            subject = %subject,
            timeout_ms = timeout.as_millis() as u64,
            payload_bytes = payload.len(),
            "nats request start",
        );
        let response = tokio::time::timeout(
            timeout,
            self.nats.request(subject.to_string(), payload.into()),
        )
        .await
        .map_err(|_| {
            tracing::warn!(
                target: "wacht_sandbox_client",
                subject = %subject,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "nats request timed out",
            );
            SandboxNatsClientError::Nats(format!("timed out waiting for {subject}"))
        })?
        .map_err(|err| {
            tracing::warn!(
                target: "wacht_sandbox_client",
                subject = %subject,
                elapsed_ms = started.elapsed().as_millis() as u64,
                error = %err,
                "nats request errored",
            );
            SandboxNatsClientError::Nats(err.to_string())
        })?;
        tracing::info!(
            target: "wacht_sandbox_client",
            subject = %subject,
            elapsed_ms = started.elapsed().as_millis() as u64,
            response_bytes = response.payload.len(),
            "nats request returned",
        );

        let envelope: SandboxResponse<Res> = serde_json::from_slice(&response.payload)
            .map_err(|err| SandboxNatsClientError::Decode(err.to_string()))?;

        if envelope.ok {
            envelope.data.ok_or_else(|| {
                SandboxNatsClientError::Decode("daemon returned ok=true without data".into())
            })
        } else {
            Err(SandboxNatsClientError::Daemon(
                envelope
                    .error
                    .unwrap_or_else(|| "daemon returned ok=false without error".into()),
            ))
        }
    }
}

