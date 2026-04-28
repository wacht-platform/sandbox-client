use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::kv::Store;
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::handle::SandboxHandle;
use crate::placement::{self, NODE_ALIVE_MAX_AGE, PlacementError, PlacementInputs, PlacedNode};
use crate::protocol::{
    CancelExecRequest, CancelExecResponse, CreateSandboxResponse, CreateTaskSandboxRequest,
    CreateThreadSandboxRequest, DeleteSandboxRequest, DeleteSandboxResponse, ExecSandboxRequest,
    ExecSandboxResponse, NodeRecord, SandboxResponse, SessionRecord,
};
use crate::{EXEC_OUTPUTS_BUCKET, NODES_BUCKET, SESSIONS_BUCKET, affinity, subjects};

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
}

impl SandboxNatsClient {
    pub fn new(nats: async_nats::Client) -> Self {
        let jetstream = jetstream::new(nats.clone());
        Self {
            nats,
            jetstream,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub async fn thread(
        &self,
        request: &CreateThreadSandboxRequest,
    ) -> Result<SandboxHandle, SandboxNatsClientError> {
        let sandbox_id = format!("thread-{}-{}", request.deployment_id, request.thread_id);
        if let Some(handle) = self.try_attach(&request.deployment_id, &sandbox_id).await? {
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
            .place_with_retry(&request.deployment_id, Some(&affinity_key))
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
        if let Some(handle) = self.try_attach(&request.deployment_id, &sandbox_id).await? {
            return Ok(handle);
        }
        let affinity_key = affinity::task_key(
            &request.deployment_id,
            &request.project_id,
            &request.task_key,
        );
        let placed = self
            .place_with_retry(&request.deployment_id, Some(&affinity_key))
            .await?;
        let response = self.create_task(&placed.node_id, request).await?;
        Ok(self.handle(placed.node_id, response.sandbox_id))
    }

    async fn try_attach(
        &self,
        _deployment_id: &str,
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
        let store = self
            .jetstream
            .get_key_value(SESSIONS_BUCKET)
            .await
            .map_err(|err| SandboxNatsClientError::Nats(err.to_string()))?;
        let _ = store.delete(sandbox_id).await;
        Ok(())
    }

    async fn read_session(
        &self,
        sandbox_id: &str,
    ) -> Result<Option<SessionRecord>, SandboxNatsClientError> {
        let store = self
            .jetstream
            .get_key_value(SESSIONS_BUCKET)
            .await
            .map_err(|err| SandboxNatsClientError::Nats(err.to_string()))?;
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
        let store = self
            .jetstream
            .get_key_value(NODES_BUCKET)
            .await
            .map_err(|err| SandboxNatsClientError::Nats(err.to_string()))?;
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
        deployment_id: &str,
        affinity_key: Option<&str>,
    ) -> Result<PlacedNode, SandboxNatsClientError> {
        Ok(placement::pick_node_for_deployment(
            &self.jetstream,
            PlacementInputs {
                deployment_id,
                affinity_key,
            },
        )
        .await?)
    }

    pub(crate) async fn place_with_retry(
        &self,
        deployment_id: &str,
        affinity_key: Option<&str>,
    ) -> Result<PlacedNode, SandboxNatsClientError> {
        const ATTEMPTS: u32 = 6;
        let mut delay = std::time::Duration::from_millis(500);
        let mut last_err: Option<SandboxNatsClientError> = None;
        for attempt in 0..ATTEMPTS {
            match self.place(deployment_id, affinity_key).await {
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

    pub(crate) async fn read_exec_stream(
        &self,
        prefix: &str,
        stream: &str,
        chunk_count: usize,
    ) -> Result<Vec<u8>, SandboxNatsClientError> {
        if chunk_count == 0 {
            return Ok(Vec::new());
        }
        let store = self
            .jetstream
            .get_key_value(EXEC_OUTPUTS_BUCKET)
            .await
            .map_err(|err| SandboxNatsClientError::Nats(err.to_string()))?;
        let mut out = Vec::new();
        for idx in 0..chunk_count {
            let key = format!("{prefix}/{stream}/{idx}");
            let bytes = read_chunk(&store, &key).await?;
            out.extend_from_slice(&bytes);
        }
        Ok(out)
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

async fn read_chunk(store: &Store, key: &str) -> Result<Vec<u8>, SandboxNatsClientError> {
    store
        .get(key)
        .await
        .map_err(|err| SandboxNatsClientError::Nats(format!("read {key}: {err}")))?
        .map(|bytes| bytes.to_vec())
        .ok_or_else(|| {
            SandboxNatsClientError::Nats(format!("missing exec output chunk {key} (expired?)"))
        })
}
