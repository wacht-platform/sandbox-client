use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::kv::Store;
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::EXEC_OUTPUTS_BUCKET;
use crate::handle::SandboxHandle;
use crate::placement::{PlacementError, PlacementInputs, PlacedNode};
use crate::protocol::{
    CancelExecRequest, CancelExecResponse, CreateSandboxResponse, CreateTaskSandboxRequest,
    CreateThreadSandboxRequest, DeleteSandboxRequest, DeleteSandboxResponse, ExecSandboxRequest,
    ExecSandboxResponse, SandboxResponse,
};
use crate::{affinity, placement, subjects};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

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

    pub fn jetstream(&self) -> &jetstream::Context {
        &self.jetstream
    }

    pub async fn place(
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

    pub async fn create_thread(
        &self,
        node_id: &str,
        request: &CreateThreadSandboxRequest,
    ) -> Result<CreateSandboxResponse, SandboxNatsClientError> {
        self.request(&subjects::thread_create(node_id), request).await
    }

    pub async fn create_task(
        &self,
        node_id: &str,
        request: &CreateTaskSandboxRequest,
    ) -> Result<CreateSandboxResponse, SandboxNatsClientError> {
        self.request(&subjects::task_create(node_id), request).await
    }

    pub fn handle(&self, node_id: String, sandbox_id: String) -> SandboxHandle {
        SandboxHandle::new(self.clone(), node_id, sandbox_id)
    }

    pub async fn ensure_thread(
        &self,
        request: &CreateThreadSandboxRequest,
    ) -> Result<SandboxHandle, SandboxNatsClientError> {
        let affinity_key = affinity::thread_key(&request.deployment_id, &request.thread_id);
        let placed = self
            .place(&request.deployment_id, Some(&affinity_key))
            .await?;
        let response = self.create_thread(&placed.node_id, request).await?;
        Ok(self.handle(placed.node_id, response.sandbox_id))
    }

    pub async fn ensure_task(
        &self,
        request: &CreateTaskSandboxRequest,
    ) -> Result<SandboxHandle, SandboxNatsClientError> {
        let affinity_key = affinity::task_key(
            &request.deployment_id,
            &request.project_id,
            &request.task_key,
        );
        let placed = self
            .place(&request.deployment_id, Some(&affinity_key))
            .await?;
        let response = self.create_task(&placed.node_id, request).await?;
        Ok(self.handle(placed.node_id, response.sandbox_id))
    }

    pub async fn exec(
        &self,
        node_id: &str,
        request: &ExecSandboxRequest,
        request_timeout: Duration,
    ) -> Result<ExecSandboxResponse, SandboxNatsClientError> {
        self.request_with_timeout(&subjects::exec(node_id), request, request_timeout)
            .await
    }

    pub async fn cancel_exec(
        &self,
        node_id: &str,
        request: &CancelExecRequest,
    ) -> Result<CancelExecResponse, SandboxNatsClientError> {
        self.request(&subjects::exec_cancel(node_id), request).await
    }

    pub async fn delete(
        &self,
        node_id: &str,
        request: &DeleteSandboxRequest,
    ) -> Result<DeleteSandboxResponse, SandboxNatsClientError> {
        self.request(&subjects::delete(node_id), request).await
    }

    /// Read full exec output (stdout or stderr) by walking chunks in `sandbox_exec_outputs`.
    /// Returns `None` if the bucket has no entries for the given prefix (e.g. expired by TTL).
    pub async fn read_exec_stream(
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
        let response = tokio::time::timeout(
            timeout,
            self.nats.request(subject.to_string(), payload.into()),
        )
        .await
        .map_err(|_| SandboxNatsClientError::Nats(format!("timed out waiting for {subject}")))?
        .map_err(|err| SandboxNatsClientError::Nats(err.to_string()))?;

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

