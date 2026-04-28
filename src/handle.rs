use std::time::Duration;

use crate::client::{SandboxNatsClient, SandboxNatsClientError};
use crate::protocol::{
    CancelExecRequest, DeleteSandboxResponse, ExecSandboxRequest, ExecSandboxResponse,
};

#[derive(Clone)]
pub struct SandboxHandle {
    client: SandboxNatsClient,
    node_id: String,
    sandbox_id: String,
}

impl SandboxHandle {
    pub(crate) fn new(client: SandboxNatsClient, node_id: String, sandbox_id: String) -> Self {
        Self {
            client,
            node_id,
            sandbox_id,
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn sandbox_id(&self) -> &str {
        &self.sandbox_id
    }

    pub async fn exec(
        &self,
        mut request: ExecSandboxRequest,
        request_timeout: Duration,
    ) -> Result<ExecSandboxResponse, SandboxNatsClientError> {
        request.sandbox_id = self.sandbox_id.clone();
        let result = self
            .client
            .exec(&self.node_id, &request, request_timeout)
            .await;
        if let Err(SandboxNatsClientError::Daemon(msg)) = &result {
            if msg.contains("sandbox not found on this node") {
                let _ = self.client.forget_session(&self.sandbox_id).await;
            }
        }
        result
    }

    pub async fn cancel(&self, exec_id: &str) -> Result<bool, SandboxNatsClientError> {
        let response = self
            .client
            .cancel_exec(
                &self.node_id,
                &CancelExecRequest {
                    sandbox_id: self.sandbox_id.clone(),
                    exec_id: exec_id.to_string(),
                },
            )
            .await?;
        Ok(response.cancelled)
    }

    pub async fn read_file(&self, path: &str) -> Result<Vec<u8>, SandboxNatsClientError> {
        self.client
            .fs_read(&self.node_id, &self.sandbox_id, path)
            .await
    }

    pub async fn write_file(
        &self,
        path: &str,
        content: &[u8],
    ) -> Result<(), SandboxNatsClientError> {
        let _ = self
            .client
            .fs_write(&self.node_id, &self.sandbox_id, path, content)
            .await?;
        Ok(())
    }

    pub async fn delete(&self) -> Result<DeleteSandboxResponse, SandboxNatsClientError> {
        self.client
            .delete(
                &self.node_id,
                &crate::protocol::DeleteSandboxRequest {
                    sandbox_id: self.sandbox_id.clone(),
                },
            )
            .await
    }

    pub async fn read_exec_output(
        &self,
        response: &ExecSandboxResponse,
    ) -> Result<(Vec<u8>, Vec<u8>), SandboxNatsClientError> {
        let stdout = if response.stdout.in_object_store {
            self.client
                .read_exec_object(&response.output_handle.prefix, "stdout")
                .await?
        } else {
            response.stdout_inline.clone()
        };
        let stderr = if response.stderr.in_object_store {
            self.client
                .read_exec_object(&response.output_handle.prefix, "stderr")
                .await?
        } else {
            response.stderr_inline.clone()
        };
        Ok((stdout, stderr))
    }
}
