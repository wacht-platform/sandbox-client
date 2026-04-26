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
    pub fn new(client: SandboxNatsClient, node_id: String, sandbox_id: String) -> Self {
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
        self.client
            .exec(&self.node_id, &request, request_timeout)
            .await
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
        let stdout = self
            .client
            .read_exec_stream(&response.output_handle.prefix, "stdout", response.stdout.chunk_count)
            .await?;
        let stderr = self
            .client
            .read_exec_stream(&response.output_handle.prefix, "stderr", response.stderr.chunk_count)
            .await?;
        Ok((stdout, stderr))
    }
}
