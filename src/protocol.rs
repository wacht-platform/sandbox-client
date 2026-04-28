use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateThreadSandboxRequest {
    pub deployment_id: String,
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTaskSandboxRequest {
    pub deployment_id: String,
    pub project_id: String,
    pub task_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecSandboxRequest {
    pub sandbox_id: String,
    pub command: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exec_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelExecRequest {
    pub sandbox_id: String,
    pub exec_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteSandboxRequest {
    pub sandbox_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(serialize = "T: Serialize", deserialize = "T: serde::de::DeserializeOwned"))]
pub struct SandboxResponse<T> {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl<T> SandboxResponse<T> {
    pub fn ok(data: T) -> Self {
        Self {
            ok: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn error(error: impl Into<String>) -> Self {
        Self {
            ok: false,
            data: None,
            error: Some(error.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSandboxResponse {
    pub sandbox_id: String,
    pub scope: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecOutputHandle {
    pub bucket: String,
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecStreamSummary {
    pub total_bytes: u64,
    pub truncated: bool,
    pub in_object_store: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecSandboxResponse {
    pub sandbox_id: String,
    pub exec_id: String,
    pub exit_code: i32,
    pub timed_out: bool,
    pub cancelled: bool,
    #[serde(with = "serde_bytes")]
    pub stdout_inline: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub stderr_inline: Vec<u8>,
    pub stdout: ExecStreamSummary,
    pub stderr: ExecStreamSummary,
    pub output_handle: ExecOutputHandle,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelExecResponse {
    pub sandbox_id: String,
    pub exec_id: String,
    pub cancelled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteSandboxResponse {
    pub sandbox_id: String,
}

// ---------- KV record schemas (written by the daemon, read by clients) ----------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRecord {
    pub node_id: String,
    pub started_at_ms: u64,
    pub last_seen_ms: u64,
    pub nats_subject_prefix: String,
    pub capacity: NodeCapacity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapacity {
    pub max_parallel_execs: usize,
    pub current_execs: usize,
    pub current_sessions: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AffinityRecord {
    pub node_id: String,
    pub sandbox_id: String,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecord {
    pub sandbox_id: String,
    pub scope: String,
    pub node_id: String,
    pub deployment_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    pub last_activity_ms: u64,
    pub status: String,
}

