pub mod affinity;
pub mod client;
pub mod handle;
pub mod protocol;

pub(crate) mod placement;
pub(crate) mod subjects;

pub use client::{SandboxNatsClient, SandboxNatsClientError};
pub use handle::SandboxHandle;
pub use protocol::{
    AffinityRecord, CancelExecRequest, CancelExecResponse, CreateSandboxResponse,
    CreateTaskSandboxRequest, CreateThreadSandboxRequest, DeleteSandboxRequest,
    DeleteSandboxResponse, ExecOutputHandle, ExecSandboxRequest, ExecSandboxResponse,
    ExecStreamSummary, NodeCapacity, NodeRecord, SandboxResponse, SessionRecord,
};

pub const NODES_BUCKET: &str = "sandbox_nodes";
pub const AFFINITY_BUCKET: &str = "sandbox_affinity";
pub const SESSIONS_BUCKET: &str = "sandbox_sessions";
pub const EXEC_OUTPUTS_BUCKET: &str = "sandbox_exec_outputs";
