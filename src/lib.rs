pub mod affinity;
pub mod client;
pub mod handle;
pub mod placement;
pub mod protocol;
pub mod subjects;

pub use client::{SandboxNatsClient, SandboxNatsClientError};
pub use handle::SandboxHandle;
pub use placement::{PlacementError, PlacementTier, PlacedNode, pick_node_for_deployment};
pub use protocol::{
    AffinityRecord, CancelExecRequest, CancelExecResponse, CreateSandboxResponse,
    CreateTaskSandboxRequest, CreateThreadSandboxRequest, DeleteSandboxRequest,
    DeleteSandboxResponse, DeploymentMountRecord, ExecOutputHandle, ExecSandboxRequest,
    ExecSandboxResponse, ExecStreamSummary, NodeCapacity, NodeRecord, SandboxResponse,
    SessionRecord,
};

pub const NODES_BUCKET: &str = "sandbox_nodes";
pub const AFFINITY_BUCKET: &str = "sandbox_affinity";
pub const SESSIONS_BUCKET: &str = "sandbox_sessions";
pub const DEPLOYMENT_MOUNTS_BUCKET: &str = "sandbox_deployment_mounts";
pub const EXEC_OUTPUTS_BUCKET: &str = "sandbox_exec_outputs";
