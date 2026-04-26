# wacht-sandbox-client

NATS client and shared protocol types for the [wacht sandbox runtime](https://github.com/wacht-platform/sandbox-node).

The wacht sandbox daemon owns Kata-on-containerd microVMs as the isolation boundary for agent execution. It is controlled over NATS request/reply. This crate is what callers (agent runtimes, ops tools, tests) use to talk to it.

## What's in here

| Module      | Purpose                                                                 |
|-------------|-------------------------------------------------------------------------|
| `protocol`  | Wire types for requests, responses, and KV records the daemon publishes |
| `subjects`  | NATS subject builders (`sandbox.node.<id>.exec`, etc.)                  |
| `affinity`  | Affinity-key helpers (`thread/<dep>/<thr>`, `task/<dep>/<proj>/<key>`)  |
| `placement` | KV-tier node selection (affinity → hot mount → warm mount → least loaded)|
| `client`    | `SandboxNatsClient` — typed request/reply over NATS                     |
| `handle`    | `SandboxHandle` — bundle `(client, node_id, sandbox_id)` for an attached sandbox |

## Wire contract

Subjects (`{node_id}` is the daemon's `--node-id`):

```
sandbox.node.{node_id}.thread.create
sandbox.node.{node_id}.task.create
sandbox.node.{node_id}.exec
sandbox.node.{node_id}.exec.cancel
sandbox.node.{node_id}.delete
```

JetStream KV buckets the daemon writes:

| Bucket                       | Key shape                                      | Type                    |
|------------------------------|------------------------------------------------|-------------------------|
| `sandbox_nodes`              | `node/<node_id>`                               | `NodeRecord`            |
| `sandbox_affinity`           | `thread/<dep>/<thr>`, `task/<dep>/<proj>/<key>`, `session/<sandbox>` | `AffinityRecord` |
| `sandbox_sessions`           | `<sandbox_id>`                                 | `SessionRecord`         |
| `sandbox_deployment_mounts`  | `deployment/<dep>/<node>`                      | `DeploymentMountRecord` |
| `sandbox_exec_outputs`       | `<sandbox>/<exec>/{stdout,stderr}/<chunk>`     | raw bytes               |

## Usage

```rust
use std::time::Duration;
use wacht_sandbox_client::{
    CreateThreadSandboxRequest, ExecSandboxRequest, SandboxNatsClient,
};

let nats = async_nats::connect("nats://127.0.0.1:4222").await?;
let client = SandboxNatsClient::new(nats);

let handle = client
    .ensure_thread(&CreateThreadSandboxRequest {
        deployment_id: "123".into(),
        thread_id: "456".into(),
        project_id: Some("789".into()),
        agent_id: Some("321".into()),
        image: None,
    })
    .await?;

let response = handle
    .exec(
        ExecSandboxRequest {
            sandbox_id: String::new(),
            command: vec!["bash".into(), "-lc".into(), "echo hello".into()],
            cwd: Some("/workspace".into()),
            env: Default::default(),
            exec_id: None,
            timeout_ms: Some(60_000),
        },
        Duration::from_secs(70),
    )
    .await?;

let (stdout, stderr) = handle.read_exec_output(&response).await?;
```

## Placement

`SandboxNatsClient::place(deployment_id, affinity_key)` picks a node by:

1. **Affinity** — if the affinity key resolves to a live node and that node's deployment mount is healthy.
2. **Hot mount** — node has the deployment mounted with active sandboxes.
3. **Warm mount** — node has the deployment mounted, idle.
4. **Least loaded** — fewest concurrent execs/sessions across all live nodes.

A node is considered live if its `last_seen_ms` is within 15 s (≈3 missed heartbeats).

## Exec output

`ExecSandboxResponse` carries:
- `stdout_inline` / `stderr_inline` — first 8 KiB lossily decoded as UTF-8.
- `stdout` / `stderr` summaries — `total_bytes`, `chunk_count`, `truncated`.
- `output_handle` — bucket + prefix to walk for full bytes.

Use `SandboxHandle::read_exec_output` to materialize both streams as `Vec<u8>`. Daemon caps each stream at 8 MiB; over that, `truncated=true`.

## License

Apache-2.0.
