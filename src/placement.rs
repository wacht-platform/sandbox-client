use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::kv::Store;
use futures::StreamExt;
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::protocol::{AffinityRecord, DeploymentMountRecord, NodeRecord};
use crate::{AFFINITY_BUCKET, DEPLOYMENT_MOUNTS_BUCKET, NODES_BUCKET};

const NODE_ALIVE_MAX_AGE: Duration = Duration::from_secs(15);

#[derive(Debug, Clone)]
pub struct PlacedNode {
    pub node_id: String,
}

#[derive(Debug, Error)]
pub enum PlacementError {
    #[error("no live sandbox node available")]
    NoNodes,
    #[error("nats error: {0}")]
    Nats(String),
}

pub struct PlacementInputs<'a> {
    pub deployment_id: &'a str,
    pub affinity_key: Option<&'a str>,
}

pub async fn pick_node_for_deployment(
    jetstream: &jetstream::Context,
    inputs: PlacementInputs<'_>,
) -> Result<PlacedNode, PlacementError> {
    let nodes_store = open_bucket(jetstream, NODES_BUCKET).await?;
    let mounts_store = open_bucket(jetstream, DEPLOYMENT_MOUNTS_BUCKET).await?;
    let affinity_store = open_bucket(jetstream, AFFINITY_BUCKET).await?;

    let live_nodes = list_live_nodes(&nodes_store).await?;
    if live_nodes.is_empty() {
        return Err(PlacementError::NoNodes);
    }

    if let Some(affinity_key) = inputs.affinity_key {
        if let Some(record) = read_value::<AffinityRecord>(&affinity_store, affinity_key).await? {
            if live_nodes.iter().any(|node| node.node_id == record.node_id) {
                let mount_key = format!("deployment/{}/{}", inputs.deployment_id, record.node_id);
                let mount = read_value::<DeploymentMountRecord>(&mounts_store, &mount_key).await?;
                if mount.is_none_or(|m| m.mounted) {
                    return Ok(PlacedNode {
                        node_id: record.node_id,
                    });
                }
            }
        }
    }

    let deployment_mounts = list_deployment_mounts(&mounts_store, inputs.deployment_id).await?;
    let live_node_ids: Vec<&str> = live_nodes.iter().map(|n| n.node_id.as_str()).collect();

    if let Some(node) =
        pick_by_mount_status(&deployment_mounts, &live_nodes, &live_node_ids, "hot")
    {
        return Ok(PlacedNode { node_id: node });
    }

    if let Some(node) =
        pick_by_mount_status(&deployment_mounts, &live_nodes, &live_node_ids, "warm")
    {
        return Ok(PlacedNode { node_id: node });
    }

    let least_loaded = live_nodes
        .into_iter()
        .min_by_key(|node| (node.capacity.current_execs, node.capacity.current_sessions))
        .ok_or(PlacementError::NoNodes)?;

    Ok(PlacedNode {
        node_id: least_loaded.node_id,
    })
}

fn pick_by_mount_status(
    mounts: &[DeploymentMountRecord],
    live_nodes: &[NodeRecord],
    live_node_ids: &[&str],
    status: &str,
) -> Option<String> {
    mounts
        .iter()
        .filter(|mount| {
            mount.mounted && mount.status == status && live_node_ids.contains(&mount.node_id.as_str())
        })
        .min_by_key(|mount| {
            live_nodes
                .iter()
                .find(|node| node.node_id == mount.node_id)
                .map(|node| {
                    (
                        node.capacity.current_execs,
                        node.capacity.current_sessions,
                    )
                })
                .unwrap_or((usize::MAX, usize::MAX))
        })
        .map(|mount| mount.node_id.clone())
}

async fn list_live_nodes(store: &Store) -> Result<Vec<NodeRecord>, PlacementError> {
    let now_ms = unix_time_ms();
    let mut keys = store
        .keys()
        .await
        .map_err(|err| PlacementError::Nats(err.to_string()))?;
    let mut out = Vec::new();
    while let Some(key) = keys.next().await {
        let key = key.map_err(|err| PlacementError::Nats(err.to_string()))?;
        if let Some(node) = read_value::<NodeRecord>(store, &key).await? {
            if now_ms.saturating_sub(node.last_seen_ms) <= NODE_ALIVE_MAX_AGE.as_millis() as u64 {
                out.push(node);
            }
        }
    }
    Ok(out)
}

async fn list_deployment_mounts(
    store: &Store,
    deployment_id: &str,
) -> Result<Vec<DeploymentMountRecord>, PlacementError> {
    let prefix = format!("deployment/{deployment_id}/");
    let mut keys = store
        .keys()
        .await
        .map_err(|err| PlacementError::Nats(err.to_string()))?;
    let mut out = Vec::new();
    while let Some(key) = keys.next().await {
        let key = key.map_err(|err| PlacementError::Nats(err.to_string()))?;
        if !key.starts_with(&prefix) {
            continue;
        }
        if let Some(record) = read_value::<DeploymentMountRecord>(store, &key).await? {
            out.push(record);
        }
    }
    Ok(out)
}

async fn read_value<T>(store: &Store, key: &str) -> Result<Option<T>, PlacementError>
where
    T: DeserializeOwned,
{
    let entry = store
        .get(key)
        .await
        .map_err(|err| PlacementError::Nats(err.to_string()))?;
    let Some(bytes) = entry else {
        return Ok(None);
    };
    let value = serde_json::from_slice(&bytes)
        .map_err(|err| PlacementError::Nats(format!("decode {key}: {err}")))?;
    Ok(Some(value))
}

async fn open_bucket(jetstream: &jetstream::Context, bucket: &str) -> Result<Store, PlacementError> {
    jetstream
        .get_key_value(bucket)
        .await
        .map_err(|err| PlacementError::Nats(format!("open {bucket}: {err}")))
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
