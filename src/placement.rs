use std::time::Duration;

use async_nats::jetstream::kv::Store;
use futures::StreamExt;
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::protocol::{AffinityRecord, NodeRecord};

pub(crate) const NODE_ALIVE_MAX_AGE: Duration = Duration::from_secs(15);

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

pub fn pick_from_live_nodes(
    live_nodes: Vec<NodeRecord>,
    affinity_record: Option<AffinityRecord>,
) -> Result<PlacedNode, PlacementError> {
    if live_nodes.is_empty() {
        return Err(PlacementError::NoNodes);
    }

    if let Some(record) = affinity_record {
        if live_nodes.iter().any(|node| node.node_id == record.node_id) {
            return Ok(PlacedNode {
                node_id: record.node_id,
            });
        }
    }

    let least_loaded = live_nodes
        .into_iter()
        .min_by_key(|node| (node.capacity.current_execs, node.capacity.current_sessions))
        .ok_or(PlacementError::NoNodes)?;

    Ok(PlacedNode {
        node_id: least_loaded.node_id,
    })
}

pub async fn read_affinity_record(
    affinity_store: &Store,
    affinity_key: &str,
) -> Result<Option<AffinityRecord>, PlacementError> {
    read_value::<AffinityRecord>(affinity_store, affinity_key).await
}

pub async fn list_live_nodes(store: &Store) -> Result<Vec<NodeRecord>, PlacementError> {
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

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
