use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;

use nodelib::error::*;
use nodelib::etcd;
use nodelib::etcd::pb::mvccpb::{event::EventType, Event};
use nodelib::etcd::prefix;
use nodelib::etcd::watcher;
use nodelib::resources::node::{NodeResource, NodeType};
use nodelib::resources::pod::PodResource;

use crate::scheduler::UnscheduledPod;
use crate::state::SharedNodeState;

/// Scan etcd for the current state of all worker nodes and unscheduled pods,
/// and establish watches on the `node_available_cpu`, `node_available_memory`,
/// `node_heartbeat_healthy`, `resource` (worker node), and `unscheduled_pods`
/// prefixes to keep the state up to date as things change.
pub async fn initialise(
    etcd_config: etcd::Config,
    node_state: SharedNodeState,
    new_pod_tx: mpsc::UnboundedSender<UnscheduledPod>,
) -> Result<(), Error> {
    watcher::setup_watcher(
        &etcd_config,
        Arc::new(RwLock::new(WatchState {
            etcd_config: etcd_config.clone(),
            node_state,
            new_pod_tx,
        })),
        vec![
            prefix::node_available_cpu(&etcd_config, NodeType::Worker),
            prefix::node_available_memory(&etcd_config, NodeType::Worker),
            prefix::node_heartbeat_healthy(&etcd_config, NodeType::Worker),
            prefix::resource(&etcd_config, &NodeType::Worker.to_string()),
            prefix::unscheduled_pods(&etcd_config),
        ],
    )
    .await?;

    Ok(())
}

struct WatchState {
    etcd_config: etcd::Config,
    node_state: SharedNodeState,
    new_pod_tx: mpsc::UnboundedSender<UnscheduledPod>,
}

impl watcher::Watcher for WatchState {
    async fn apply_event(&mut self, event: Event) {
        let node_available_cpu_prefix =
            prefix::node_available_cpu(&self.etcd_config, NodeType::Worker);
        let node_available_memory_prefix =
            prefix::node_available_memory(&self.etcd_config, NodeType::Worker);
        let node_heartbeat_healthy_prefix =
            prefix::node_heartbeat_healthy(&self.etcd_config, NodeType::Worker);
        let resource_prefix = prefix::resource(&self.etcd_config, &NodeType::Worker.to_string());
        let unscheduled_pods_prefix = prefix::unscheduled_pods(&self.etcd_config);

        let is_create = event.r#type() == EventType::Put;
        let kv = event.kv.unwrap();
        let key = String::from_utf8(kv.key).unwrap();

        if let Some((_, node_name)) = key.split_once(&node_available_cpu_prefix) {
            if is_create {
                if let Some(cpu) = decimal_from_bytes(kv.value) {
                    tracing::info!(node_name, ?cpu, "node available cpu changed");
                    self.node_state
                        .set_node_available_cpu(node_name, Some(cpu))
                        .await;
                } else {
                    tracing::warn!(node_name, "could not parse node available cpu definition");
                }
            } else {
                self.node_state
                    .set_node_available_cpu(node_name, None)
                    .await;
            }
        } else if let Some((_, node_name)) = key.split_once(&node_available_memory_prefix) {
            if is_create {
                if let Some(memory) = u64_from_bytes(kv.value) {
                    tracing::info!(node_name, ?memory, "node available memory changed");
                    self.node_state
                        .set_node_available_memory(node_name, Some(memory))
                        .await;
                } else {
                    tracing::warn!(
                        node_name,
                        "could not parse node available memory definition"
                    );
                }
            } else {
                self.node_state
                    .set_node_available_memory(node_name, None)
                    .await;
            }
        } else if let Some((_, node_name)) = key.split_once(&node_heartbeat_healthy_prefix) {
            tracing::info!(
                node_name,
                is_healthy = is_create,
                "node healthy check changed"
            );
            self.node_state
                .set_node_is_healthy(node_name, is_create)
                .await;
        } else if let Some((_, node_name)) = key.split_once(&resource_prefix) {
            match NodeResource::try_from(kv.value) {
                Ok(resource) => {
                    tracing::info!(node_name, "found new worker node");
                    self.node_state.discover_worker(resource).await;
                }
                Err(error) => {
                    tracing::warn!(node_name, ?error, "could not parse worker node definition");
                }
            }
        } else if let Some((_, pod_name)) = key.split_once(&unscheduled_pods_prefix) {
            if is_create {
                match PodResource::try_from(kv.value) {
                    Ok(resource) => {
                        tracing::info!(pod_name, "found new pod");
                        self.new_pod_tx
                            .send(UnscheduledPod::new(resource, kv.mod_revision))
                            .expect("could not send to unbounded channel");
                    }
                    Err(error) => {
                        tracing::warn!(?key, ?error, "could not parse pod definition");
                    }
                }
            }
        } else {
            tracing::warn!(?key, "unexpected watch key");
        }
    }
}

/// Parse a value as a `Decimal`
fn decimal_from_bytes(bytes: Vec<u8>) -> Option<Decimal> {
    let s = String::from_utf8(bytes).ok()?;
    Decimal::from_str_exact(&s).ok()
}

/// Parse a value as a `u64`
fn u64_from_bytes(bytes: Vec<u8>) -> Option<u64> {
    let s = String::from_utf8(bytes).ok()?;
    s.parse().ok()
}
