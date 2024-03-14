use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;

use nodelib::error::*;
use nodelib::etcd;
use nodelib::etcd::pb::mvccpb::{event::EventType, Event};
use nodelib::etcd::prefix;
use nodelib::etcd::watcher;
use nodelib::resources::node::NodeType;

use crate::types::*;

/// Establish watches on the `claimed_pods` and `node_heartbeat_alive` prefixes
/// to detect newly-dead nodes and mark their pods as dead.
pub async fn initialise(
    etcd_config: etcd::Config,
    reap_node_tx: mpsc::UnboundedSender<NodeName>,
    reap_pod_tx: mpsc::UnboundedSender<PodName>,
) -> Result<(), Error> {
    let state = Arc::new(RwLock::new(WatchState {
        etcd_config: etcd_config.clone(),
        reap_node_tx,
        reap_pod_tx,
    }));

    watcher::setup_watcher(
        &etcd_config,
        state,
        vec![
            prefix::claimed_pods(&etcd_config),
            prefix::node_heartbeat_alive(&etcd_config, NodeType::Worker),
        ],
    )
    .await?;
    Ok(())
}

#[derive(Debug)]
struct WatchState {
    pub etcd_config: etcd::Config,
    pub reap_node_tx: mpsc::UnboundedSender<NodeName>,
    pub reap_pod_tx: mpsc::UnboundedSender<PodName>,
}

impl watcher::Watcher for WatchState {
    async fn apply_event(&mut self, event: Event) {
        if event.r#type() != EventType::Delete {
            return;
        }

        let pod_prefix = prefix::claimed_pods(&self.etcd_config);
        let node_prefix = prefix::node_heartbeat_alive(&self.etcd_config, NodeType::Worker);
        let kv = event.kv.unwrap();
        let key = String::from_utf8(kv.key).unwrap();

        if let Some((_, node_name)) = key.split_once(&node_prefix) {
            tracing::info!(node_name, "found reapable node");
            self.reap_node_tx
                .send(NodeName::from(node_name))
                .expect("could not send to unbounded channel");
        } else if let Some((_, pod_name)) = key.split_once(&pod_prefix) {
            tracing::info!(pod_name, "found reapable pod");
            self.reap_pod_tx
                .send(PodName::from(pod_name))
                .expect("could not send to unbounded channel");
        } else {
            tracing::warn!(?key, "unexpected watch key");
        }
    }
}
