use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

use nodelib::etcd;
use nodelib::etcd::pb::mvccpb::{event::EventType, Event};
use nodelib::etcd::watcher;
use nodelib::Error;

use crate::util;

/// A handle to the shared state.
#[derive(Debug, Clone)]
pub struct State(Arc<RwLock<InnerState>>);

/// Fetch the current state of the cluster from etcd, and establish watches to
/// keep the state up to date.
///
/// If a connection cannot be established to any of the configured etcd hosts,
/// this raises an error and terminates the process.
pub async fn initialise(etcd_config: etcd::Config) -> Result<State, Error> {
    let inner = Arc::new(RwLock::new(InnerState::new(etcd_config.prefix.clone())));

    let prefixes = &[
        format!(
            "{prefix}/node/heartbeat/healthy/",
            prefix = etcd_config.prefix
        ),
        format!(
            "{prefix}/node/heartbeat/alive/",
            prefix = etcd_config.prefix
        ),
        format!(
            "{prefix}/resource/node.worker/",
            prefix = etcd_config.prefix
        ),
    ];

    for prefix in prefixes {
        watcher::setup_watcher(&etcd_config, inner.clone(), prefix.clone()).await?;
    }

    Ok(State(inner))
}

impl State {
    /// Get all known worker nodes and their latest state.
    pub async fn get_worker_nodes(&self) -> HashMap<String, NodeState> {
        let inner = self.0.read().await;
        inner.get_nodes(&inner.worker_nodes)
    }
}

/// The internal state, only visible to the update task and query methods.
#[derive(Debug)]
struct InnerState {
    pub etcd_prefix: String,
    pub worker_nodes: HashMap<String, SocketAddr>,
    pub node_is_healthy: HashMap<String, bool>,
    pub node_is_alive: HashMap<String, bool>,
}

impl InnerState {
    /// Create a new InnerState.
    fn new(etcd_prefix: String) -> Self {
        Self {
            etcd_prefix,
            worker_nodes: HashMap::new(),
            node_is_healthy: HashMap::new(),
            node_is_alive: HashMap::new(),
        }
    }

    /// Get the states of a collection of nodes.
    fn get_nodes(&self, nodes: &HashMap<String, SocketAddr>) -> HashMap<String, NodeState> {
        nodes
            .iter()
            .map(|(name, address)| (name.to_owned(), self.get_node_state(name, *address)))
            .collect()
    }

    /// Get the state of a single node.
    fn get_node_state(&self, name: &str, address: SocketAddr) -> NodeState {
        NodeState {
            address,
            is_healthy: self.node_is_healthy.get(name).copied(),
            is_alive: self.node_is_alive.get(name).copied(),
        }
    }
}

impl watcher::Watcher for InnerState {
    fn apply_event(&mut self, event: Event) {
        let etcd_prefix = &self.etcd_prefix;
        let is_create = event.r#type() == EventType::Put;
        let kv = event.kv.unwrap();
        let key = String::from_utf8(kv.key).unwrap();

        if let Some((_, name)) = key.split_once(&format!("{etcd_prefix}/node/heartbeat/healthy/")) {
            tracing::info!(name, is_healthy = is_create, "node healthy check changed");
            self.node_is_healthy.insert(name.to_owned(), is_create);
        } else if let Some((_, name)) =
            key.split_once(&format!("{etcd_prefix}/node/heartbeat/alive/"))
        {
            tracing::info!(name, is_alive = is_create, "node alive check changed");
            self.node_is_alive.insert(name.to_owned(), is_create);
        } else if let Some((_, name)) =
            key.split_once(&format!("{etcd_prefix}/resource/node.worker/"))
        {
            if let Some(address) = address_from_node_json(kv.value) {
                tracing::info!(name, ?address, "found worker node");
                self.worker_nodes.insert(name.to_owned(), address);
            } else {
                tracing::warn!(name, "could not parse worker node definition");
            }
        } else {
            tracing::warn!(?key, "unexpected watch key");
        }
    }
}

/// The state of a single node.
#[derive(Debug)]
pub struct NodeState {
    pub address: SocketAddr,
    pub is_healthy: Option<bool>,
    pub is_alive: Option<bool>,
}

/// Parse a value as node JSON and extract the address field.
fn address_from_node_json(bytes: Vec<u8>) -> Option<SocketAddr> {
    if let Some(value) = util::bytes_to_json(bytes) {
        if let Some(address_str) = value["spec"]["address"].as_str() {
            if let Ok(address) = address_str.parse() {
                return Some(address);
            }
        }
    }

    None
}
