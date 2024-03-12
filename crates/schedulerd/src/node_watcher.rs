use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use nodelib::error::*;
use nodelib::etcd;
use nodelib::etcd::pb::mvccpb::{event::EventType, Event};
use nodelib::etcd::prefix;
use nodelib::etcd::watcher;
use nodelib::resources::node::*;

/// A handle to the shared state.
#[derive(Debug, Clone)]
pub struct State(Arc<RwLock<InnerState>>);

/// Fetch the current state of the cluster from etcd, and establish watches to
/// keep the state up to date.
///
/// If a connection cannot be established to any of the configured etcd hosts,
/// this raises an error and terminates the process.
pub async fn initialise(etcd_config: etcd::Config) -> Result<State, Error> {
    let inner = Arc::new(RwLock::new(InnerState {
        etcd_config: etcd_config.clone(),
        worker_nodes: HashMap::new(),
        node_is_healthy: HashMap::new(),
        node_available_cpu: HashMap::new(),
        node_available_memory: HashMap::new(),
    }));

    let prefixes = &[
        prefix::node_heartbeat_healthy(&etcd_config, NodeType::Worker),
        prefix::node_available_cpu(&etcd_config, NodeType::Worker),
        prefix::node_available_memory(&etcd_config, NodeType::Worker),
        prefix::resource(&etcd_config, &NodeType::Worker.to_string()),
    ];

    for prefix in prefixes {
        watcher::setup_watcher(&etcd_config, inner.clone(), prefix.clone()).await?;
    }

    Ok(State(inner))
}

impl State {
    /// Get all known worker nodes and their latest state.
    pub async fn get_healthy_workers(&self) -> HashMap<String, NodeState> {
        let inner = self.0.read().await;
        inner.get_workers(|node| node.is_healthy.unwrap_or(false))
    }
}

/// The internal state, only visible to the update task and query methods.
#[derive(Debug)]
struct InnerState {
    pub etcd_config: etcd::Config,
    pub worker_nodes: HashMap<String, NodeResource>,
    pub node_is_healthy: HashMap<String, bool>,
    pub node_available_cpu: HashMap<String, Decimal>,
    pub node_available_memory: HashMap<String, u64>,
}

impl InnerState {
    /// Get the worker nodes matching a predicate.
    fn get_workers<P>(&self, mut predicate: P) -> HashMap<String, NodeState>
    where
        P: FnMut(&NodeState) -> bool,
    {
        self.worker_nodes
            .iter()
            .map(|(name, res)| (name.to_owned(), self.get_node_state(name, res.clone())))
            .filter(|(_, v)| predicate(v))
            .collect()
    }

    /// Get the state of a single node.
    fn get_node_state(&self, name: &str, resource: NodeResource) -> NodeState {
        NodeState {
            resource,
            is_healthy: self.node_is_healthy.get(name).copied(),
            available_cpu: self.node_available_cpu.get(name).copied(),
            available_memory: self.node_available_memory.get(name).copied(),
        }
    }
}

impl watcher::Watcher for InnerState {
    async fn apply_event(&mut self, event: Event) {
        let healthcheck_prefix =
            prefix::node_heartbeat_healthy(&self.etcd_config, NodeType::Worker);
        let available_cpu_prefix = prefix::node_available_cpu(&self.etcd_config, NodeType::Worker);
        let available_memory_prefix =
            prefix::node_available_memory(&self.etcd_config, NodeType::Worker);
        let resource_prefix = prefix::resource(&self.etcd_config, &NodeType::Worker.to_string());

        let is_create = event.r#type() == EventType::Put;
        let kv = event.kv.unwrap();
        let key = String::from_utf8(kv.key).unwrap();

        if let Some((_, name)) = key.split_once(&healthcheck_prefix) {
            tracing::info!(name, is_healthy = is_create, "node healthy check changed");
            self.node_is_healthy.insert(name.to_owned(), is_create);
        } else if let Some((_, name)) = key.split_once(&available_cpu_prefix) {
            if is_create {
                if let Some(cpu) = decimal_from_bytes(kv.value) {
                    tracing::info!(name, ?cpu, "node available cpu changed");
                    self.node_available_cpu.insert(name.to_owned(), cpu);
                } else {
                    tracing::warn!(name, "could not parse node available cpu definition");
                }
            } else {
                self.node_available_cpu.remove(name);
            }
        } else if let Some((_, name)) = key.split_once(&available_memory_prefix) {
            if is_create {
                if let Some(memory) = u64_from_bytes(kv.value) {
                    tracing::info!(name, ?memory, "node available memory changed");
                    self.node_available_memory.insert(name.to_owned(), memory);
                } else {
                    tracing::warn!(name, "could not parse node available memory definition");
                }
            } else {
                self.node_available_memory.remove(name);
            }
        } else if let Some((_, name)) = key.split_once(&resource_prefix) {
            match NodeResource::try_from(kv.value) {
                Ok(resource) => {
                    tracing::info!(name, "found new worker node");
                    self.worker_nodes.insert(name.to_owned(), resource);
                }
                Err(error) => {
                    tracing::warn!(name, ?error, "could not parse worker node definition");
                }
            }
        } else {
            tracing::warn!(?key, "unexpected watch key");
        }
    }
}

/// The state of a single node.
#[derive(Debug)]
pub struct NodeState {
    pub resource: NodeResource,
    pub is_healthy: Option<bool>,
    pub available_cpu: Option<Decimal>,
    pub available_memory: Option<u64>,
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
