use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use nodelib::resources::node::{NodeResource, NodeType};

/// A handle to the shared mutable state of all nodes.
#[derive(Clone)]
pub struct SharedNodeState(Arc<RwLock<InnerState>>);

/// A snapshot of the state of a single node.
#[derive(Debug)]
pub struct NodeState {
    pub resource: NodeResource,
    pub is_healthy: Option<bool>,
    pub available_cpu: Option<Decimal>,
    pub available_memory: Option<u64>,
}

impl Default for SharedNodeState {
    fn default() -> Self {
        SharedNodeState(Arc::new(RwLock::new(InnerState::default())))
    }
}

impl SharedNodeState {
    /// Get all known worker nodes and their latest state.
    pub async fn get_healthy_workers(&self) -> HashMap<String, NodeState> {
        let inner = self.0.read().await;
        inner.get_workers(|node| node.is_healthy.unwrap_or(false))
    }

    /// Set or remove the available CPU for a node.
    pub async fn set_node_available_cpu(&self, name: &str, val: Option<Decimal>) {
        let mut inner = self.0.write().await;
        if let Some(new) = val {
            inner.node_available_cpu.insert(name.to_owned(), new);
        } else {
            inner.node_available_cpu.remove(name);
        }
    }

    /// Set or remove the available memory for a node.
    pub async fn set_node_available_memory(&self, name: &str, val: Option<u64>) {
        let mut inner = self.0.write().await;
        if let Some(new) = val {
            inner.node_available_memory.insert(name.to_owned(), new);
        } else {
            inner.node_available_memory.remove(name);
        }
    }

    /// Set the healthcheck status of a node.
    pub async fn set_node_is_healthy(&self, name: &str, new: bool) {
        let mut inner = self.0.write().await;
        inner.node_is_healthy.insert(name.to_owned(), new);
    }

    /// Add a new worker to the known set.
    pub async fn discover_worker(&self, new: NodeResource) {
        assert!(new.rtype == NodeType::Worker, "not a worker");

        let mut inner = self.0.write().await;
        inner.worker_nodes.insert(new.name.clone(), new);
    }
}

///////////////////////////////////////////////////////////////////////////////

/// The internal state, not visible outside this module.
#[derive(Default)]
struct InnerState {
    worker_nodes: HashMap<String, NodeResource>,
    node_is_healthy: HashMap<String, bool>,
    node_available_cpu: HashMap<String, Decimal>,
    node_available_memory: HashMap<String, u64>,
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
