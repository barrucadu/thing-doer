use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::net::Ipv4Addr;
use std::str::FromStr;

use crate::error::ResourceError;
use crate::resources::types::{GenericResource, Resource, TryFromError};

/// A resource where the spec is a node.
pub type NodeResource = GenericResource<NodeType, NodeState, NodeSpec>;

/// The type of a node.
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum NodeType {
    #[serde(rename = "node.reaper")]
    Reaper,
    #[serde(rename = "node.scheduler")]
    Scheduler,
    #[serde(rename = "node.worker")]
    Worker,
}

impl fmt::Display for NodeType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = serde_json::from_value::<String>(serde_json::json!(self)).unwrap();
        write!(f, "{s}")
    }
}

impl FromStr for NodeType {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_value::<Self>(serde_json::json!(s))
    }
}

///////////////////////////////////////////////////////////////////////////////

/// The state of a node resource.
///
/// The state stored in the resource json isn't really used - the "state" of a
/// node is determined by the state of its leases:
///
/// - alive && healthy => healthy
/// - alive && !healthy => degraded
/// - !alive => dead
///
/// Nevertheless, those are captured in a type, to avoid being stringly typed.
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NodeState {
    Healthy,
    Degraded,
    Dead,
}

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = serde_json::from_value::<String>(serde_json::json!(self)).unwrap();
        write!(f, "{s}")
    }
}

impl FromStr for NodeState {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_value::<Self>(serde_json::json!(s))
    }
}

///////////////////////////////////////////////////////////////////////////////

/// A node resource specification.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct NodeSpec {
    /// Cluster-reachable IP address.  Only set for worker nodes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<Ipv4Addr>,

    /// Resource limits.  Only set for worker nodes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<NodeLimitSpec>,
}

/// The maximum resources available to pods on this node.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeLimitSpec {
    /// An amount of CPU.
    pub cpu: Decimal,

    /// An amount of memory, in MiB.
    pub memory: u64,
}

impl TryFrom<Resource> for NodeResource {
    type Error = TryFromError;

    fn try_from(resource: Resource) -> Result<Self, Self::Error> {
        let GenericResource {
            name,
            rtype,
            state,
            metadata,
            spec,
        } = resource;

        let node_type = NodeType::from_str(&rtype)?;
        let node_state = if let Some(s) = state {
            NodeState::from_str(&s)?
        } else {
            return Err(ResourceError::BadState.into());
        };

        let value = serde_json::to_value(spec).unwrap();
        let node_spec = serde_json::from_value::<NodeSpec>(value)?;

        Ok(GenericResource {
            name,
            rtype: node_type,
            state: Some(node_state),
            metadata,
            spec: node_spec,
        })
    }
}

impl TryFrom<Value> for NodeResource {
    type Error = TryFromError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Self::try_from(Resource::try_from(value)?)
    }
}

impl TryFrom<Vec<u8>> for NodeResource {
    type Error = TryFromError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(Resource::try_from(bytes)?)
    }
}
