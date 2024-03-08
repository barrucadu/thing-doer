use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use crate::error::ResourceError;
use crate::resources::types::{GenericResource, Resource, TryFromError};

/// A resource where the spec is a pod.
pub type PodResource = GenericResource<PodType, PodState, PodSpec>;

/// There's only one pod resource type.
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum PodType {
    #[serde(rename = "pod")]
    Pod,
}

impl fmt::Display for PodType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = serde_json::from_value::<String>(serde_json::json!(self)).unwrap();
        write!(f, "{s}")
    }
}

impl FromStr for PodType {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_value::<Self>(serde_json::json!(s))
    }
}

///////////////////////////////////////////////////////////////////////////////

/// The state of a pod resource.
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PodState {
    /// Initial state
    Created,
    /// Assigned to a worker
    Scheduled,
    /// Could not be assigned to a worker within the retry limit
    Abandoned,
    /// Picked up by a worker but not yet started
    Accepted,
    /// Running, not yet terminated
    Running,
    /// Exited with a successful exit code
    ExitSuccess,
    /// Exited with an unsuccessful exit code
    ExitFailure,
    /// Failed to start
    Errored,
    /// Presumed dead, due to worker failure
    Dead,
}

impl PodState {
    /// True if this is a terminal state.
    pub fn is_terminal(&self) -> bool {
        match self {
            Self::Abandoned
            | Self::ExitSuccess
            | Self::ExitFailure
            | Self::Errored
            | Self::Dead => true,
            Self::Created | Self::Scheduled | Self::Accepted | Self::Running => false,
        }
    }
}

impl fmt::Display for PodState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = serde_json::from_value::<String>(serde_json::json!(self)).unwrap();
        write!(f, "{s}")
    }
}

impl FromStr for PodState {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_value::<Self>(serde_json::json!(s))
    }
}

///////////////////////////////////////////////////////////////////////////////

/// A pod resource specification.
///
/// TODO: Validations:
///
/// - Each container's name is unique.
///
/// - Each container's resource limit is >= its resource request (if given).
///
/// - Each port mapping uses a unique cluster port.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PodSpec {
    #[serde(default)]
    pub containers: Vec<PodContainerSpec>,
}

/// The specification of a container inside a pod.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodContainerSpec {
    /// Name - must be unique inside this pod.
    pub name: String,

    /// Container image to run.
    pub image: String,

    /// Override the default entrypoint set by the image.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<String>,

    /// Command-line argument to pass to the container image's entrypoint.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cmd: Vec<String>,

    /// Environment variables.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub env: HashMap<String, String>,

    /// Ports to expose.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<ContainerPortSpec>,

    /// Resources to use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ContainerResourceSpec>,
}

/// A port to expose.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged, rename_all = "camelCase")]
pub enum ContainerPortSpec {
    /// Semantically equivalent to `Map` with just `container` specified - this
    /// is to allow terser spec JSON in that case.
    Expose(u16),
    Map {
        /// The container port to expose to the cluster.
        container: u16,

        /// The port, on the pod's cluster IP, to expose the container port on.  If
        /// unspecified, use the same as the container port.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cluster: Option<u16>,
    },
}

/// The requested and maximum resources of a container.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContainerResourceSpec {
    /// The minimum resources required.  If the container is scheduled, it will
    /// get at least these.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requests: Option<ContainerResourceSpecInner>,

    /// The maximum resources required.  If the container is scheduled, it will
    /// get at most these.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<ContainerResourceSpecInner>,
}

/// Actual resource requests.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContainerResourceSpecInner {
    /// An amount of CPU.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<Decimal>,

    /// An amount of memory, in MiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<u64>,
}

impl PodSpec {
    /// Aggregate requested resources across all containers: the request is the
    /// sum of all the container requests, the limit is the maximum of all the
    /// container limits.
    pub fn aggregate_resources(&self) -> ContainerResourceSpec {
        let mut cpu_requests = Vec::with_capacity(self.containers.len());
        let mut cpu_limits = Vec::with_capacity(self.containers.len());
        let mut memory_requests = Vec::with_capacity(self.containers.len());
        let mut memory_limits = Vec::with_capacity(self.containers.len());

        for r in self.containers.iter().filter_map(|c| c.resources.as_ref()) {
            if let Some(requests) = &r.requests {
                cpu_requests.extend(requests.cpu.iter());
                memory_requests.extend(requests.memory.iter());
            }
            if let Some(limits) = &r.limits {
                cpu_limits.extend(limits.cpu.iter());
                memory_limits.extend(limits.memory.iter());
            }
        }

        ContainerResourceSpec {
            requests: Some(ContainerResourceSpecInner {
                cpu: if cpu_requests.is_empty() {
                    None
                } else {
                    Some(cpu_requests.iter().sum())
                },
                memory: if memory_requests.is_empty() {
                    None
                } else {
                    Some(memory_requests.iter().sum())
                },
            }),
            limits: Some(ContainerResourceSpecInner {
                cpu: cpu_limits.iter().max().copied(),
                memory: memory_limits.iter().max().copied(),
            }),
        }
    }
}

impl ContainerResourceSpec {
    /// Check if an available amount of CPU meets the requested amount.
    pub fn cpu_ok(&self, available_cpu: Option<Decimal>) -> bool {
        if let Some(requested) = self.requests.as_ref().and_then(|r| r.cpu) {
            available_cpu.map_or(false, |available| available >= requested)
        } else {
            true
        }
    }

    /// Check if an available amount of memory meets the requested amount.
    pub fn memory_ok(&self, available_memory: Option<u64>) -> bool {
        if let Some(requested) = self.requests.as_ref().and_then(|r| r.memory) {
            available_memory.map_or(false, |available| available >= requested)
        } else {
            true
        }
    }
}

impl TryFrom<Resource> for PodResource {
    type Error = TryFromError;

    fn try_from(resource: Resource) -> Result<Self, Self::Error> {
        let GenericResource {
            name,
            rtype,
            state,
            metadata,
            spec,
        } = resource;

        let pod_type = PodType::from_str(&rtype)?;
        let pod_state = if let Some(s) = state {
            PodState::from_str(&s)?
        } else {
            return Err(ResourceError::BadState.into());
        };

        let value = serde_json::to_value(spec).unwrap();
        let pod_spec = serde_json::from_value::<PodSpec>(value)?;

        Ok(GenericResource {
            name,
            rtype: pod_type,
            state: Some(pod_state),
            metadata,
            spec: pod_spec,
        })
    }
}

impl TryFrom<Value> for PodResource {
    type Error = TryFromError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Self::try_from(Resource::try_from(value)?)
    }
}

impl TryFrom<Vec<u8>> for PodResource {
    type Error = TryFromError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(Resource::try_from(bytes)?)
    }
}
