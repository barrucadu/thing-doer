use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;

/// The type of a node.
#[derive(Debug, Copy, Clone)]
pub enum NodeType {
    Reaper,
    Scheduler,
    Worker,
}

impl fmt::Display for NodeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reaper => write!(f, "reaper"),
            Self::Scheduler => write!(f, "scheduler"),
            Self::Worker => write!(f, "worker"),
        }
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
            Self::Created | Self::Scheduled | Self::Accepted => false,
        }
    }

    /// Turn this to a resource `state`.
    pub fn to_resource_state(&self) -> String {
        serde_plain::to_string(self).unwrap()
    }

    /// Parse this from a resource `state`.
    pub fn from_resource_state(s: &str) -> Option<Self> {
        serde_plain::from_str(s).ok()
    }
}

///////////////////////////////////////////////////////////////////////////////

/// The requested / maximum resources of a pod.
#[derive(Debug, Default)]
pub struct PodLimits {
    pub requested_cpu: Option<Decimal>,
    pub maximum_cpu: Option<Decimal>,
    pub requested_memory: Option<u64>,
    pub maximum_memory: Option<u64>,
}

impl PodLimits {
    /// Check if an available amount of CPU meets the requested amount.
    pub fn cpu_ok(&self, available_cpu: Option<Decimal>) -> bool {
        if let Some(requested) = self.requested_cpu {
            available_cpu.map_or(false, |available| available >= requested)
        } else {
            true
        }
    }

    /// Check if an available amount of memory meets the requested amount.
    pub fn memory_ok(&self, available_memory: Option<u64>) -> bool {
        if let Some(requested) = self.requested_memory {
            available_memory.map_or(false, |available| available >= requested)
        } else {
            true
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Generic error type
#[derive(Debug)]
pub enum Error {
    EtcdResponse(String),
    FromUtf8(std::string::FromUtf8Error),
    Resource(ResourceError),
    Streaming(StreamingError),
    TonicStatus(tonic::Status),
    TonicTransport(tonic::transport::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EtcdResponse(s) => write!(f, "etcd response: {s}"),
            Self::FromUtf8(s) => write!(f, "from utf8: {s}"),
            Self::Resource(s) => write!(f, "resource: {s:?}"),
            Self::Streaming(s) => write!(f, "streaming: {s:?}"),
            Self::TonicStatus(s) => write!(f, "tonic status: {s}"),
            Self::TonicTransport(s) => write!(f, "tonic transport: {s}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::string::FromUtf8Error> for Error {
    fn from(error: std::string::FromUtf8Error) -> Self {
        Self::FromUtf8(error)
    }
}

impl From<ResourceError> for Error {
    fn from(error: ResourceError) -> Self {
        Self::Resource(error)
    }
}

impl From<StreamingError> for Error {
    fn from(error: StreamingError) -> Self {
        Self::Streaming(error)
    }
}

impl From<tonic::Status> for Error {
    fn from(error: tonic::Status) -> Self {
        Self::TonicStatus(error)
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(error: tonic::transport::Error) -> Self {
        Self::TonicTransport(error)
    }
}

/// Errors specific to resource processing.
#[derive(Debug, Copy, Clone)]
pub enum ResourceError {
    BadName,
    BadType,
}

/// Errors specific to streaming RPCs.
#[derive(Debug, Copy, Clone)]
pub enum StreamingError {
    CannotSend,
    Ended,
    TimedOut,
}
