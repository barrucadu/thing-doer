use std::fmt;

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
#[derive(Debug)]
pub enum ResourceError {
    BadName,
    BadType,
    BadState,
    Deserialise(serde_json::Error),
}

impl From<serde_json::Error> for ResourceError {
    fn from(error: serde_json::Error) -> Self {
        Self::Deserialise(error)
    }
}

/// Errors specific to streaming RPCs.
#[derive(Debug)]
pub enum StreamingError {
    Ended,
    TimedOut,
}
