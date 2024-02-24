pub mod args;
pub mod heartbeat;

/// Exit code in case the heartbeat process loses connection to etcd and cannot
/// reestablish it.
pub static EXIT_CODE_HEARTBEAT_FAILED: i32 = 3;

/// Generic error type
#[derive(Debug)]
pub enum Error {
    EtcdResponse(String),
    Streaming(StreamingError),
    TonicStatus(tonic::Status),
    TonicTransport(tonic::transport::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::EtcdResponse(s) => write!(f, "etcd response: {s}"),
            Error::Streaming(s) => write!(f, "streaming: {s:?}"),
            Error::TonicStatus(s) => write!(f, "tonic status: {s}"),
            Error::TonicTransport(s) => write!(f, "tonic transport: {s}"),
        }
    }
}

impl std::error::Error for Error {}

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

/// Errors specific to streaming RPCs.
#[derive(Debug)]
pub enum StreamingError {
    CannotSend,
    Ended,
    TimedOut,
}
