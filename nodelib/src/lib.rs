pub mod etcd;
pub mod heartbeat;
pub mod resources;
pub mod util;

use std::net::SocketAddr;
use std::process;

use crate::etcd::leaser;

/// Exit code in case the initialisation process failed.
pub static EXIT_CODE_INITIALISE_FAILED: i32 = 1;

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Config {
    /// Name of this instance, must be unique across the cluster
    #[clap(long, value_parser = |s: &str| Ok::<String, String>(s.to_lowercase()))]
    pub name: String,

    /// Address to listen for new connections on.
    #[clap(long, value_parser, env = "LISTEN_ADDRESS")]
    pub listen_address: SocketAddr,

    /// Address to advertise to the rest of the cluster.  If unspecified, the
    /// listen address is used.
    #[clap(long, value_parser, env = "ADVERTISE_ADDRESS")]
    pub advertise_address: Option<SocketAddr>,

    #[command(flatten)]
    pub etcd: etcd::Config,
}

/// The type of a node.
#[derive(Debug)]
pub enum NodeType {
    Scheduler,
    Worker,
}

impl std::fmt::Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeType::Scheduler => write!(f, "scheduler"),
            NodeType::Worker => write!(f, "worker"),
        }
    }
}

/// Start up a new node: register the resource definition and begin the
/// heartbeat processes.
pub async fn initialise(config: Config, node_type: NodeType) -> Result<(), Error> {
    if heartbeat::is_alive(&config.etcd, &config.name).await? {
        tracing::error!(
            name = config.name,
            "another node with this name is still alive, terminating..."
        );
        process::exit(EXIT_CODE_INITIALISE_FAILED);
    }

    let spec = serde_json::json!({
        "type": format!("node.{node_type}"),
        "name": config.name,
        "spec": {
            "address": config.advertise_address.unwrap_or(config.listen_address),
        },
    });
    resources::put(&config.etcd, spec).await?;

    let (healthy_lease, alive_lease) =
        heartbeat::establish_leases(&config.etcd, &config.name).await?;

    tokio::spawn(leaser::task(config.etcd.clone(), healthy_lease));
    tokio::spawn(leaser::task(config.etcd.clone(), alive_lease));

    Ok(())
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

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::EtcdResponse(s) => write!(f, "etcd response: {s}"),
            Error::FromUtf8(s) => write!(f, "from utf8: {s}"),
            Error::Resource(s) => write!(f, "resource: {s:?}"),
            Error::Streaming(s) => write!(f, "streaming: {s:?}"),
            Error::TonicStatus(s) => write!(f, "tonic status: {s}"),
            Error::TonicTransport(s) => write!(f, "tonic transport: {s}"),
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
    BadStructure,
}

/// Errors specific to streaming RPCs.
#[derive(Debug)]
pub enum StreamingError {
    CannotSend,
    Ended,
    TimedOut,
}
