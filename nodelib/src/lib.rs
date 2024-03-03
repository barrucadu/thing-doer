pub mod etcd;
pub mod heartbeat;
pub mod resources;
pub mod util;

use std::net::SocketAddr;
use std::process;
use tokio::signal::unix::{signal, SignalKind};

use crate::etcd::leaser;

/// Exit code in case the initialisation process failed.
pub static EXIT_CODE_INITIALISE_FAILED: i32 = 1;

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Config {
    /// Name of this instance, must be unique across the cluster.  If
    /// unspecified, a random name is generated from the advertise address.
    #[clap(
        long,
        value_parser = |s: &str| Ok::<Option<String>, String>(Some(s.to_lowercase())),
    )]
    pub name: Option<String>,

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
    Reaper,
    Scheduler,
    Worker,
}

impl std::fmt::Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeType::Reaper => write!(f, "reaper"),
            NodeType::Scheduler => write!(f, "scheduler"),
            NodeType::Worker => write!(f, "worker"),
        }
    }
}

/// Start up a new node: register the resource definition and begin the
/// heartbeat processes.
///
/// Returns the node name, which will be a random unique one if not specified by
/// the `config`, and the liveness lease ID.
pub async fn initialise(
    config: Config,
    node_type: NodeType,
) -> Result<(String, leaser::LeaseId), Error> {
    let address = config.advertise_address.unwrap_or(config.listen_address);
    let name = config.name.unwrap_or(util::sockaddr_to_name(address));

    if heartbeat::is_alive(&config.etcd, &name).await? {
        tracing::error!(
            name,
            "another node with this name is still alive, terminating..."
        );
        process::exit(EXIT_CODE_INITIALISE_FAILED);
    }

    let resource = resources::Resource::new(name.clone(), format!("node.{node_type}"))
        .with_spec("address", serde_json::json!(address));
    resources::put(&config.etcd, resource).await?;

    let (healthy_lease, alive_lease) = heartbeat::establish_leases(&config.etcd, &name).await?;
    let alive_lease_id = alive_lease.id;

    tokio::spawn(leaser::task(config.etcd.clone(), healthy_lease));
    tokio::spawn(leaser::task(config.etcd.clone(), alive_lease));

    Ok((name, alive_lease_id))
}

/// Wait for SIGTERM.
pub async fn wait_for_sigterm() {
    match signal(SignalKind::terminate()) {
        Ok(mut stream) => {
            stream.recv().await;
            tracing::info!("received shutdown signal, terminating...");
        }
        Err(error) => {
            tracing::error!(?error, "could not subscribe to SIGTERM");
            process::exit(EXIT_CODE_INITIALISE_FAILED);
        }
    };
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
    BadType,
}

/// Errors specific to streaming RPCs.
#[derive(Debug)]
pub enum StreamingError {
    CannotSend,
    Ended,
    TimedOut,
}
