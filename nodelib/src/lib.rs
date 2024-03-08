pub mod etcd;
pub mod heartbeat;
pub mod resources;
pub mod types;
pub mod util;

use serde_json::Value;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::process;
use tokio::signal::unix::{signal, SignalKind};

use crate::etcd::leaser;
use crate::types::{Error, NodeType};

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

/// Start up a new node: register the resource definition and begin the
/// heartbeat processes.
///
/// Returns the node name, which will be a random unique one if not specified by
/// the `config`, and the liveness lease ID.
pub async fn initialise(
    config: Config,
    node_type: NodeType,
    mut spec: HashMap<String, Value>,
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

    spec.insert("address".to_owned(), serde_json::json!(address));
    let resource = resources::Resource::new(name.clone(), format!("node.{node_type}"), spec);

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
