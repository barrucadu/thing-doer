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
use tokio::sync::oneshot;

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

/// The static state of the node, set at initialisation time.
#[derive(Debug)]
pub struct State {
    pub name: String,
    pub alive_lease_id: leaser::LeaseId,
    pub expire_healthy_tx: oneshot::Sender<oneshot::Sender<()>>,
    pub expire_alive_tx: oneshot::Sender<oneshot::Sender<()>>,
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
) -> Result<State, Error> {
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

    let (expire_healthy_tx, expire_healthy_rx) = oneshot::channel();
    let (expire_alive_tx, expire_alive_rx) = oneshot::channel();
    tokio::spawn(leaser::task(
        config.etcd.clone(),
        expire_healthy_rx,
        healthy_lease,
    ));
    tokio::spawn(leaser::task(
        config.etcd.clone(),
        expire_alive_rx,
        alive_lease,
    ));

    Ok(State {
        name,
        alive_lease_id,
        expire_healthy_tx,
        expire_alive_tx,
    })
}

/// Wait for SIGTERM and mark the node as unhealthy when it arrives.  Returns a
/// channel to send a message on to mark the node as dead.
pub async fn wait_for_sigterm(state: State) -> oneshot::Sender<oneshot::Sender<()>> {
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    let mut sigquit = signal(SignalKind::interrupt()).unwrap();

    tokio::select! {
        _ = sigterm.recv() => (),
        _ = sigint.recv() => (),
        _ = sigquit.recv() => (),
    }

    tracing::info!("received shutdown signal, terminating...");
    signal_channel(state.expire_healthy_tx).await;

    state.expire_alive_tx
}

/// The `leaser` uses these channels to coordinate destroying the lease.
pub async fn signal_channel(ch: oneshot::Sender<oneshot::Sender<()>>) {
    let (tx, rx) = oneshot::channel();
    let _ = ch.send(tx);
    let _ = rx.await;
}
