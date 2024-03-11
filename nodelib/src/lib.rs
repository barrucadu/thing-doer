pub mod error;
pub mod etcd;
pub mod heartbeat;
pub mod resources;
pub mod util;

use std::net::Ipv4Addr;
use std::process;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::oneshot;
use tonic::Request;

use crate::error::*;
use crate::etcd::leaser;
use crate::etcd::pb::etcdserverpb::PutRequest;
use crate::etcd::prefix;
use crate::resources::node::*;

/// Exit code in case the initialisation process failed.
pub static EXIT_CODE_INITIALISE_FAILED: i32 = 1;

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
    etcd_config: etcd::Config,
    node_name: Option<String>,
    node_type: NodeType,
    node_spec: NodeSpec,
) -> Result<State, Error> {
    let name = node_name.unwrap_or(util::random_name()).to_lowercase();
    let address = node_spec.address;

    if heartbeat::is_alive(&etcd_config, &name).await? {
        tracing::error!(
            name,
            "another node with this name is still alive, terminating..."
        );
        process::exit(EXIT_CODE_INITIALISE_FAILED);
    }

    let resource = resources::NodeResource::new(name.clone(), node_type, node_spec)
        .with_state(NodeState::Healthy);
    resources::put(&etcd_config, resource).await?;

    let (healthy_lease, alive_lease) = heartbeat::establish_leases(&etcd_config, &name).await?;
    let alive_lease_id = alive_lease.id;

    let (expire_healthy_tx, expire_healthy_rx) = oneshot::channel();
    let (expire_alive_tx, expire_alive_rx) = oneshot::channel();
    tokio::spawn(leaser::task(
        etcd_config.clone(),
        expire_healthy_rx,
        healthy_lease,
    ));
    tokio::spawn(leaser::task(
        etcd_config.clone(),
        expire_alive_rx,
        alive_lease,
    ));

    if let Some(ip) = address {
        create_node_dns_record(&etcd_config, alive_lease_id, &name, ip).await?;
    }

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

///////////////////////////////////////////////////////////////////////////////

/// Create the DNS record for a node.
async fn create_node_dns_record(
    etcd_config: &etcd::Config,
    lease_id: leaser::LeaseId,
    name: &str,
    address: Ipv4Addr,
) -> Result<(), Error> {
    let mut kv_client = etcd_config.kv_client().await?;
    kv_client
        .put(Request::new(PutRequest {
            key: format!(
                "{prefix}{name}.node.cluster.local.",
                prefix = prefix::domain_name(etcd_config),
            )
            .into(),
            value: address.to_string().into(),
            lease: lease_id.0,
            ..Default::default()
        }))
        .await?;

    Ok(())
}
