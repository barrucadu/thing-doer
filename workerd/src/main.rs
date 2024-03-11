use clap::Parser;
use rust_decimal::Decimal;
use std::net::{Ipv4Addr, SocketAddr};
use std::process;

use nodelib::resources::node::*;

use workerd::dns;
use workerd::limits;
use workerd::pod_claimer;
use workerd::pod_worker;
use workerd::podman;

/// thing-doer workerd.
#[derive(Clone, Debug, Parser)]
struct Args {
    #[command(flatten)]
    pub node: nodelib::Config,

    /// Address to bind on to provide services to local pods.  Must be reachable
    /// within the cluster.
    #[clap(long, value_parser, env = "ADDRESS")]
    pub address: Ipv4Addr,

    /// DNS resolver to forward non-cluster DNS queries from pods to.
    #[clap(long, value_parser, default_value = "1.1.1.1:53", env = "EXTERNAL_DNS")]
    pub external_dns: SocketAddr,

    /// Available amount of CPU resource.
    #[clap(long, value_parser, default_value = "1", env = "AVAILABLE_CPU")]
    pub cpu: Decimal,

    /// Available amount of memory resource, in megabytes.
    #[clap(long, value_parser, default_value = "1024", env = "AVAILABLE_MEMORY")]
    pub memory: u64,

    #[command(flatten)]
    pub podman: podman::Config,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().json().init();

    let Args {
        node,
        address,
        external_dns,
        cpu,
        memory,
        podman,
    } = Args::parse();
    let etcd_config = node.etcd.clone();

    let state = nodelib::initialise(
        node,
        NodeType::Worker,
        NodeSpec {
            address: Some(address),
            limits: Some(NodeLimitSpec { cpu, memory }),
        },
    )
    .await?;

    dns::initialise(etcd_config.clone(), &state.name, address, external_dns).await?;

    let limit_tx = limits::initialise(
        etcd_config.clone(),
        state.name.clone(),
        state.alive_lease_id,
        cpu,
        memory,
    )
    .await?;
    let work_pod_tx = pod_worker::initialise(
        etcd_config.clone(),
        podman,
        state.name.clone(),
        address,
        state.alive_lease_id,
        limit_tx,
    )
    .await?;
    pod_claimer::initialise(
        etcd_config,
        state.name.clone(),
        state.alive_lease_id,
        work_pod_tx,
    )
    .await?;

    let ch = nodelib::wait_for_sigterm(state).await;
    // TODO: terminate pods
    nodelib::signal_channel(ch).await;
    process::exit(0)
}
