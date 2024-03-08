use clap::Parser;
use rust_decimal::Decimal;
use std::process;

use nodelib::resources::node::*;

use workerd::limits;
use workerd::pod_claimer;
use workerd::pod_worker;
use workerd::podman;

/// thing-doer workerd.
#[derive(Clone, Debug, Parser)]
struct Args {
    #[command(flatten)]
    pub node: nodelib::Config,

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
        cpu,
        memory,
        podman,
    } = Args::parse();
    let etcd_config = node.etcd.clone();

    let state = nodelib::initialise(
        node,
        NodeType::Worker,
        NodeSpec {
            limits: Some(NodeLimitSpec { cpu, memory }),
        },
    )
    .await?;

    let limit_tx = limits::initialise(
        etcd_config.clone(),
        state.name.clone(),
        state.alive_lease_id,
        cpu,
        memory,
    )
    .await?;
    let work_pod_tx =
        pod_worker::initialise(etcd_config.clone(), podman, state.name.clone(), limit_tx).await?;
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
