use clap::Parser;
use std::process;

use nodelib::types::NodeType;

use workerd::pod_claimer;
use workerd::pod_worker;

/// thing-doer workerd.
#[derive(Clone, Debug, Parser)]
struct Args {
    #[command(flatten)]
    pub node: nodelib::Config,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().json().init();

    let config = Args::parse().node;
    let etcd_config = config.etcd.clone();

    let (name, lease_id) = nodelib::initialise(config, NodeType::Worker).await?;

    let work_pod_tx = pod_worker::initialise(etcd_config.clone()).await?;
    pod_claimer::initialise(etcd_config, name, lease_id, work_pod_tx).await?;

    nodelib::wait_for_sigterm().await;
    process::exit(0);
}
