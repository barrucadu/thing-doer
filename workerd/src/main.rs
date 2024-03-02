use clap::Parser;
use std::process;

use workerd::pod_watcher;

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

    let (name, lease_id) = nodelib::initialise(config, nodelib::NodeType::Worker).await?;

    pod_watcher::initialise(etcd_config, name, lease_id).await?;

    nodelib::wait_for_sigterm().await;
    process::exit(0);
}
