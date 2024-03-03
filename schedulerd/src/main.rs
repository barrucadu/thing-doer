use clap::Parser;
use std::process;

use nodelib::types::NodeType;

use schedulerd::node_watcher;
use schedulerd::pod_scheduler;

/// thing-doer schedulerd.
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
    let node_state = node_watcher::initialise(etcd_config.clone()).await?;

    let (name, _) = nodelib::initialise(config, NodeType::Scheduler).await?;

    pod_scheduler::initialise(etcd_config, node_state, name).await?;

    nodelib::wait_for_sigterm().await;
    process::exit(0);
}
