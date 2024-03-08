use clap::Parser;
use std::collections::HashMap;
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

    let state = nodelib::initialise(config, NodeType::Scheduler, HashMap::new()).await?;

    pod_scheduler::initialise(etcd_config, node_state, state.name.clone()).await?;

    let ch = nodelib::wait_for_sigterm(state).await;
    nodelib::signal_channel(ch).await;
    process::exit(0)
}
