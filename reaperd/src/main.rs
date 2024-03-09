use clap::Parser;
use std::process;

use nodelib::resources::node::*;

use reaperd::node_watcher;
use reaperd::pod_watcher;

/// thing-doer reaperd.
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

    let state = nodelib::initialise(config, NodeType::Reaper, NodeSpec::default()).await?;

    let reap_pod_tx = pod_watcher::initialise(etcd_config.clone(), state.name.clone()).await?;
    node_watcher::initialise(etcd_config, reap_pod_tx).await?;

    let ch = nodelib::wait_for_sigterm(state).await;
    nodelib::signal_channel(ch).await;
    process::exit(0)
}
