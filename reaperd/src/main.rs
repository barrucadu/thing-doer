use clap::Parser;
use std::process;

use nodelib::etcd;
use nodelib::resources::node::*;

use reaperd::node_watcher;
use reaperd::pod_watcher;

/// thing-doer reaperd.
#[derive(Clone, Debug, Parser)]
struct Args {
    /// Name of this instance, must be unique across the cluster.  If
    /// unspecified, a random name is generated.
    #[clap(long)]
    pub name: Option<String>,

    #[command(flatten)]
    pub etcd: etcd::Config,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().json().init();

    let Args { name, etcd } = Args::parse();

    let state =
        nodelib::initialise(etcd.clone(), name, NodeType::Reaper, NodeSpec::default()).await?;

    let reap_pod_tx = pod_watcher::initialise(etcd.clone(), state.name.clone()).await?;
    node_watcher::initialise(etcd, reap_pod_tx).await?;

    let ch = nodelib::wait_for_sigterm(state).await;
    nodelib::signal_channel(ch).await;
    process::exit(0)
}
