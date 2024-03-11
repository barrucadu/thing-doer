use clap::Parser;
use std::process;

use nodelib::etcd;
use nodelib::resources::node::*;

use schedulerd::node_watcher;
use schedulerd::pod_scheduler;

/// thing-doer schedulerd.
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
    let node_state = node_watcher::initialise(etcd.clone()).await?;

    let state =
        nodelib::initialise(etcd.clone(), name, NodeType::Scheduler, NodeSpec::default()).await?;

    pod_scheduler::initialise(etcd, node_state, state.name.clone()).await?;

    let ch = nodelib::wait_for_sigterm(state).await;
    nodelib::signal_channel(ch).await;
    process::exit(0)
}
