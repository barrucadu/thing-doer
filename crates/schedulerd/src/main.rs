use clap::Parser;
use std::process;
use tokio::sync::mpsc;

use nodelib::etcd;
use nodelib::resources::node::*;

use schedulerd::scheduler;
use schedulerd::state::SharedNodeState;
use schedulerd::watcher;

/// thing-doer schedulerd.
#[derive(Clone, Debug, Parser)]
struct Args {
    /// Name of this instance, must be unique across the cluster.  If
    /// unspecified, a random name is generated.
    #[clap(long, env = "NODE_NAME")]
    pub name: Option<String>,

    #[command(flatten)]
    pub etcd: etcd::Config,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().json().init();

    let Args { name, etcd } = Args::parse();

    let state = nodelib::initialise(
        etcd.clone(),
        name,
        NodeType::Scheduler,
        NodeSpec::default(),
        None,
    )
    .await?;

    let shared_node_state = SharedNodeState::default();
    let (new_pod_tx, new_pod_rx) = mpsc::unbounded_channel();
    tokio::spawn(scheduler::task(
        etcd.clone(),
        shared_node_state.clone(),
        state.name.clone(),
        new_pod_rx,
        new_pod_tx.clone(),
    ));
    watcher::initialise(etcd, shared_node_state, new_pod_tx).await?;

    let ch = nodelib::wait_for_sigterm(state).await;
    nodelib::signal_channel(ch).await;
    process::exit(0)
}
