use clap::Parser;
use std::process;
use tokio::sync::mpsc;

use nodelib::etcd;
use nodelib::resources::node::*;

use reaperd::node_reaper;
use reaperd::pod_reaper;
use reaperd::watcher;

/// thing-doer reaperd.
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
        NodeType::Reaper,
        NodeSpec::default(),
        None,
    )
    .await?;

    let (reap_node_tx, reap_node_rx) = mpsc::unbounded_channel();
    let (reap_pod_tx, reap_pod_rx) = mpsc::unbounded_channel();
    tokio::spawn(node_reaper::task(
        etcd.clone(),
        reap_node_tx.clone(),
        reap_node_rx,
        reap_pod_tx.clone(),
    ));
    tokio::spawn(pod_reaper::task(
        etcd.clone(),
        state.name.clone(),
        reap_pod_tx.clone(),
        reap_pod_rx,
    ));
    watcher::initialise(etcd, reap_node_tx, reap_pod_tx).await?;

    let ch = nodelib::wait_for_sigterm(state).await;
    nodelib::signal_channel(ch).await;
    process::exit(0)
}
