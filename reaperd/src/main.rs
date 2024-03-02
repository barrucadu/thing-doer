use clap::Parser;
use std::process;

use reaperd::pod_reaper;

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

    let (name, _) = nodelib::initialise(config, nodelib::NodeType::Reaper).await?;

    pod_reaper::initialise(etcd_config, name).await?;

    nodelib::wait_for_sigterm().await;
    process::exit(0)
}
