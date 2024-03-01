use clap::Parser;
use std::process;
use tokio::signal::unix::{signal, SignalKind};

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

    let name = nodelib::initialise(config, nodelib::NodeType::Scheduler).await?;

    pod_scheduler::initialise(etcd_config, node_state, name).await?;

    match signal(SignalKind::terminate()) {
        Ok(mut stream) => {
            stream.recv().await;
            tracing::info!("received shutdown signal, terminating...");
            process::exit(0);
        }
        Err(error) => {
            tracing::error!(?error, "could not subscribe to SIGUSR1");
            process::exit(1);
        }
    };
}
