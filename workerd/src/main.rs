use clap::Parser;
use std::process;
use tokio::signal::unix::{signal, SignalKind};

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

    let name = nodelib::initialise(config, nodelib::NodeType::Worker).await?;

    pod_watcher::initialise(etcd_config, name).await?;

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
