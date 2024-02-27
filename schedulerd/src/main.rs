use clap::Parser;
use std::process;
use tokio::signal::unix::{signal, SignalKind};

use schedulerd::state::State;

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
    let _state = State::initialise(config.etcd.clone()).await?;

    nodelib::initialise(config, nodelib::NodeType::Scheduler).await?;

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
