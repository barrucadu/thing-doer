use clap::Parser;

use nodelib::etcd;
use nodelib::resources;

/// thing-doer integration test tools: pod spammer.
#[derive(Clone, Debug, Parser)]
struct Args {
    /// The name of the pod to kill.
    pub pod_name: String,

    #[command(flatten)]
    pub etcd: etcd::Config,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Args::parse();
    resources::delete_and_kill_pod(&config.etcd, &config.pod_name).await?;

    Ok(())
}
