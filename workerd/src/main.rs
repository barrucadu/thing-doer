use clap::Parser;
use rust_decimal::Decimal;
use std::process;

use nodelib::types::NodeType;

use workerd::limits;
use workerd::pod_claimer;
use workerd::pod_worker;

/// thing-doer workerd.
#[derive(Clone, Debug, Parser)]
struct Args {
    #[command(flatten)]
    pub node: nodelib::Config,

    /// Available amount of CPU resource.
    #[clap(long, value_parser, default_value = "1", env = "AVAILABLE_CPU")]
    pub cpu: Decimal,

    /// Available amount of memory resource, in megabytes.
    #[clap(long, value_parser, default_value = "1024", env = "AVAILABLE_MEMORY")]
    pub memory: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().json().init();

    let config = Args::parse();
    let etcd_config = config.node.etcd.clone();

    let (name, lease_id) = nodelib::initialise(
        config.node,
        NodeType::Worker,
        &[(
            "limits",
            serde_json::json!({
                "cpu": config.cpu,
                "memory": config.memory,
            }),
        )],
    )
    .await?;

    let limit_tx = limits::initialise(
        etcd_config.clone(),
        name.clone(),
        lease_id,
        config.cpu,
        config.memory,
    )
    .await?;
    let work_pod_tx = pod_worker::initialise(etcd_config.clone(), limit_tx).await?;
    pod_claimer::initialise(etcd_config, name, lease_id, work_pod_tx).await?;

    nodelib::wait_for_sigterm().await;
    process::exit(0);
}
