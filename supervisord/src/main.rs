use clap::Parser;
use serde_json::json;

use supervisord::args::Args;
use supervisord::heartbeat;
use supervisord::resources;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().json().init();

    let args = Args::parse();

    let spec = json!({
        "type": "node.supervisor",
        "name": args.name,
        "spec": {
            "address": args.advertise_address.unwrap_or(args.listen_address),
        },
    });
    resources::put(&args.etcd_config, spec).await?;

    let (healthy_lease, alive_lease) =
        heartbeat::establish_leases(&args.etcd_config, &args.name).await?;

    tokio::spawn(heartbeat::task(args.etcd_config.clone(), healthy_lease));
    heartbeat::task(args.etcd_config, alive_lease).await;

    Ok(())
}
