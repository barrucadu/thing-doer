use clap::Parser;
use serde_json::json;

use supervisord::args::Args;
use supervisord::etcd::leaser;
use supervisord::heartbeat;
use supervisord::resources;
use supervisord::services;
use supervisord::state;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().json().init();

    let args = Args::parse();
    let _state = state::State::initialise(args.etcd_config.clone()).await?;

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

    tokio::spawn(leaser::task(args.etcd_config.clone(), healthy_lease));
    tokio::spawn(leaser::task(args.etcd_config.clone(), alive_lease));

    services::task(args.etcd_config, args.listen_address).await?;

    Ok(())
}
