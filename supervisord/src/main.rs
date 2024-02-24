use clap::Parser;

use supervisord::args::Args;
use supervisord::heartbeat;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().json().init();

    let args = Args::parse();

    let (healthy_lease, alive_lease) =
        heartbeat::establish_leases(&args.etcd_config, &args.name).await?;

    tokio::spawn(heartbeat::task(args.etcd_config.clone(), healthy_lease));
    heartbeat::task(args.etcd_config, alive_lease).await;

    Ok(())
}
