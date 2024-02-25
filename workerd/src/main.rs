use clap::Parser;

use workerd::args::Args;
use workerd::supervisord;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().json().init();

    let args = Args::parse();

    supervisord::task(
        args.supervisord_config,
        args.name,
        args.advertise_address.unwrap_or(args.listen_address),
    )
    .await?;

    Ok(())
}
