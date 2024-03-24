use clap::{Parser, Subcommand};
use reqwest::Url;

use apiclient::pod;

/// thing-doer API client.
#[derive(Debug, Parser)]
struct Args {
    /// Address of the API server.
    #[clap(long, value_parser, default_value = "http://127.0.0.1/")]
    apid_url: Url,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run a single-container pod, like `podman run`.  Prints the name of the
    /// pod to stdout.
    Run(pod::RunArgs),
}

fn main() {
    let args = Args::parse();
    match args.command {
        Command::Run(run_args) => pod::cmd_run(&args.apid_url, run_args),
    }
}
