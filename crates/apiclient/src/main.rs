use clap::{Parser, Subcommand};
use reqwest::Url;

use apiclient::dns_alias;
use apiclient::pod;
use apiclient::resource;

/// thing-doer API client.
#[derive(Debug, Parser)]
struct Args {
    /// Address of the API server.
    #[clap(long, value_parser, default_value = "http://127.0.0.1/")]
    apid_url: Url,

    #[command(subcommand)]
    command: Command,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Subcommand)]
enum Command {
    /// Run a single-container pod, like `podman run`.  Prints the name of the
    /// pod to stdout.
    Run(pod::RunArgs),
    /// Create a resource, if it does not already exist, or patch it, if it
    /// does.
    Apply(resource::ApplyArgs),
    /// List matching resources.
    List(resource::ListArgs),
    /// Get a single resource.
    Get(resource::IdentifyArgs),
    /// Delete a single resource.  If this is a running pod, it is killed.
    Delete(resource::IdentifyArgs),
    /// List all DNS aliases from a given source name.
    Aliases(dns_alias::ListArgs),
    /// Create a DNS alias.
    CreateAlias(dns_alias::IdentifyArgs),
    /// Delete a DNS alias.
    DeleteAlias(dns_alias::IdentifyArgs),
}

fn main() {
    let args = Args::parse();
    match args.command {
        Command::Run(cmd_args) => pod::cmd_run(&args.apid_url, cmd_args),
        Command::Apply(cmd_args) => resource::cmd_apply(&args.apid_url, cmd_args),
        Command::List(cmd_args) => resource::cmd_list(&args.apid_url, cmd_args),
        Command::Get(cmd_args) => resource::cmd_get(&args.apid_url, cmd_args),
        Command::Delete(cmd_args) => resource::cmd_delete(&args.apid_url, cmd_args),
        Command::Aliases(cmd_args) => dns_alias::cmd_aliases(&args.apid_url, cmd_args),
        Command::CreateAlias(cmd_args) => dns_alias::cmd_create_alias(&args.apid_url, cmd_args),
        Command::DeleteAlias(cmd_args) => dns_alias::cmd_delete_alias(&args.apid_url, cmd_args),
    }
}
