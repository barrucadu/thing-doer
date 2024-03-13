use clap::Parser;
use std::net::Ipv4Addr;
use std::process;

use nodelib::etcd;
use nodelib::resources::node::*;

use apid::web;

/// Add an alias record for the current host to `api.special.cluster.local.`
pub static SPECIAL_HOSTNAME: &str = "api";

/// thing-doer workerd.
#[derive(Clone, Debug, Parser)]
struct Args {
    /// Name of this instance, must be unique across the cluster.  If
    /// unspecified, a random name is generated.
    #[clap(long)]
    pub name: Option<String>,

    /// Cluster address to bind on to provide services to pods.  This, or
    /// `--external-address`, or both must be specified.
    #[clap(long = "cluster-address", value_parser, env = "CLUSTER_ADDRESS")]
    pub cluster_address: Option<Ipv4Addr>,

    /// External address to bind on to provide services to out-of-cluster users.
    /// This, or `--cluster-address`, or both must be specified.
    #[clap(long = "external-address", value_parser, env = "EXTERNAL_ADDRESS")]
    pub external_address: Option<Ipv4Addr>,

    #[command(flatten)]
    pub etcd: etcd::Config,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().json().init();

    let Args {
        name,
        cluster_address,
        external_address,
        etcd,
    } = Args::parse();

    if cluster_address.is_none() && external_address.is_none() {
        tracing::error!("--cluster-address or --external-address (or both) must be given");
        process::exit(1);
    }

    if let Some(address) = cluster_address {
        tokio::spawn(web::serve(etcd.clone(), address));
    }
    if let Some(address) = external_address {
        tokio::spawn(web::serve(etcd.clone(), address));
    }

    let state = nodelib::initialise(
        etcd.clone(),
        name,
        NodeType::Api,
        NodeSpec {
            address: cluster_address,
            limits: None,
        },
        Some(SPECIAL_HOSTNAME),
    )
    .await?;

    let ch = nodelib::wait_for_sigterm(state).await;
    nodelib::signal_channel(ch).await;
    process::exit(0)
}
