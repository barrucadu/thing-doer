use clap::Parser;
use std::fs;
use std::net::Ipv4Addr;
use std::path::PathBuf;
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

    /// Read the cluster address from a file.  This option is incompatible with
    /// `--cluster-address`.
    #[clap(
        long = "cluster-address-file",
        value_parser,
        env = "CLUSTER_ADDRESS_FILE"
    )]
    pub cluster_address_file: Option<PathBuf>,

    /// External address to bind on to provide services to out-of-cluster users.
    /// This, or `--cluster-address`, or both must be specified.
    #[clap(long = "external-address", value_parser, env = "EXTERNAL_ADDRESS")]
    pub external_address: Option<Ipv4Addr>,

    /// Read the external address from a file.  This option is incompatible with
    /// `--external-address`.
    #[clap(
        long = "external-address-file",
        value_parser,
        env = "EXTERNAL_ADDRESS_FILE"
    )]
    pub external_address_file: Option<PathBuf>,

    #[command(flatten)]
    pub etcd: etcd::Config,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().json().init();

    let Args {
        name,
        cluster_address,
        cluster_address_file,
        external_address,
        external_address_file,
        etcd,
    } = Args::parse();

    let actual_cluster_address = match (cluster_address, cluster_address_file) {
        (Some(_), Some(_)) => {
            tracing::error!(
                "--cluster-address cannot be specified at the same time as --cluster-address-file"
            );
            process::exit(1);
        }
        (Some(ip), _) => Some(ip),
        (_, Some(fpath)) => match read_ip_from_file(fpath) {
            Ok(ip) => Some(ip),
            Err(error) => {
                tracing::error!(?error, "cannot parse --cluster-address-file");
                process::exit(1);
            }
        },
        (_, _) => None,
    };

    let actual_external_address = match (external_address, external_address_file) {
        (Some(_), Some(_)) => {
            tracing::error!("--external-address cannot be specified at the same time as --external-address-file");
            process::exit(1);
        }
        (Some(ip), _) => Some(ip),
        (_, Some(fpath)) => match read_ip_from_file(fpath) {
            Ok(ip) => Some(ip),
            Err(error) => {
                tracing::error!(?error, "cannot parse --external-address-file");
                process::exit(1);
            }
        },
        (_, _) => None,
    };

    if actual_cluster_address.is_none() && actual_external_address.is_none() {
        tracing::error!("--cluster-address or --external-address (or both) must be given");
        process::exit(1);
    }

    if let Some(address) = actual_cluster_address {
        tokio::spawn(web::serve(etcd.clone(), address));
    }
    if let Some(address) = actual_external_address {
        tokio::spawn(web::serve(etcd.clone(), address));
    }

    let state = nodelib::initialise(
        etcd.clone(),
        name,
        NodeType::Api,
        NodeSpec {
            address: actual_cluster_address,
            limits: None,
        },
        Some(SPECIAL_HOSTNAME),
    )
    .await?;

    let ch = nodelib::wait_for_sigterm(state).await;
    nodelib::signal_channel(ch).await;
    process::exit(0)
}

fn read_ip_from_file(p: PathBuf) -> Result<Ipv4Addr, Box<dyn std::error::Error>> {
    let ip = fs::read_to_string(p)?.parse()?;

    Ok(ip)
}
