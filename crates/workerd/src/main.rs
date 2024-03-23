use clap::Parser;
use rust_decimal::Decimal;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::process;

use nodelib::etcd;
use nodelib::resources::node::*;

use workerd::cluster_nameserver;
use workerd::containers;
use workerd::limits;
use workerd::pod_claimer;
use workerd::pod_killer;
use workerd::pod_worker;

/// Add an alias record for the current host to `dns.special.cluster.local.`
pub static SPECIAL_HOSTNAME: &str = "dns";

/// thing-doer workerd.
#[derive(Clone, Debug, Parser)]
struct Args {
    /// Name of this instance, must be unique across the cluster.  If
    /// unspecified, a random name is generated.
    #[clap(long, env = "NODE_NAME")]
    pub name: Option<String>,

    /// Address to bind on to provide services to local pods.  Must be reachable
    /// within the cluster.  This or `--cluster-address-file` must be specified.
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

    #[command(flatten)]
    pub etcd: etcd::Config,

    /// DNS resolver to forward non-cluster DNS queries from pods to.
    #[clap(
        long = "pod-external-dns",
        value_parser,
        default_value = "1.1.1.1:53",
        env = "POD_EXTERNAL_DNS"
    )]
    pub external_dns: SocketAddr,

    /// Available amount of CPU resource.
    #[clap(
        long = "pod-cpu-limit",
        value_parser,
        default_value = "1",
        env = "POD_CPU_LIMIT"
    )]
    pub cpu: Decimal,

    /// Available amount of memory resource, in megabytes.
    #[clap(
        long = "pod-memory-limit",
        value_parser,
        default_value = "1024",
        env = "POD_MEMORY_LIMIT"
    )]
    pub memory: u64,

    #[command(flatten)]
    pub containers: ContainersArgs,
}

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct ContainersArgs {
    /// Name of the podman binary.  Defaults to "podman".
    #[clap(
        long = "podman-command",
        value_parser,
        default_value = "podman",
        env = "PODMAN_CMD"
    )]
    pub podman: String,

    /// Prefix to prepend to pod names so they can be easily identified in
    /// `podman` command output.  If not given defaults to the node name.
    #[clap(
        long = "podman-pod-name-prefix",
        value_parser,
        env = "PODMAN_POD_NAME_PREFIX"
    )]
    pub pod_name_prefix: Option<String>,

    /// Bridge network to use.  This must already exist, and be set up so that
    /// all the pods in the cluster can communicate.
    #[clap(
        long = "podman-bridge-network",
        value_parser,
        env = "PODMAN_BRIDGE_NETWORK"
    )]
    pub bridge_network: String,

    /// Name of the iptables binary.  Defaults to "iptables".
    #[clap(
        long = "iptables-command",
        value_parser,
        default_value = "iptables",
        env = "IPTABLES_CMD"
    )]
    pub iptables: String,

    /// Name of the iptables chain to create.  If not given defaults to the node name.
    #[clap(long = "iptables-chain", value_parser, env = "IPTABLES_CHAIN")]
    pub iptables_chain: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().json().init();

    let Args {
        name,
        cluster_address,
        cluster_address_file,
        external_dns,
        cpu,
        memory,
        etcd,
        containers,
    } = Args::parse();

    let actual_cluster_address = match (cluster_address, cluster_address_file) {
        (Some(_), Some(_)) => {
            tracing::error!(
                "--cluster-address cannot be specified at the same time as --cluster-address-file"
            );
            process::exit(1);
        }
        (Some(ip), _) => ip,
        (_, Some(fpath)) => match read_ip_from_file(fpath) {
            Ok(ip) => ip,
            Err(error) => {
                tracing::error!(?error, "cannot parse --cluster-address-file");
                process::exit(1);
            }
        },
        (_, _) => {
            tracing::error!("--cluster-address or --cluster-address-file must be given");
            process::exit(1);
        }
    };

    let state = nodelib::initialise(
        etcd.clone(),
        name,
        NodeType::Worker,
        NodeSpec {
            address: Some(actual_cluster_address),
            limits: Some(NodeLimitSpec { cpu, memory }),
        },
        Some(SPECIAL_HOSTNAME),
    )
    .await?;
    let node_name = state.name.clone();

    let containers = containers::Config {
        firewall: containers::firewall::Config {
            command: containers.iptables.clone(),
            chain: containers
                .iptables_chain
                .clone()
                .unwrap_or(node_name.clone()),
        },
        runtime: containers::runtime::Config {
            command: containers.podman.clone(),
            pod_name_prefix: containers
                .pod_name_prefix
                .clone()
                .unwrap_or(node_name.clone()),
            bridge_network: containers.bridge_network.clone(),
        },
    };

    // TODO: kill pods and cleanup iptables rules left over from a prior unclean
    // shutdown.
    containers::initialise(&containers).await?;

    cluster_nameserver::initialise(
        etcd.clone(),
        &node_name,
        actual_cluster_address,
        external_dns,
    )
    .await?;

    let limit_tx = limits::initialise(
        etcd.clone(),
        node_name.clone(),
        state.alive_lease_id,
        cpu,
        memory,
    )
    .await?;
    let (pw_handle, work_pod_tx, kill_pod_tx) = pod_worker::initialise(
        etcd.clone(),
        containers.clone(),
        actual_cluster_address,
        state.alive_lease_id,
        limit_tx,
    );
    let pc_handle = pod_claimer::initialise(
        etcd.clone(),
        node_name.clone(),
        state.alive_lease_id,
        work_pod_tx,
    )
    .await?;
    pod_killer::initialise(etcd, kill_pod_tx).await?;

    let ch = nodelib::wait_for_sigterm(state).await;

    // Terminating pods might take a little time if there are lots of them, so
    // also stop claiming new ones.
    pc_handle.terminate().await;
    pw_handle.terminate().await;
    containers::teardown(&containers).await?;

    nodelib::signal_channel(ch).await;
    process::exit(0)
}

fn read_ip_from_file(p: PathBuf) -> Result<Ipv4Addr, Box<dyn std::error::Error>> {
    let ip = fs::read_to_string(p)?.parse()?;

    Ok(ip)
}
