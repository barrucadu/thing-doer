use clap::Parser;
use rust_decimal::Decimal;
use std::net::{Ipv4Addr, SocketAddr};
use std::process;

use nodelib::etcd;
use nodelib::resources::node::*;

use workerd::cluster_nameserver;
use workerd::limits;
use workerd::pod_claimer;
use workerd::pod_killer;
use workerd::pod_worker;
use workerd::podman;

/// Add an alias record for the current host to `dns.special.cluster.local.`
pub static SPECIAL_HOSTNAME: &str = "dns";

/// thing-doer workerd.
#[derive(Clone, Debug, Parser)]
struct Args {
    /// Name of this instance, must be unique across the cluster.  If
    /// unspecified, a random name is generated.
    #[clap(long)]
    pub name: Option<String>,

    /// Address to bind on to provide services to local pods.  Must be reachable
    /// within the cluster.
    #[clap(long = "cluster-address", value_parser, env = "CLUSTER_ADDRESS")]
    pub address: Ipv4Addr,

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
    pub podman: podman::Config,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().json().init();

    let Args {
        name,
        address,
        external_dns,
        cpu,
        memory,
        etcd,
        podman,
    } = Args::parse();

    let state = nodelib::initialise(
        etcd.clone(),
        name,
        NodeType::Worker,
        NodeSpec {
            address: Some(address),
            limits: Some(NodeLimitSpec { cpu, memory }),
        },
        Some(SPECIAL_HOSTNAME),
    )
    .await?;

    cluster_nameserver::initialise(etcd.clone(), &state.name, address, external_dns).await?;

    let limit_tx = limits::initialise(
        etcd.clone(),
        state.name.clone(),
        state.alive_lease_id,
        cpu,
        memory,
    )
    .await?;
    let (pw_handle, work_pod_tx, kill_pod_tx) = pod_worker::initialise(
        etcd.clone(),
        podman,
        state.name.clone(),
        address,
        state.alive_lease_id,
        limit_tx,
    )
    .await?;
    let pc_handle = pod_claimer::initialise(
        etcd.clone(),
        state.name.clone(),
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

    nodelib::signal_channel(ch).await;
    process::exit(0)
}
