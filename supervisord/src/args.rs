use std::net::SocketAddr;

pub use crate::etcd::config::Config as EtcdConfig;

/// thing-doer supervisord
#[derive(Clone, Debug, clap::Parser)]
pub struct Args {
    /// Name of this instance, must be unique across the cluster
    #[clap(long)]
    pub name: String,

    /// Address to listen for new connections on.
    #[clap(long, value_parser, env = "LISTEN_ADDRESS")]
    pub listen_address: SocketAddr,

    /// Address to advertise to the rest of the cluster.  If unspecified, the
    /// listen address is used.
    #[clap(long, value_parser, env = "ADVERTISE_ADDRESS")]
    pub advertise_address: Option<SocketAddr>,

    #[command(flatten)]
    pub etcd_config: EtcdConfig,
}
