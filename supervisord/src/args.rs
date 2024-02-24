use grpc_etcd::etcdserverpb::{kv_client::KvClient, lease_client::LeaseClient};
use std::net::SocketAddr;
use std::time::Duration;
use tonic::transport;

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

#[derive(Clone, Debug, clap::Args)]
pub struct EtcdConfig {
    /// etcd gRPC endpoints
    #[clap(
        long,
        value_parser,
        value_delimiter = ',',
        default_value = "127.0.0.1:2379",
        env = "ETCD_HOSTS"
    )]
    pub etcd_hosts: Vec<SocketAddr>,

    /// Timeout (in seconds) for connecting to etcd
    #[clap(long, value_parser = |secs: &str| secs.parse().map(Duration::from_secs), default_value="2", env="ETCD_CONNECT_TIMEOUT")]
    pub etcd_connect_timeout: Duration,
}

impl EtcdConfig {
    /// Connect to etcd by trying each host in turn
    pub async fn connect(&self) -> Result<transport::Channel, transport::Error> {
        let mut iter = self.etcd_hosts.iter().peekable();
        while let Some(addr) = iter.next() {
            match transport::Endpoint::new(format!("http://{addr}")) {
                Ok(endpoint) => match endpoint
                    .connect_timeout(self.etcd_connect_timeout)
                    .connect()
                    .await
                {
                    Ok(client) => return Ok(client),
                    Err(error) => {
                        tracing::warn!(?addr, ?error, "could not connect to etcd endpoint");
                        if iter.peek().is_none() {
                            tracing::error!("connection to all etcd hosts has failed");
                            return Err(error);
                        }
                    }
                },
                Err(error) => {
                    tracing::warn!(?addr, ?error, "could not connect to etcd endpoint");
                    if iter.peek().is_none() {
                        tracing::error!("connection to all etcd hosts has failed");
                        return Err(error);
                    }
                }
            }
        }

        // `self.etcd_hosts` is guaranteed to be nonempty
        unreachable!();
    }

    /// Obtain a kv client by trying each etcd host in turn.
    pub async fn kv_client(&self) -> Result<KvClient<transport::Channel>, transport::Error> {
        let conn = self.connect().await?;
        Ok(KvClient::new(conn))
    }

    /// Obtain a lease client by trying each etcd host in turn.
    pub async fn lease_client(&self) -> Result<LeaseClient<transport::Channel>, transport::Error> {
        let conn = self.connect().await?;
        Ok(LeaseClient::new(conn))
    }
}
