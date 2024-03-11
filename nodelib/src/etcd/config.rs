use std::net::SocketAddr;
use std::time::Duration;
use tonic::transport;

use crate::etcd::pb::etcdserverpb::kv_client::KvClient;
use crate::etcd::pb::etcdserverpb::lease_client::LeaseClient;
use crate::etcd::pb::etcdserverpb::watch_client::WatchClient;

#[derive(Clone, Debug, clap::Args)]
pub struct Config {
    /// etcd gRPC endpoints
    #[clap(
        long = "etcd-hosts",
        value_parser,
        value_delimiter = ',',
        default_value = "127.0.0.1:2379",
        env = "ETCD_HOSTS"
    )]
    pub hosts: Vec<SocketAddr>,

    /// Timeout (in seconds) for connecting to etcd
    #[clap(
        long = "etcd-connect-timeout",
        value_parser = |secs: &str| secs.parse().map(Duration::from_secs),
        default_value = "2",
        env = "ETCD_CONNECT_TIMEOUT",
    )]
    pub connect_timeout: Duration,

    /// Prefix to store etcd keys under.
    #[clap(
        long = "etcd-prefix",
        default_value = "/thing-doer",
        env = "ETCD_PREFIX"
    )]
    pub prefix: String,
}

impl Config {
    /// Connect to etcd by trying each host in turn
    pub async fn connect(&self) -> Result<transport::Channel, transport::Error> {
        let mut iter = self.hosts.iter().peekable();
        while let Some(addr) = iter.next() {
            match transport::Endpoint::new(format!("http://{addr}")) {
                Ok(endpoint) => match endpoint
                    .connect_timeout(self.connect_timeout)
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

        // `self.hosts` is guaranteed to be nonempty
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

    /// Obtain a watch client by trying each etcd host in turn.
    pub async fn watch_client(&self) -> Result<WatchClient<transport::Channel>, transport::Error> {
        let conn = self.connect().await?;
        Ok(WatchClient::new(conn))
    }
}
