use grpc_supervisord::worker::worker_client::WorkerClient;
use std::net::SocketAddr;
use std::time::Duration;
use tonic::transport;

/// thing-doer workerd
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
    pub supervisord_config: SupervisordConfig,
}

#[derive(Clone, Debug, clap::Args)]
pub struct SupervisordConfig {
    /// supervisord gRPC endpoints
    #[clap(long, value_parser, value_delimiter = ',', env = "SUPERVISORD_HOSTS")]
    pub supervisord_hosts: Vec<SocketAddr>,

    /// Timeout (in seconds) for connecting to supervisord
    #[clap(long, value_parser = |secs: &str| secs.parse().map(Duration::from_secs), default_value="2", env="SUPERVISORD_CONNECT_TIMEOUT")]
    pub supervisord_connect_timeout: Duration,
}

impl SupervisordConfig {
    /// Connect to supervisord by trying each host in turn
    pub async fn connect(&self) -> Result<transport::Channel, transport::Error> {
        let mut iter = self.supervisord_hosts.iter().peekable();
        while let Some(addr) = iter.next() {
            match transport::Endpoint::new(format!("http://{addr}")) {
                Ok(endpoint) => match endpoint
                    .connect_timeout(self.supervisord_connect_timeout)
                    .connect()
                    .await
                {
                    Ok(client) => return Ok(client),
                    Err(error) => {
                        tracing::warn!(?addr, ?error, "could not connect to supervisord endpoint");
                        if iter.peek().is_none() {
                            tracing::error!("connection to all supervisord hosts has failed");
                            return Err(error);
                        }
                    }
                },
                Err(error) => {
                    tracing::warn!(?addr, ?error, "could not connect to supervisord endpoint");
                    if iter.peek().is_none() {
                        tracing::error!("connection to all supervisord hosts has failed");
                        return Err(error);
                    }
                }
            }
        }

        // `self.supervisord_hosts` is guaranteed to be nonempty
        unreachable!();
    }

    /// Obtain a worker client by trying each supervisord host in turn.
    pub async fn worker_client(
        &self,
    ) -> Result<WorkerClient<transport::Channel>, transport::Error> {
        let conn = self.connect().await?;
        Ok(WorkerClient::new(conn))
    }
}
