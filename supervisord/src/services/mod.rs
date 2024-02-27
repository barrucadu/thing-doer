pub mod worker;

use grpc_supervisord::worker::worker_server::WorkerServer;
use std::net::SocketAddr;
use tonic::transport::Server;

use crate::etcd;
use crate::Error;

/// Start and run the gRPC server.
pub async fn task(etcd_config: etcd::Config, listen_address: SocketAddr) -> Result<(), Error> {
    let worker = crate::services::worker::Server::new(etcd_config);

    Server::builder()
        .add_service(WorkerServer::new(worker))
        .serve(listen_address)
        .await?;

    Ok(())
}
