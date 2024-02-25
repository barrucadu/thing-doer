use grpc_supervisord::worker::{HeartbeatRequest, RegisterRequest};
use std::net::SocketAddr;
use std::time::Duration;
use tonic::Request;

use crate::args::SupervisordConfig;

/// Register with a supervisord and send periodic heartbeats.
pub async fn task(
    supervisord_config: SupervisordConfig,
    name: String,
    advertise_address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!(?name, ?advertise_address, "registering with supervisord");

    let mut client = supervisord_config.worker_client().await?;
    let res = client
        .register(Request::new(RegisterRequest {
            name: name.clone(),
            address: format!("{advertise_address}"),
        }))
        .await?
        .into_inner();

    let mut worker_id = res.worker_id;
    let mut heartbeat_ttl = res.heartbeat_ttl;
    loop {
        let wait = Duration::from_secs(heartbeat_ttl as u64 / 2);
        tracing::info!(?wait, "waiting");
        tokio::time::sleep(wait).await;

        tracing::info!("ping");
        let res = client
            .heartbeat(Request::new(HeartbeatRequest { worker_id }))
            .await?
            .into_inner();
        worker_id = res.worker_id;
        heartbeat_ttl = res.heartbeat_ttl;
    }
}
