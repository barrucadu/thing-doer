use grpc_supervisord::worker::worker_server::Worker;
use grpc_supervisord::worker::{
    HeartbeatRequest, HeartbeatResponse, RegisterRequest, RegisterResponse,
};
use serde_json::json;
use tonic::{Request, Response, Status};

use crate::args::EtcdConfig;
use crate::heartbeat::{
    establish_leases, lookup_lease, ping_and_wait_for_pong, reestablish_lease, Lease, LeaseId,
    ALIVE_LEASE_TTL, HEALTHY_LEASE_TTL,
};
use crate::resources;
use crate::{Error, ResourceError};

/// Worker heartbeat TTL.
pub static HEARTBEAT_TTL: i64 = HEALTHY_LEASE_TTL - 5;

/// A server for the worker gRPC service.
#[derive(Debug)]
pub struct Server {
    pub etcd_config: EtcdConfig,
}

impl Server {
    /// Construct a new server instance.
    pub fn new(etcd_config: EtcdConfig) -> Self {
        Self { etcd_config }
    }
}

#[tonic::async_trait]
impl Worker for Server {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        match handle_register(&self.etcd_config, request.into_inner()).await {
            Ok(response) => Ok(Response::new(response)),
            Err(Error::Resource(ResourceError::BadName)) => Err(Status::invalid_argument(
                "name must be a valid DNS label".to_string(),
            )),
            Err(error) => Err(Status::internal(format!("{error:?}"))),
        }
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        match handle_heartbeat(&self.etcd_config, request.into_inner()).await {
            Ok(response) => Ok(Response::new(response)),
            Err(HandleHeartbeatError::WorkerId) => Err(Status::invalid_argument(
                "worker_id is malformed".to_string(),
            )),
            Err(HandleHeartbeatError::Expired) => {
                Err(Status::not_found("worker_id has expired".to_string()))
            }
            Err(HandleHeartbeatError::Error(error)) => Err(Status::internal(format!("{error:?}"))),
        }
    }
}

/// Private implementation of `Server::register`
async fn handle_register(
    etcd_config: &EtcdConfig,
    request: RegisterRequest,
) -> Result<RegisterResponse, Error> {
    tracing::info!(
        name = request.name,
        address = request.address,
        "got worker register request"
    );

    let spec = json!({
        "type": "node.worker",
        "name": request.name,
        "spec": {
            "address": request.address,
        },
    });
    resources::put(etcd_config, spec).await?;

    let (healthy_lease, alive_lease) = establish_leases(etcd_config, &request.name).await?;

    Ok(RegisterResponse {
        worker_id: [healthy_lease.id.0, alive_lease.id.0].into(),
        heartbeat_ttl: HEARTBEAT_TTL,
    })
}

enum HandleHeartbeatError {
    WorkerId,
    Expired,
    Error(Error),
}

impl From<Error> for HandleHeartbeatError {
    fn from(error: Error) -> Self {
        Self::Error(error)
    }
}

/// Private implementation of `Server::heartbeat`
async fn handle_heartbeat(
    etcd_config: &EtcdConfig,
    request: HeartbeatRequest,
) -> Result<HeartbeatResponse, HandleHeartbeatError> {
    if request.worker_id.len() != 2 {
        return Err(HandleHeartbeatError::WorkerId);
    }

    let healthy_lease_id = LeaseId(request.worker_id[0]);
    let alive_lease_id = LeaseId(request.worker_id[1]);

    if let Some(alive_lease_key) =
        check_and_renew_lease(etcd_config, alive_lease_id, ALIVE_LEASE_TTL).await?
    {
        tracing::info!(name = alive_lease_key, "got heartbeat from live worker");
        if check_and_renew_lease(etcd_config, healthy_lease_id, HEALTHY_LEASE_TTL)
            .await?
            .is_some()
        {
            // both leases were still valid
            Ok(HeartbeatResponse {
                worker_id: [healthy_lease_id.0, alive_lease_id.0].into(),
                heartbeat_ttl: HEARTBEAT_TTL,
            })
        } else {
            // healthy lease expired and must be recreated
            let healthy_lease = reestablish_lease(
                etcd_config,
                &Lease {
                    key: alive_lease_key.replace("/alive", "/healthy"),
                    id: healthy_lease_id,
                    actual_ttl: HEALTHY_LEASE_TTL,
                    requested_ttl: HEALTHY_LEASE_TTL,
                },
            )
            .await?;
            Ok(HeartbeatResponse {
                worker_id: [healthy_lease.id.0, alive_lease_id.0].into(),
                heartbeat_ttl: HEARTBEAT_TTL,
            })
        }
    } else {
        // worker is considered dead
        tracing::info!("got heartbeat from dead worker");
        Err(HandleHeartbeatError::Expired)
    }
}

/// Check if a lease is still valid, and renew it if so.  Returns the lease on
/// success.
async fn check_and_renew_lease(
    etcd_config: &EtcdConfig,
    lease_id: LeaseId,
    ttl: i64,
) -> Result<Option<String>, Error> {
    // unfortunately, we can't check the lease still exists and also renew it in
    // the same transaction, so we need to check, renew, and then check that it
    // didn't elapse between the first two steps.
    if let Some(lease_key) = lookup_lease(etcd_config, lease_id).await? {
        let _ = ping_and_wait_for_pong(
            etcd_config,
            &Lease {
                key: lease_key,
                id: lease_id,
                actual_ttl: ttl,
                requested_ttl: ttl,
            },
        )
        .await?;
        let res = lookup_lease(etcd_config, lease_id).await?;
        Ok(res)
    } else {
        Ok(None)
    }
}
