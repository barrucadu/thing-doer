use grpc_supervisord::worker::worker_server::Worker;
use grpc_supervisord::worker::{
    HeartbeatRequest, HeartbeatResponse, RegisterRequest, RegisterResponse,
};
use serde_json::json;
use tonic::{Request, Response, Status};

use crate::args::EtcdConfig;
use crate::etcd::leaser;
use crate::heartbeat;
use crate::resources;
use crate::{Error, ResourceError};

/// Worker heartbeat TTL.
pub static HEARTBEAT_TTL: i64 = heartbeat::HEALTHY_LEASE_TTL - 5;

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

    let (healthy_lease, alive_lease) =
        heartbeat::establish_leases(etcd_config, &request.name).await?;

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

    let healthy_lease = leaser::Lease {
        key: heartbeat::healthy_lease_key(etcd_config, &request.name),
        id: leaser::LeaseId(request.worker_id[0]),
        requested_ttl: heartbeat::HEALTHY_LEASE_TTL,
        actual_ttl: heartbeat::HEALTHY_LEASE_TTL,
    };

    let alive_lease = leaser::Lease {
        key: heartbeat::alive_lease_key(etcd_config, &request.name),
        id: leaser::LeaseId(request.worker_id[1]),
        requested_ttl: heartbeat::ALIVE_LEASE_TTL,
        actual_ttl: heartbeat::ALIVE_LEASE_TTL,
    };

    if !check_and_renew_lease(etcd_config, &alive_lease).await? {
        tracing::info!(name = request.name, "got heartbeat from dead worker");
        return Err(HandleHeartbeatError::Expired);
    }

    tracing::info!(name = request.name, "got heartbeat from live worker");

    if check_and_renew_lease(etcd_config, &healthy_lease).await? {
        Ok(HeartbeatResponse {
            worker_id: [healthy_lease.id.0, alive_lease.id.0].into(),
            heartbeat_ttl: HEARTBEAT_TTL,
        })
    } else {
        // healthy lease expired and must be recreated
        let new_healthy_lease = leaser::reestablish_lease(etcd_config, &healthy_lease).await?;
        Ok(HeartbeatResponse {
            worker_id: [new_healthy_lease.id.0, alive_lease.id.0].into(),
            heartbeat_ttl: HEARTBEAT_TTL,
        })
    }
}

/// Check if a lease is still valid, and renew it if so.  Returns the lease on
/// success.
async fn check_and_renew_lease(
    etcd_config: &EtcdConfig,
    lease: &leaser::Lease,
) -> Result<bool, Error> {
    // unfortunately, we can't check the lease still exists and also renew it in
    // the same transaction, so we need to check, renew, and then check that it
    // didn't elapse between the first two steps.
    if leaser::is_lease_still_active(etcd_config, lease).await? {
        let _ = leaser::ping_and_wait_for_pong(etcd_config, lease).await?;
        let res = leaser::is_lease_still_active(etcd_config, lease).await?;
        Ok(res)
    } else {
        Ok(false)
    }
}
