use tonic::Request;

use crate::etcd;
use crate::etcd::leaser;
use crate::etcd::pb::etcdserverpb::RangeRequest;
use crate::etcd::prefix;
use crate::resources::node::NodeType;
use crate::Error;

/// If this time elapses without a heartbeat, this instance enters "unhealthy"
/// state.
pub static HEALTHY_LEASE_TTL: i64 = 30;

/// If this time elapses without a heartbeat, this instance enters "dead" state.
pub static ALIVE_LEASE_TTL: i64 = 300;

/// Set up the initial health / liveness leases for a node.
pub async fn establish_leases(
    etcd_config: &etcd::Config,
    node_type: NodeType,
    name: &str,
) -> Result<(leaser::Lease, leaser::Lease), Error> {
    let healthy_lease = leaser::establish_lease(
        etcd_config,
        HEALTHY_LEASE_TTL,
        format!(
            "{prefix}{name}",
            prefix = prefix::node_heartbeat_healthy(etcd_config, node_type)
        ),
    )
    .await?;

    let alive_lease = leaser::establish_lease(
        etcd_config,
        ALIVE_LEASE_TTL,
        format!(
            "{prefix}{name}",
            prefix = prefix::node_heartbeat_alive(etcd_config, node_type)
        ),
    )
    .await?;

    Ok((healthy_lease, alive_lease))
}

/// Check if a node is still alive.
pub async fn is_alive(
    etcd_config: &etcd::Config,
    node_type: NodeType,
    name: &str,
) -> Result<bool, Error> {
    let mut kv_client = etcd_config.kv_client().await?;

    let response = kv_client
        .range(Request::new(RangeRequest {
            key: format!(
                "{prefix}{name}",
                prefix = prefix::node_heartbeat_alive(etcd_config, node_type)
            )
            .into(),
            limit: 1,
            ..Default::default()
        }))
        .await?
        .into_inner();

    Ok(response.count == 1)
}
