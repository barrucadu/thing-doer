use crate::args::EtcdConfig;
use crate::etcd::leaser;
use crate::Error;

/// If this time elapses without a heartbeat, this instance enters "unhealthy"
/// state.
pub static HEALTHY_LEASE_TTL: i64 = 30;

/// If this time elapses without a heartbeat, this instance enters "dead" state.
pub static ALIVE_LEASE_TTL: i64 = 300;

/// Set up the initial health / liveness leases for a node.
pub async fn establish_leases(
    etcd_config: &EtcdConfig,
    name: &str,
) -> Result<(leaser::Lease, leaser::Lease), Error> {
    let healthy_lease = leaser::establish_lease(
        etcd_config,
        HEALTHY_LEASE_TTL,
        healthy_lease_key(etcd_config, name),
    )
    .await?;

    let alive_lease = leaser::establish_lease(
        etcd_config,
        ALIVE_LEASE_TTL,
        alive_lease_key(etcd_config, name),
    )
    .await?;

    Ok((healthy_lease, alive_lease))
}

/// The key to use for a node's "healthy" lease.
pub fn healthy_lease_key(etcd_config: &EtcdConfig, name: &str) -> String {
    format!(
        "{prefix}/node/heartbeat/healthy/{name}",
        prefix = etcd_config.prefix
    )
}

/// The key to use for a node's "alive" lease.
pub fn alive_lease_key(etcd_config: &EtcdConfig, name: &str) -> String {
    format!(
        "{prefix}/node/heartbeat/alive/{name}",
        prefix = etcd_config.prefix
    )
}
