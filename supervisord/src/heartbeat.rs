use std::process;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::time::timeout;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Streaming};

use crate::args::EtcdConfig;
use crate::etcd::pb::etcdserverpb::{
    LeaseGrantRequest, LeaseKeepAliveRequest, LeaseKeepAliveResponse, PutRequest, RangeRequest,
};
use crate::{Error, StreamingError};

/// If this time elapses without a heartbeat, this instance enters "unhealthy"
/// state.
pub static HEALTHY_LEASE_TTL: i64 = 30;

/// If this time elapses without a heartbeat, this instance enters "dead" state.
pub static ALIVE_LEASE_TTL: i64 = 300;

/// The timeout waiting for etcd to respond to a ping.
pub static PONG_TIMEOUT: u64 = 5;

/// The maximum number of retries in case of failure to renew the lease
pub static MAXIMUM_RETRIES: u32 = 10;

/// A lease ID and its TTL
#[derive(Debug)]
pub struct Lease {
    pub key: String,
    pub id: LeaseId,
    pub actual_ttl: i64,
    pub requested_ttl: i64,
}

/// A lease ID is just a i64
#[derive(Debug, Clone, Copy)]
pub struct LeaseId(pub i64);

///////////////////////////////////////////////////////////////////////////////

/// Periodically refresh a lease so that its TTL does not expire.
///
/// If a connection cannot be established to any of the configured etcd hosts,
/// this raises an error and terminates the process.
pub async fn task(etcd_config: EtcdConfig, mut lease: Lease) {
    let mut retries: u32 = 0;
    while let Err((successes, error)) = task_loop(&etcd_config, &lease).await {
        if successes > 0 {
            retries = 0;
        } else {
            retries += 1;
        }

        tracing::warn!(?error, ?retries, "error in etcd lease keepalive request");

        while retries < MAXIMUM_RETRIES {
            tokio::time::sleep(Duration::from_secs(2_u64.pow(retries))).await;
            match reestablish_lease(&etcd_config, &lease).await {
                Ok(new_lease) => {
                    tracing::info!("etcd connection reestablished");
                    lease = new_lease;
                    break;
                }
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        ?retries,
                        "could not reestablish connection to etcd, retrying..."
                    );
                    retries += 1;
                }
            }
        }
        if retries == MAXIMUM_RETRIES {
            tracing::error!(
                ?error,
                ?retries,
                "could not reestablish connection to etcd, terminating..."
            );
            process::exit(crate::EXIT_CODE_HEARTBEAT_FAILED);
        }
    }

    // The above is an infinite loop.
    unreachable!();
}

/// The "periodically refresh a lease so that its TTL does not expire" part of
/// `task`.
///
/// If this returns an error, `task` attempts to re-establish the connection to
/// etcd and calls `task_loop` again.
async fn task_loop(etcd_config: &EtcdConfig, lease: &Lease) -> Result<(), (usize, Error)> {
    let (tx, mut response_stream) = setup_heartbeat(etcd_config, lease)
        .await
        .map_err(|e| (0, e))?;

    let mut successes = 0;
    loop {
        let response = wait_for_pong(&mut response_stream)
            .await
            .map_err(|e| (successes, e))?;
        successes += 1;

        let wait = Duration::from_secs((response.ttl / 3) as u64);
        tracing::info!(
            lease_key = lease.key,
            lease_id = lease.id.0,
            ?wait,
            "waiting"
        );
        tokio::time::sleep(wait).await;

        send_ping(&tx, lease).await.map_err(|e| (successes, e))?;
    }
}

/// Set up streams for the heartbeat and send the initial ping
async fn setup_heartbeat(
    etcd_config: &EtcdConfig,
    lease: &Lease,
) -> Result<
    (
        Sender<LeaseKeepAliveRequest>,
        Streaming<LeaseKeepAliveResponse>,
    ),
    Error,
> {
    let mut lease_client = etcd_config.lease_client().await?;

    let (tx, rx) = mpsc::channel(16);
    send_ping(&tx, lease).await?;

    let response_stream = lease_client
        .lease_keep_alive(Request::new(ReceiverStream::new(rx)))
        .await?
        .into_inner();

    Ok((tx, response_stream))
}

/// Send a ping
async fn send_ping(tx: &Sender<LeaseKeepAliveRequest>, lease: &Lease) -> Result<(), Error> {
    tracing::info!(lease_key = lease.key, lease_id = lease.id.0, "ping");
    tx.send(LeaseKeepAliveRequest { id: lease.id.0 })
        .await
        .map_err(|_| Error::Streaming(StreamingError::CannotSend))
}

/// Wait for the pong
async fn wait_for_pong(
    response_stream: &mut Streaming<LeaseKeepAliveResponse>,
) -> Result<LeaseKeepAliveResponse, Error> {
    match timeout(Duration::from_secs(PONG_TIMEOUT), response_stream.next()).await {
        Ok(Some(Ok(response))) => Ok(response),
        Ok(Some(Err(error))) => Err(error.into()),
        Ok(None) => Err(Error::Streaming(StreamingError::Ended)),
        Err(_) => Err(Error::Streaming(StreamingError::TimedOut)),
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Send a single heartbeat for a lease, rather than starting an infinite loop.
/// Returns the new TTL.
pub async fn ping_and_wait_for_pong(etcd_config: &EtcdConfig, lease: &Lease) -> Result<i64, Error> {
    let (_, mut response_stream) = setup_heartbeat(etcd_config, lease).await?;
    let response = wait_for_pong(&mut response_stream).await?;

    Ok(response.ttl)
}

///////////////////////////////////////////////////////////////////////////////

/// Set up the initial health / liveness leases for a node.
pub async fn establish_leases(
    etcd_config: &EtcdConfig,
    name: &str,
) -> Result<(Lease, Lease), Error> {
    let healthy_lease = establish_lease(
        etcd_config,
        HEALTHY_LEASE_TTL,
        healthy_lease_key(etcd_config, name),
    )
    .await?;

    let alive_lease = establish_lease(
        etcd_config,
        ALIVE_LEASE_TTL,
        alive_lease_key(etcd_config, name),
    )
    .await?;

    Ok((healthy_lease, alive_lease))
}

/// Create a new lease based on an existing one.  The existing lease is not
/// deleted.
pub async fn reestablish_lease(
    etcd_config: &EtcdConfig,
    old_lease: &Lease,
) -> Result<Lease, Error> {
    establish_lease(etcd_config, old_lease.requested_ttl, old_lease.key.clone()).await
}

/// Check if a key is still owned by a lease.
pub async fn is_lease_still_active(etcd_config: &EtcdConfig, lease: &Lease) -> Result<bool, Error> {
    let mut kv_client = etcd_config.kv_client().await?;

    let response = kv_client
        .range(Request::new(RangeRequest {
            key: lease.key.clone().into(),
            limit: 1,
            ..Default::default()
        }))
        .await?
        .into_inner();

    Ok(response.count == 1 && response.kvs[0].lease == lease.id.0)
}

/// Create a lease and associate a key with it.  The value of the key is
/// unimportant.
async fn establish_lease(etcd_config: &EtcdConfig, ttl: i64, key: String) -> Result<Lease, Error> {
    let mut kv_client = etcd_config.kv_client().await?;
    let mut lease_client = etcd_config.lease_client().await?;

    let grant = lease_client
        .lease_grant(Request::new(LeaseGrantRequest { id: 0, ttl }))
        .await?
        .into_inner();
    let id = LeaseId(grant.id);

    if !grant.error.is_empty() {
        return Err(Error::EtcdResponse(grant.error));
    }

    kv_client
        .put(Request::new(PutRequest {
            key: key.clone().into(),
            value: b"lease-established".into(),
            lease: grant.id,
            ..Default::default()
        }))
        .await?;

    Ok(Lease {
        key,
        id,
        actual_ttl: grant.ttl,
        requested_ttl: ttl,
    })
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
