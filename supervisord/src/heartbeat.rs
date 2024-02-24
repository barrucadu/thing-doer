use grpc_etcd::etcdserverpb::kv_client::KvClient;
use grpc_etcd::etcdserverpb::lease_client::LeaseClient;
use grpc_etcd::etcdserverpb::{LeaseGrantRequest, LeaseKeepAliveRequest, PutRequest};
use std::process;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::transport::Channel;
use tonic::Request;

use crate::args::EtcdConfig;
use crate::{Error, StreamingError};

/// If this time elapses without a heartbeat, this instance enters "unhealthy"
/// state.
pub static HEALTHY_LEASE_TTL: i64 = 30;

/// If this time elapses without a heartbeat, this instance enters "dead" state.
pub static ALIVE_LEASE_TTL: i64 = 300;

/// The maximum number of retries in case of failure to renew the lease
pub static MAXIMUM_RETRIES: u32 = 10;

/// A lease ID and its TTL
#[derive(Debug)]
pub struct Lease {
    pub key: String,
    pub id: i64,
    pub actual_ttl: i64,
    pub requested_ttl: i64,
}

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
    let mut lease_client = etcd_config
        .lease_client()
        .await
        .map_err(|e| (0, e.into()))?;

    let (tx, rx) = mpsc::channel(16);
    tx.send(LeaseKeepAliveRequest { id: lease.id })
        .await
        .map_err(|_| (0, StreamingError::CannotSend.into()))?;

    let mut response_stream = lease_client
        .lease_keep_alive(Request::new(ReceiverStream::new(rx)))
        .await
        .map_err(|e| (0, e.into()))?
        .into_inner();

    let mut successes = 0;
    let mut wait = Duration::from_secs((lease.actual_ttl / 3) as u64);
    loop {
        tracing::info!(lease_key = lease.key, lease_id = lease.id, "ping");
        tx.send(LeaseKeepAliveRequest { id: lease.id })
            .await
            .map_err(|_| (successes, StreamingError::CannotSend.into()))?;

        if let Some(response) = timeout(wait, response_stream.next())
            .await
            .map_err(|_| (successes, StreamingError::TimedOut.into()))?
        {
            let response = response.map_err(|e| (successes, e.into()))?;
            wait = Duration::from_secs((response.ttl / 3) as u64);
            successes += 1;
            tracing::warn!(lease_key = lease.key, lease_id = lease.id, ?wait, "waiting");
            tokio::time::sleep(wait).await;
        } else {
            return Err((successes, StreamingError::Ended.into()));
        }
    }
}

/// Create a new lease based on an existing one.  The existing lease is not
/// deleted.
async fn reestablish_lease(etcd_config: &EtcdConfig, old_lease: &Lease) -> Result<Lease, Error> {
    let mut kv_client = etcd_config.kv_client().await?;
    let mut lease_client = etcd_config.lease_client().await?;

    establish_lease(
        &mut kv_client,
        &mut lease_client,
        old_lease.requested_ttl,
        old_lease.key.clone(),
    )
    .await
}

///////////////////////////////////////////////////////////////////////////////

/// Set up the initial leases for a node.
pub async fn establish_leases(
    etcd_config: &EtcdConfig,
    name: &str,
) -> Result<(Lease, Lease), Error> {
    let mut kv_client = etcd_config.kv_client().await?;
    let mut lease_client = etcd_config.lease_client().await?;

    let healthy_lease = establish_lease(
        &mut kv_client,
        &mut lease_client,
        HEALTHY_LEASE_TTL,
        format!("node/{name}/healthy"),
    )
    .await?;

    let alive_lease = establish_lease(
        &mut kv_client,
        &mut lease_client,
        ALIVE_LEASE_TTL,
        format!("node/{name}/alive"),
    )
    .await?;

    Ok((healthy_lease, alive_lease))
}

/// Create a lease and associate a key with it.  The value of the key is
/// unimportant.
async fn establish_lease(
    kv_client: &mut KvClient<Channel>,
    lease_client: &mut LeaseClient<Channel>,
    ttl: i64,
    key: String,
) -> Result<Lease, Error> {
    let grant = lease_client
        .lease_grant(Request::new(LeaseGrantRequest { id: 0, ttl }))
        .await?
        .into_inner();

    if !grant.error.is_empty() {
        return Err(Error::EtcdResponse(grant.error));
    }

    let lease = Lease {
        key,
        id: grant.id,
        actual_ttl: grant.ttl,
        requested_ttl: ttl,
    };

    set_or_claim_key(kv_client, &lease).await?;

    Ok(lease)
}

/// Write to the lease's key, and claim it for the lease.
///
/// This overwrites any previous claim by another lease.
async fn set_or_claim_key(kv_client: &mut KvClient<Channel>, lease: &Lease) -> Result<(), Error> {
    kv_client
        .put(Request::new(PutRequest {
            key: lease.key.clone().into(),
            value: b"lease-established".into(),
            lease: lease.id,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
        }))
        .await?;

    Ok(())
}
