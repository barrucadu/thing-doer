use std::process;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Streaming};

use crate::etcd::config::Config;
use crate::etcd::pb::etcdserverpb::{
    LeaseGrantRequest, LeaseKeepAliveRequest, LeaseKeepAliveResponse, LeaseRevokeRequest,
    PutRequest,
};
use crate::types::{Error, StreamingError};

/// The timeout waiting for etcd to respond to a ping.
pub static PONG_TIMEOUT: u64 = 5;

/// The maximum number of retries in case of failure to renew the lease
pub static MAXIMUM_RETRIES: u32 = 10;

/// Exit code in case the heartbeat process loses connection to etcd and cannot
/// reestablish it.
pub static EXIT_CODE_LEASE_FAILED: i32 = 3;

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

/// Periodically refresh a lease so that its TTL does not expire.
///
/// If a lease cannot be established, or gets dropped and cannot be
/// reestablished, this terminates the process.
pub async fn task(
    config: Config,
    mut expire_rx: oneshot::Receiver<oneshot::Sender<()>>,
    mut lease: Lease,
) {
    let mut retries: u32 = 0;
    loop {
        match task_loop(&config, &mut expire_rx, &lease).await {
            Ok(notify) => {
                tracing::info!(key = lease.key, "revoking lease");
                let revoke_req = LeaseRevokeRequest { id: lease.id.0 };
                let mut revoke_retries: u32 = 0;
                while let Err(error) = try_revoke_request(&config, &revoke_req).await {
                    revoke_retries += 1;
                    if revoke_retries < 3 {
                        tracing::warn!(?error, "could not revoke lease, retrying...");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    } else {
                        tracing::warn!(?error, "could not revoke lease, giving up...");
                        break;
                    }
                }
                let _ = notify.send(());
                return;
            }
            Err((successes, error)) => {
                if successes > 0 {
                    retries = 0;
                } else {
                    retries += 1;
                }

                tracing::warn!(?error, ?retries, "error in etcd lease keepalive request");

                while retries < MAXIMUM_RETRIES {
                    tokio::time::sleep(Duration::from_secs(2_u64.pow(retries))).await;
                    match establish_lease(&config, lease.requested_ttl, lease.key.clone()).await {
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
                    process::exit(EXIT_CODE_LEASE_FAILED);
                }
            }
        }
    }
}

/// Create a lease and associate a key with it.  The value of the key is
/// unimportant.
pub async fn establish_lease(config: &Config, ttl: i64, key: String) -> Result<Lease, Error> {
    let mut kv_client = config.kv_client().await?;
    let mut lease_client = config.lease_client().await?;

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

///////////////////////////////////////////////////////////////////////////////

/// The "periodically refresh a lease so that its TTL does not expire" part of
/// `task`.
///
/// If this returns an error, `task` attempts to re-establish the connection to
/// etcd and calls `task_loop` again.
async fn task_loop<T>(
    config: &Config,
    mut expire_rx: &mut oneshot::Receiver<T>,
    lease: &Lease,
) -> Result<T, (usize, Error)> {
    let (tx, mut response_stream) = setup_heartbeat(config, lease).await.map_err(|e| (0, e))?;

    let mut successes = 0;
    loop {
        let response = wait_for_pong(&mut response_stream)
            .await
            .map_err(|e| (successes, e))?;
        successes += 1;

        let wait = Duration::from_secs((response.ttl / 3) as u64);
        tokio::select! {
            _ = tokio::time::sleep(wait) => {
                send_ping(&tx, lease).await.map_err(|e| (successes, e))?;
            }
            msg = &mut expire_rx => {
                return Ok(msg.unwrap());
            }
        }
    }
}

/// Set up streams for the heartbeat and send the initial ping
async fn setup_heartbeat(
    config: &Config,
    lease: &Lease,
) -> Result<
    (
        mpsc::Sender<LeaseKeepAliveRequest>,
        Streaming<LeaseKeepAliveResponse>,
    ),
    Error,
> {
    let mut lease_client = config.lease_client().await?;

    let (tx, rx) = mpsc::channel(16);
    send_ping(&tx, lease).await?;

    let response_stream = lease_client
        .lease_keep_alive(Request::new(ReceiverStream::new(rx)))
        .await?
        .into_inner();

    Ok((tx, response_stream))
}

/// Send a ping
async fn send_ping(tx: &mpsc::Sender<LeaseKeepAliveRequest>, lease: &Lease) -> Result<(), Error> {
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

/// Attempt a revoke request - `task` retries failures.
async fn try_revoke_request(etcd_config: &Config, req: &LeaseRevokeRequest) -> Result<(), Error> {
    let mut lease_client = etcd_config.lease_client().await?;
    lease_client.lease_revoke(Request::new(req.clone())).await?;

    Ok(())
}
