use std::net::Ipv4Addr;
use std::process;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::Request;

use nodelib::error::*;
use nodelib::etcd;
use nodelib::etcd::leaser::LeaseId;
use nodelib::etcd::pb::etcdserverpb::{DeleteRangeRequest, PutRequest};
use nodelib::etcd::prefix;
use nodelib::resources::pod::*;

use crate::limits;
use crate::podman;

/// Exit code in case the worker channel closes.
pub static EXIT_CODE_WORKER_FAILED: i32 = 1;

/// Start background tasks to work pods.
pub async fn initialise(
    etcd_config: etcd::Config,
    podman_config: podman::Config,
    my_name: String,
    my_ip: Ipv4Addr,
    lease_id: LeaseId,
    limit_state: limits::State,
) -> Result<Sender<PodResource>, Error> {
    let (work_pod_tx, work_pod_rx) = mpsc::channel(128);

    tokio::spawn(work_task(
        etcd_config,
        podman_config,
        my_name,
        my_ip,
        lease_id,
        limit_state,
        work_pod_rx,
    ));

    Ok(work_pod_tx)
}

/// Background task to work pods.
async fn work_task(
    etcd_config: etcd::Config,
    podman_config: podman::Config,
    my_name: String,
    my_ip: Ipv4Addr,
    lease_id: LeaseId,
    limit_state: limits::State,
    mut work_pod_rx: Receiver<PodResource>,
) {
    while let Some(pod) = work_pod_rx.recv().await {
        tracing::info!(pod_name = pod.name, "spawned worker task");
        tokio::spawn(work_pod(
            etcd_config.clone(),
            podman_config.clone(),
            my_name.clone(),
            my_ip,
            lease_id,
            limit_state.clone(),
            pod,
        ));
    }

    tracing::error!("worker channel unexpectedly closed, termianting...");
    process::exit(EXIT_CODE_WORKER_FAILED);
}

/// Proof-of-concept pod-worker
async fn work_pod(
    etcd_config: etcd::Config,
    podman_config: podman::Config,
    my_name: String,
    my_ip: Ipv4Addr,
    lease_id: LeaseId,
    mut limit_state: limits::State,
    pod: PodResource,
) {
    // for logging
    let pod_name = pod.name.clone();

    let committed = limit_state.claim_resources(&pod).await;
    if committed.is_none() {
        tracing::warn!(pod_name, "overcommitted resources");
    }

    put_request(
        &etcd_config,
        pod.clone()
            .with_state(PodState::Running)
            .to_put_request(&etcd_config),
    )
    .await;

    let state = match run_pod_process(
        &etcd_config,
        &podman_config,
        &my_name,
        my_ip,
        lease_id,
        &pod,
    )
    .await
    {
        Ok(Some(true)) => PodState::ExitSuccess,
        Ok(Some(false)) => PodState::ExitFailure,
        Ok(None) => {
            tracing::warn!(pod_name, "could not initialise pod");
            PodState::Errored
        }
        Err(error) => {
            tracing::warn!(pod_name, ?error, "pod errored");
            PodState::Errored
        }
    };

    put_request(
        &etcd_config,
        pod.with_state(state).to_put_request(&etcd_config),
    )
    .await;

    delete_request(
        &etcd_config,
        DeleteRangeRequest {
            key: format!(
                "{prefix}{pod_name}",
                prefix = prefix::claimed_pods(&etcd_config)
            )
            .into(),
            ..Default::default()
        },
    )
    .await;

    if let Some(to_free) = committed {
        limit_state.release_resources(to_free).await;
    }
}

/// Run the pod process and return whether it exited successfully or not.
async fn run_pod_process(
    etcd_config: &etcd::Config,
    podman_config: &podman::Config,
    my_name: &str,
    my_ip: Ipv4Addr,
    lease_id: LeaseId,
    pod: &PodResource,
) -> std::io::Result<Option<bool>> {
    match podman::create_pod(podman_config, my_name, my_ip, pod).await? {
        Some(pod_state) => {
            tracing::info!(name=pod_state.name, address=?pod_state.address, "created pod");
            create_pod_dns_record(etcd_config, lease_id, &pod_state).await;

            for container in &pod.spec.containers {
                if !podman::start_container(podman_config, &pod_state, container).await? {
                    delete_pod_dns_record(etcd_config, &pod_state).await;
                    podman::terminate_pod(podman_config, &pod_state).await?;
                    return Ok(None);
                }
            }

            let exit_success = podman::wait_for_containers(podman_config, &pod_state).await?;
            delete_pod_dns_record(etcd_config, &pod_state).await;
            podman::terminate_pod(podman_config, &pod_state).await?;
            Ok(Some(exit_success))
        }
        None => Ok(None),
    }
}

/// Write the key for the DNS record of the pod.
async fn create_pod_dns_record(
    etcd_config: &etcd::Config,
    lease_id: LeaseId,
    pod_state: &podman::PodState,
) {
    let key = format!(
        "{prefix}{pod_name}.pod.cluster.local.",
        prefix = prefix::domain_name(etcd_config),
        pod_name = pod_state.hostname,
    );
    let req = PutRequest {
        key: key.into(),
        value: pod_state.address.to_string().into(),
        lease: lease_id.0,
        ..Default::default()
    };

    put_request(etcd_config, req).await;
}

/// Delete the key for the DNS record of the pod.
async fn delete_pod_dns_record(etcd_config: &etcd::Config, pod_state: &podman::PodState) {
    let key = format!(
        "{prefix}{pod_name}.pod.cluster.local.",
        prefix = prefix::domain_name(etcd_config),
        pod_name = pod_state.hostname,
    );
    let req = DeleteRangeRequest {
        key: key.into(),
        ..Default::default()
    };

    delete_request(etcd_config, req).await;
}

///////////////////////////////////////////////////////////////////////////////

/// Retry a put request until it succeeds.
async fn put_request(etcd_config: &etcd::Config, req: PutRequest) {
    while let Err(error) = try_put_request(etcd_config, req.clone()).await {
        tracing::warn!(?error, "could not put record, retrying...");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Attempt a put request.
async fn try_put_request(etcd_config: &etcd::Config, req: PutRequest) -> Result<(), Error> {
    let mut kv_client = etcd_config.kv_client().await?;
    kv_client.put(Request::new(req)).await?;

    Ok(())
}

/// Retry a delete request until it succeeds.
async fn delete_request(etcd_config: &etcd::Config, req: DeleteRangeRequest) {
    while let Err(error) = try_delete_request(etcd_config, req.clone()).await {
        tracing::warn!(?error, "could not delete record, retrying...");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Attempt a delete request.
async fn try_delete_request(
    etcd_config: &etcd::Config,
    req: DeleteRangeRequest,
) -> Result<(), Error> {
    let mut kv_client = etcd_config.kv_client().await?;
    kv_client.delete_range(Request::new(req)).await?;

    Ok(())
}
