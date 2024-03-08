use std::process;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::Request;

use nodelib::etcd;
use nodelib::etcd::pb::etcdserverpb::{DeleteRangeRequest, PutRequest};
use nodelib::etcd::prefix;
use nodelib::resources::PodResource;
use nodelib::types::{Error, PodState};

use crate::limits;
use crate::podman;

/// Exit code in case the worker channel closes.
pub static EXIT_CODE_WORKER_FAILED: i32 = 1;

/// Start background tasks to work pods.
pub async fn initialise(
    etcd_config: etcd::Config,
    podman_config: podman::Config,
    my_name: String,
    limit_state: limits::State,
) -> Result<Sender<PodResource>, Error> {
    let (work_pod_tx, work_pod_rx) = mpsc::channel(128);

    tokio::spawn(work_task(
        etcd_config,
        podman_config,
        my_name,
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
    limit_state: limits::State,
    mut work_pod_rx: Receiver<PodResource>,
) {
    while let Some(pod) = work_pod_rx.recv().await {
        tracing::info!(pod_name = pod.name, "spawned worker task");
        tokio::spawn(work_pod(
            etcd_config.clone(),
            podman_config.clone(),
            my_name.clone(),
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
    mut limit_state: limits::State,
    pod: PodResource,
) {
    // for logging
    let pod_name = pod.name.clone();

    let committed = limit_state.claim_resources(&pod).await;
    if committed.is_none() {
        tracing::warn!(pod_name, "overcommitted resources");
    }

    let put_req = pod
        .clone()
        .with_state(PodState::Running.to_resource_state())
        .to_put_request(&etcd_config);
    while let Err(error) = try_put_request(&etcd_config, &put_req).await {
        tracing::warn!(pod_name, ?error, "could not update pod state, retrying...");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    let state = match run_pod_process(podman_config, &my_name, &pod).await {
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

    let put_req = pod
        .with_state(state.to_resource_state())
        .to_put_request(&etcd_config);
    while let Err(error) = try_put_request(&etcd_config, &put_req).await {
        tracing::warn!(pod_name, ?error, "could not update pod state, retrying...");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    let delete_req = DeleteRangeRequest {
        key: format!(
            "{prefix}{pod_name}",
            prefix = prefix::claimed_pods(&etcd_config)
        )
        .into(),
        ..Default::default()
    };
    while let Err(error) = try_delete_request(&etcd_config, &delete_req).await {
        tracing::warn!(pod_name, ?error, "could not delete pod claim, retrying...");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    if let Some(to_free) = committed {
        limit_state.release_resources(to_free).await;
    }
}

/// Run the pod process and return whether it exited successfully or not.
async fn run_pod_process(
    podman_config: podman::Config,
    my_name: &str,
    pod: &PodResource,
) -> std::io::Result<Option<bool>> {
    match podman::create_pod(&podman_config, my_name, pod).await? {
        Some(pod_state) => {
            tracing::info!(pod_name=pod_state.name, pod_ip=?pod_state.address, "created pod");
            for container in &pod.spec.containers {
                if !podman::start_container(&podman_config, &pod_state, container).await? {
                    podman::terminate_pod(&podman_config, &pod_state).await?;
                    return Ok(None);
                }
            }

            let exit_success = podman::wait_for_containers(&podman_config, &pod_state).await?;
            podman::terminate_pod(&podman_config, &pod_state).await?;
            Ok(Some(exit_success))
        }
        None => Ok(None),
    }
}

/// Attempt a put request - `work_pod` retries failures.
async fn try_put_request(etcd_config: &etcd::Config, req: &PutRequest) -> Result<(), Error> {
    let mut kv_client = etcd_config.kv_client().await?;
    kv_client.put(Request::new(req.clone())).await?;

    Ok(())
}

/// Attempt a delete request - `work_pod` retries failures.
async fn try_delete_request(
    etcd_config: &etcd::Config,
    req: &DeleteRangeRequest,
) -> Result<(), Error> {
    let mut kv_client = etcd_config.kv_client().await?;
    kv_client.delete_range(Request::new(req.clone())).await?;

    Ok(())
}
