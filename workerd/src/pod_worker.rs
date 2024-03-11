use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tonic::Request;

use nodelib::dns;
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
) -> Result<(Handle, Sender<PodResource>), Error> {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (work_pod_tx, work_pod_rx) = mpsc::channel(128);

    let prc = PodRunConfig {
        etcd: etcd_config,
        podman: podman_config,
        node_name: my_name,
        node_ip: my_ip,
        node_lease: lease_id,
    };

    tokio::spawn(work_task(prc, limit_state, shutdown_rx, work_pod_rx));

    Ok((Handle { shutdown_tx }, work_pod_tx))
}

/// To signal that the claimer should stop claiming things.
pub struct Handle {
    shutdown_tx: oneshot::Sender<oneshot::Sender<()>>,
}

impl Handle {
    /// Terminate all pods.
    pub async fn terminate(self) {
        nodelib::signal_channel(self.shutdown_tx).await;
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Wrapper for a bunch of static state.
#[derive(Clone)]
struct PodRunConfig {
    etcd: etcd::Config,
    podman: podman::Config,
    node_name: String,
    node_ip: Ipv4Addr,
    node_lease: LeaseId,
}

/// Background task to work pods.
async fn work_task(
    prc: PodRunConfig,
    limit_state: limits::State,
    mut shutdown_rx: oneshot::Receiver<oneshot::Sender<()>>,
    mut work_pod_rx: Receiver<PodResource>,
) {
    let running_pods = Arc::new(RwLock::new(HashMap::new()));

    loop {
        tokio::select! {
            res = work_pod_rx.recv() => match res {
                Some(pod) => launch_pod(prc.clone(), limit_state.clone(), running_pods.clone(), pod).await,
                None => break
            },
            ch = &mut shutdown_rx => {
                kill_all_pods(running_pods.clone()).await;
                let _ = ch.unwrap().send(());
                return;
            }
        }
    }

    tracing::error!("worker channel unexpectedly closed, termianting...");
    process::exit(EXIT_CODE_WORKER_FAILED);
}

/// Start executing a new pod.
async fn launch_pod(
    prc: PodRunConfig,
    limit_state: limits::State,
    running_pods: Arc<RwLock<HashMap<String, oneshot::Sender<oneshot::Sender<()>>>>>,
    pod: PodResource,
) {
    tracing::info!(pod_name = pod.name, "spawned worker task");
    let kill_rx = {
        let (tx, rx) = oneshot::channel();
        let mut inner = running_pods.write().await;
        inner.insert(pod.name.clone(), tx);
        rx
    };

    tokio::spawn(work_pod(prc, limit_state, running_pods, kill_rx, pod));
}

/// Kill all running pods.
async fn kill_all_pods(
    running_pods: Arc<RwLock<HashMap<String, oneshot::Sender<oneshot::Sender<()>>>>>,
) {
    let blocks = {
        let mut inner = running_pods.write().await;
        let mut out = Vec::with_capacity(inner.len());
        for (pod_name, kill_tx) in inner.drain() {
            tracing::info!(pod_name, "killing pod due to worker shutdown");
            let (tx, rx) = oneshot::channel();
            let _ = kill_tx.send(tx);
            out.push(rx);
        }
        out
    };

    for block in blocks {
        let _ = block.await;
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Proof-of-concept pod-worker
async fn work_pod<T>(
    prc: PodRunConfig,
    mut limit_state: limits::State,
    running_pods: Arc<RwLock<HashMap<String, T>>>,
    kill_rx: oneshot::Receiver<oneshot::Sender<()>>,
    pod: PodResource,
) {
    // for logging
    let pod_name = pod.name.clone();

    let committed = limit_state.claim_resources(&pod).await;
    if committed.is_none() {
        tracing::warn!(pod_name, "overcommitted resources");
    }

    set_pod_state(&prc.etcd, pod.clone(), PodState::Running).await;

    let (state, ch) = match run_and_wait_for_pod(&prc, &pod, kill_rx).await {
        Ok(Some(podman::WFC::Terminated(true))) => (PodState::ExitSuccess, None),
        Ok(Some(podman::WFC::Terminated(false))) => (PodState::ExitFailure, None),
        Ok(Some(podman::WFC::Signal(ch))) => (PodState::Killed, Some(ch)),
        Ok(None) => {
            tracing::warn!(pod_name, "could not initialise pod");
            (PodState::Errored, None)
        }
        Err(error) => {
            tracing::warn!(pod_name, ?error, "pod errored");
            (PodState::Errored, None)
        }
    };

    set_pod_state(&prc.etcd, pod.clone(), state).await;
    delete_pod_claim(&prc.etcd, &pod_name).await;

    if let Some(to_free) = committed {
        limit_state.release_resources(to_free).await;
    }

    if let Some(ch) = ch {
        let _ = ch.send(());
    }

    let mut w = running_pods.write().await;
    w.remove(&pod_name);
}

/// Run the pod process and return whether it exited successfully or not.
async fn run_and_wait_for_pod(
    prc: &PodRunConfig,
    pod: &PodResource,
    mut kill_rx: oneshot::Receiver<oneshot::Sender<()>>,
) -> std::io::Result<Option<podman::WFC>> {
    match podman::create_pod(&prc.podman, &prc.node_name, prc.node_ip, pod).await? {
        Some(pod_state) => {
            tracing::info!(name=pod_state.name, address=?pod_state.address, "created pod");
            create_pod_dns_record(&prc.etcd, prc.node_lease, &pod_state).await;

            for container in &pod.spec.containers {
                if !podman::start_container(&prc.podman, &pod_state, container).await? {
                    delete_pod_dns_record(&prc.etcd, &pod_state).await;
                    podman::terminate_pod(&prc.podman, &pod_state).await?;
                    return Ok(None);
                }
            }

            let wfc = podman::wait_for_containers(&prc.podman, &pod_state, &mut kill_rx).await?;
            delete_pod_dns_record(&prc.etcd, &pod_state).await;
            podman::terminate_pod(&prc.podman, &pod_state).await?;
            Ok(Some(wfc))
        }
        None => Ok(None),
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Write the key for the DNS record of the pod, retrying until it succeeds.
///
/// TODO: add a retry limit
async fn create_pod_dns_record(
    etcd_config: &etcd::Config,
    lease_id: LeaseId,
    pod_state: &podman::PodState,
) {
    while let Err(error) = dns::create_leased_a_record(
        etcd_config,
        lease_id,
        dns::Namespace::Pod,
        &pod_state.hostname,
        pod_state.address,
    )
    .await
    {
        tracing::warn!(?error, "could not create DNS record, retrying...");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Delete the key for the DNS record of the pod, retrying until it succeeds.
///
/// TODO: add a retry limit
async fn delete_pod_dns_record(etcd_config: &etcd::Config, pod_state: &podman::PodState) {
    while let Err(error) =
        dns::delete_record(etcd_config, dns::Namespace::Pod, &pod_state.hostname).await
    {
        tracing::warn!(?error, "could not destroy DNS record, retrying...");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Set the state of the pod resource, retrying until it succeeds.
///
/// TODO: add a retry limit
async fn set_pod_state(etcd_config: &etcd::Config, pod: PodResource, state: PodState) {
    let req = pod.with_state(state).to_put_request(etcd_config);
    while let Err(error) = try_put_request(etcd_config, req.clone()).await {
        tracing::warn!(?error, "could not update pod state, retrying...");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Delete the node's claim on a pod, retrying until it succeeds.
///
/// TODO: add a retry limit
async fn delete_pod_claim(etcd_config: &etcd::Config, pod_name: &str) {
    let req = DeleteRangeRequest {
        key: format!(
            "{prefix}{pod_name}",
            prefix = prefix::claimed_pods(etcd_config)
        )
        .into(),
        ..Default::default()
    };
    while let Err(error) = try_delete_request(etcd_config, req.clone()).await {
        tracing::warn!(?error, "could not delete record, retrying...");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Attempt a put request.
async fn try_put_request(etcd_config: &etcd::Config, req: PutRequest) -> Result<(), Error> {
    let mut kv_client = etcd_config.kv_client().await?;
    kv_client.put(Request::new(req)).await?;

    Ok(())
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
