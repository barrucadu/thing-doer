use rand::prelude::SliceRandom;
use std::collections::HashMap;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tonic::Request;

use nodelib::error::*;
use nodelib::etcd;
use nodelib::etcd::pb::etcdserverpb::compare;
use nodelib::etcd::pb::etcdserverpb::request_op;
use nodelib::etcd::pb::etcdserverpb::{
    Compare, DeleteRangeRequest, PutRequest, RequestOp, TxnRequest,
};
use nodelib::etcd::pb::mvccpb::{event::EventType, Event};
use nodelib::etcd::prefix;
use nodelib::etcd::watcher;
use nodelib::resources::pod::*;

use crate::node_watcher;

/// Exit code in case the scheduler channel closes.
pub static EXIT_CODE_SCHEDULER_FAILED: i32 = 1;

/// Maximum number of times to retry scheduling a pod before calling it
/// unschedulable.
pub static MAXIMUM_RETRIES: u64 = 5;

/// Interval to retry pods that could not be scheduled.
pub static RETRY_INTERVAL: u64 = 60;

/// Set up a watcher for new unscheduled pods, and the background tasks to
/// schedule pods to workers.
pub async fn initialise(
    etcd_config: etcd::Config,
    node_state: node_watcher::State,
    my_name: String,
) -> Result<(), Error> {
    let (new_pod_tx, new_pod_rx) = mpsc::unbounded_channel();

    let state = Arc::new(RwLock::new(WatchState {
        etcd_config: etcd_config.clone(),
        my_name,
        node_state,
        unscheduled_pods: HashMap::new(),
        new_pod_tx,
    }));

    watcher::setup_watcher(
        &etcd_config,
        state.clone(),
        vec![prefix::unscheduled_pods(&etcd_config)],
    )
    .await?;

    tokio::spawn(schedule_task(state.clone(), new_pod_rx));
    tokio::spawn(retry_task(state.clone()));

    Ok(())
}

/// State to schedule pods.
#[derive(Debug)]
struct WatchState {
    pub my_name: String,
    pub etcd_config: etcd::Config,
    pub node_state: node_watcher::State,
    pub unscheduled_pods: HashMap<String, (u64, PodResource)>,
    pub new_pod_tx: mpsc::UnboundedSender<String>,
}

impl watcher::Watcher for WatchState {
    async fn apply_event(&mut self, event: Event) {
        let prefix = prefix::unscheduled_pods(&self.etcd_config);
        let is_create = event.r#type() == EventType::Put;
        let kv = event.kv.unwrap();
        let key = String::from_utf8(kv.key).unwrap();

        if let Some((_, name)) = key.split_once(&prefix) {
            if is_create {
                match PodResource::try_from(kv.value) {
                    Ok(resource) => {
                        tracing::info!(name, "found new pod");
                        self.unscheduled_pods.insert(name.to_owned(), (0, resource));
                        self.new_pod_tx
                            .send(name.to_owned())
                            .expect("could not send to unbounded stream");
                    }
                    Err(error) => {
                        tracing::warn!(?key, ?error, "could not parse pod definition");
                    }
                }
            } else if self.unscheduled_pods.remove(name).is_some() {
                tracing::info!(name, "deleted unscheduled pod");
            }
        } else {
            tracing::warn!(?key, "unexpected watch key");
        }
    }
}

/// Background task to queue up all unscheduled pods every `RETRY_INTERVAL`
/// seconds.
async fn retry_task(state: Arc<RwLock<WatchState>>) {
    loop {
        tokio::time::sleep(Duration::from_secs(RETRY_INTERVAL)).await;

        let r = state.read().await;
        for name in r.unscheduled_pods.keys() {
            tracing::info!(name, "retrying unscheduled pod");
            r.new_pod_tx
                .send(name.clone())
                .expect("could not send to unbounded stream");
        }
    }
}

/// Background task to schedule pods.
async fn schedule_task(
    state: Arc<RwLock<WatchState>>,
    mut new_pod_rx: mpsc::UnboundedReceiver<String>,
) {
    while let Some(pod_name) = new_pod_rx.recv().await {
        let mut w = state.write().await;
        if let Some((retries, resource)) = w.unscheduled_pods.remove(&pod_name) {
            tracing::info!(pod_name, "got schedule request");
            match schedule_pod(&mut w, resource.clone()).await {
                ScheduleResult::Ok => (),
                ScheduleResult::RetryWithoutPenalty => {
                    tracing::warn!(
                        pod_name,
                        ?retries,
                        "could not schedule pod, retrying without penalty..."
                    );
                    w.unscheduled_pods
                        .insert(pod_name.clone(), (retries, resource));
                }
                ScheduleResult::RetryWithPenalty => {
                    if retries == MAXIMUM_RETRIES {
                        tracing::warn!(pod_name, "could not schedule pod, retry limit reached");
                        if let Err(error) = abandon_pod(&w.etcd_config, &w.my_name, resource).await
                        {
                            tracing::warn!(
                                pod_name,
                                ?error,
                                "could not abandon pod, now it is abandoned"
                            );
                        }
                    } else {
                        tracing::warn!(
                            pod_name,
                            ?retries,
                            "could not schedule pod, retrying with penalty..."
                        );
                        w.unscheduled_pods
                            .insert(pod_name.clone(), (retries + 1, resource.clone()));
                    }
                }
            }
        } else {
            tracing::warn!(pod_name, "got schedule request for missing pod");
        }
    }

    tracing::error!("scheduling channel unexpectedly closed, termianting...");
    process::exit(EXIT_CODE_SCHEDULER_FAILED);
}

/// The result of attempting to schedule a pod.
enum ScheduleResult {
    /// The pod was successfully scheduled (by us or another scheduler).
    Ok,
    /// The pod could not be scheduled for some reason that's not its fault
    /// (e.g. no healthy worker nodes).
    RetryWithoutPenalty,
    /// The pod could not be scheduled for some reason that *is* its fault
    /// (e.g. there are healthy workers, but none that meet its constraints).
    RetryWithPenalty,
}

/// Assign a pod to a worker.
async fn schedule_pod(state: &mut WatchState, pod: PodResource) -> ScheduleResult {
    // for logging
    let pod_name = pod.name.clone();

    let workers = state.node_state.get_healthy_workers().await;
    if workers.is_empty() {
        tracing::warn!(pod_name, "there are no healthy workers");
        return ScheduleResult::RetryWithoutPenalty;
    }

    if let Some(worker_name) = choose_worker_for_pod(&workers, &pod) {
        match apply_pod_schedule(&state.etcd_config, &state.my_name, &worker_name, pod).await {
            Ok(true) => {
                tracing::info!(pod_name, worker_name, "scheduled pod to worker");
                ScheduleResult::Ok
            }
            Ok(false) => {
                tracing::info!(pod_name, "pod already scheduled");
                ScheduleResult::Ok
            }
            Err(error) => {
                tracing::warn!(pod_name, ?error, "error applying pod schedule");
                ScheduleResult::RetryWithoutPenalty
            }
        }
    } else {
        ScheduleResult::RetryWithPenalty
    }
}

/// Schedule the pod to an arbitrary worker.
fn choose_worker_for_pod(
    workers: &HashMap<String, node_watcher::NodeState>,
    pod: &PodResource,
) -> Option<String> {
    let limits = pod.spec.aggregate_resources();
    let mut candidates = Vec::with_capacity(workers.len());
    for (name, node) in workers {
        if limits.cpu_ok(node.available_cpu) && limits.memory_ok(node.available_memory) {
            candidates.push(name.to_owned());
        }
    }
    candidates.choose(&mut rand::thread_rng()).cloned()
}

/// Assign a pod to a worker.
async fn apply_pod_schedule(
    etcd_config: &etcd::Config,
    my_name: &str,
    worker_name: &str,
    pod: PodResource,
) -> Result<bool, Error> {
    txn_check_and_schedule(
        etcd_config,
        Some(worker_name),
        pod.with_state(PodState::Scheduled)
            .with_metadata("scheduledBy", my_name.to_owned())
            .with_metadata("workedBy", worker_name.to_owned()),
    )
    .await
}

/// Mark a pod as abandoned, so no scheduler tries to take it.
async fn abandon_pod(
    etcd_config: &etcd::Config,
    my_name: &str,
    pod: PodResource,
) -> Result<bool, Error> {
    txn_check_and_schedule(
        etcd_config,
        None,
        pod.with_state(PodState::Abandoned)
            .with_metadata("scheduledBy", my_name.into()),
    )
    .await
}

/// Atomically check if a pod is still unscheduled and if so: delete the
/// unscheduled flag and write the new resources.
async fn txn_check_and_schedule(
    etcd_config: &etcd::Config,
    worker_name: Option<&str>,
    pod: PodResource,
) -> Result<bool, Error> {
    let unscheduled_pod_key = format!(
        "{prefix}{pod_name}",
        prefix = prefix::unscheduled_pods(etcd_config),
        pod_name = pod.name
    );

    // create revision (ie `Create`) = 0 => the key does not exist
    let compare = vec![Compare {
        result: compare::CompareResult::NotEqual.into(),
        target: compare::CompareTarget::Create.into(),
        key: unscheduled_pod_key.clone().into(),
        ..Default::default()
    }];

    let mut success = vec![
        RequestOp {
            request: Some(request_op::Request::RequestDeleteRange(
                DeleteRangeRequest {
                    key: unscheduled_pod_key.into(),
                    ..Default::default()
                },
            )),
        },
        RequestOp {
            request: Some(request_op::Request::RequestPut(
                pod.clone().to_put_request(etcd_config),
            )),
        },
    ];

    if let Some(n) = worker_name {
        success.push(RequestOp {
            request: Some(request_op::Request::RequestPut(PutRequest {
                key: format!(
                    "{prefix}{pod_name}",
                    prefix = prefix::worker_inbox(etcd_config, n),
                    pod_name = pod.name
                )
                .into(),
                value: pod.to_json_string().into(),
                ..Default::default()
            })),
        });
    }

    let mut kv_client = etcd_config.kv_client().await?;
    let res = kv_client
        .txn(Request::new(TxnRequest {
            compare,
            success,
            failure: Vec::new(),
        }))
        .await?;

    Ok(res.into_inner().succeeded)
}
