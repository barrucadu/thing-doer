use rand::prelude::SliceRandom;
use serde_json::Value;
use std::collections::HashMap;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tonic::Request;

use nodelib::etcd;
use nodelib::etcd::pb::etcdserverpb::compare;
use nodelib::etcd::pb::etcdserverpb::request_op;
use nodelib::etcd::pb::etcdserverpb::{
    Compare, DeleteRangeRequest, PutRequest, RequestOp, TxnRequest,
};
use nodelib::etcd::pb::mvccpb::{event::EventType, Event};
use nodelib::etcd::prefix;
use nodelib::etcd::watcher;
use nodelib::util;
use nodelib::Error;

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
    let (new_pod_tx, new_pod_rx) = mpsc::channel(128);

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
        prefix::unscheduled_pods(&etcd_config),
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
    pub unscheduled_pods: HashMap<String, (u64, Value)>,
    pub new_pod_tx: Sender<String>,
}

impl watcher::Watcher for WatchState {
    async fn apply_event(&mut self, event: Event) {
        let prefix = prefix::unscheduled_pods(&self.etcd_config);
        let is_create = event.r#type() == EventType::Put;
        let kv = event.kv.unwrap();
        let key = String::from_utf8(kv.key).unwrap();

        if let Some((_, name)) = key.split_once(&prefix) {
            if is_create {
                if let Some(value) = util::bytes_to_json(kv.value) {
                    tracing::info!(name, "found new pod");
                    self.unscheduled_pods.insert(name.to_owned(), (0, value));
                    if let Err(error) = self.new_pod_tx.try_send(name.to_owned()) {
                        tracing::warn!(name, ?error, "could not trigger scheduler");
                    }
                } else {
                    tracing::warn!(?key, "could not parse pod definition");
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

        let pods_to_retry = {
            let r = state.read().await;
            r.unscheduled_pods.keys().cloned().collect::<Vec<_>>()
        };

        if !pods_to_retry.is_empty() {
            tracing::info!(count = ?pods_to_retry.len(), "retrying unscheduled pods");
            let w = state.write().await;
            for name in pods_to_retry {
                tracing::info!(name, "retrying unscheduled pod");
                if let Err(error) = w.new_pod_tx.try_send(name.clone()) {
                    tracing::warn!(name, ?error, "could not trigger scheduler");
                }
            }
        }
    }
}

/// Background task to schedule pods.
async fn schedule_task(state: Arc<RwLock<WatchState>>, mut new_pod_rx: Receiver<String>) {
    while let Some(pod_name) = new_pod_rx.recv().await {
        let mut w = state.write().await;
        if let Some((retries, resource)) = w.unscheduled_pods.remove(&pod_name) {
            tracing::info!(pod_name, "got schedule request");
            match schedule_pod(&mut w, pod_name.to_owned(), resource.clone()).await {
                ScheduleResult::Ok => (),
                ScheduleResult::RetryWithoutPenalty => {
                    tracing::warn!(
                        pod_name,
                        ?retries,
                        "could not schedule pod, retrying without penalty..."
                    );
                    w.unscheduled_pods
                        .insert(pod_name.to_owned(), (retries, resource));
                }
                ScheduleResult::RetryWithPenalty => {
                    if retries == MAXIMUM_RETRIES {
                        tracing::warn!(pod_name, "could not schedule pod, retry limit reached");
                        if let Err(error) =
                            abandon_pod(&w.etcd_config, &w.my_name, &pod_name, resource).await
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
                            .insert(pod_name.to_owned(), (retries + 1, resource.clone()));
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
async fn schedule_pod(state: &mut WatchState, pod_name: String, resource: Value) -> ScheduleResult {
    let workers = state.node_state.get_healthy_workers().await;
    if workers.is_empty() {
        tracing::warn!(pod_name, "there are no healthy workers");
        return ScheduleResult::RetryWithoutPenalty;
    }

    if let Some(worker_name) = choose_worker_for_pod(workers, &resource).await {
        match apply_pod_schedule(
            &state.etcd_config,
            &state.my_name,
            &worker_name,
            &pod_name,
            resource,
        )
        .await
        {
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
async fn choose_worker_for_pod(
    workers: HashMap<String, node_watcher::NodeState>,
    _pod_resource: &Value,
) -> Option<String> {
    let names = workers.keys().cloned().collect::<Vec<_>>();
    names.choose(&mut rand::thread_rng()).cloned()
}

/// Assign a pod to a worker.
async fn apply_pod_schedule(
    etcd_config: &etcd::Config,
    my_name: &str,
    worker_name: &str,
    pod_name: &str,
    mut pod_resource: Value,
) -> Result<bool, Error> {
    pod_resource["state"] = "scheduled".into();
    pod_resource["metadata"]["scheduledBy"] = my_name.into();
    pod_resource["metadata"]["workedBy"] = worker_name.into();

    txn_check_and_schedule(etcd_config, pod_name, Some(worker_name), pod_resource).await
}

/// Mark a pod as abandoned, so no scheduler tries to take it.
async fn abandon_pod(
    etcd_config: &etcd::Config,
    my_name: &str,
    pod_name: &str,
    mut pod_resource: Value,
) -> Result<bool, Error> {
    pod_resource["state"] = "abandoned".into();
    pod_resource["metadata"]["scheduledBy"] = my_name.into();

    txn_check_and_schedule(etcd_config, pod_name, None, pod_resource).await
}

/// Atomically check if a pod is still unscheduled and if so: delete the
/// unscheduled flag and write the new resources.
async fn txn_check_and_schedule(
    etcd_config: &etcd::Config,
    pod_name: &str,
    worker_name: Option<&str>,
    pod_resource: Value,
) -> Result<bool, Error> {
    let unscheduled_pod_key = format!(
        "{prefix}{pod_name}",
        prefix = prefix::unscheduled_pods(etcd_config)
    );
    let pod_resource_key = format!(
        "{prefix}{pod_name}",
        prefix = prefix::resource(etcd_config, "pod")
    );
    let pod_resource_json = pod_resource.to_string();

    // not sure how to check it exists directly, so just check it exists and has
    // lease ID 0 - i.e. no lease.
    let compare = vec![Compare {
        result: compare::CompareResult::Equal.into(),
        target: compare::CompareTarget::Lease.into(),
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
            request: Some(request_op::Request::RequestPut(PutRequest {
                key: pod_resource_key.into(),
                value: pod_resource_json.clone().into(),
                ..Default::default()
            })),
        },
    ];
    if let Some(n) = worker_name {
        let worker_inbox_key = format!(
            "{prefix}{pod_name}",
            prefix = prefix::worker_inbox(etcd_config, n)
        );
        success.push(RequestOp {
            request: Some(request_op::Request::RequestPut(PutRequest {
                key: worker_inbox_key.into(),
                value: pod_resource_json.into(),
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
