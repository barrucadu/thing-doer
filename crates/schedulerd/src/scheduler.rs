use rand::prelude::SliceRandom;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::Request;

use nodelib::error::*;
use nodelib::etcd;
use nodelib::etcd::pb::etcdserverpb::compare;
use nodelib::etcd::pb::etcdserverpb::request_op;
use nodelib::etcd::pb::etcdserverpb::{
    Compare, DeleteRangeRequest, PutRequest, RequestOp, TxnRequest,
};
use nodelib::etcd::prefix;
use nodelib::resources::pod::*;

use crate::state::{NodeState, SharedNodeState};

/// Maximum number of times to retry scheduling a pod before calling it
/// unschedulable.
pub static MAXIMUM_RETRIES: u64 = 5;

/// Interval to retry pods that could not be scheduled.
pub static RETRY_INTERVAL: u64 = 60;

/// A pod-to-schedule plus metadata
#[derive(Clone)]
pub struct UnscheduledPod {
    resource: PodResource,
    mod_revision: i64,
    retries: u64,
}

impl UnscheduledPod {
    /// Construct a new `UnscheduledPod`.
    pub fn new(resource: PodResource, mod_revision: i64) -> Self {
        Self {
            resource,
            mod_revision,
            retries: 0,
        }
    }
}

/// Background task to schedule pods.
pub async fn task(
    etcd_config: etcd::Config,
    node_state: SharedNodeState,
    my_name: String,
    mut new_pod_rx: mpsc::UnboundedReceiver<UnscheduledPod>,
    new_pod_tx: mpsc::UnboundedSender<UnscheduledPod>,
) {
    let (retry_tx, mut retry_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(RETRY_INTERVAL)).await;
            retry_tx
                .send(())
                .expect("could not write to unbounded channel");
        }
    });

    let mut to_retry = Vec::new();
    loop {
        tokio::select! {
            msg = new_pod_rx.recv() => {
                let mut unscheduled_pod = msg.unwrap();
                // for logging
                let pod_name = unscheduled_pod.resource.name.clone();
                let retries = unscheduled_pod.retries;
                tracing::info!(pod_name, ?retries, "got schedule request");
                match schedule_pod(&etcd_config, &node_state, &my_name, unscheduled_pod.clone()).await {
                    ScheduleResult::Ok => (),
                    ScheduleResult::RetryWithoutPenalty => {
                        tracing::warn!(pod_name, ?retries, "could not schedule pod, retrying without penalty...");
                        to_retry.push(unscheduled_pod);
                    }
                    ScheduleResult::RetryWithPenalty => {
                        if unscheduled_pod.retries == MAXIMUM_RETRIES {
                            tracing::warn!(pod_name, ?retries, "could not schedule pod, retry limit reached");
                            if let Err(error) = abandon_pod(&etcd_config, &my_name, unscheduled_pod).await {
                                tracing::warn!(pod_name, ?error, "could not abandon pod");
                            }
                        } else {
                            tracing::warn!(pod_name, ?retries, "could not schedule pod, retrying with penalty...");
                            unscheduled_pod.retries += 1;
                            to_retry.push(unscheduled_pod);
                        }
                    }
                }
            }
            _ = retry_rx.recv() => {
                enqueue_retries(to_retry, &new_pod_tx);
                to_retry = Vec::new();
            }
        }
    }
}

/// Queue up all of the pods in need of retrying.
fn enqueue_retries(
    to_retry: Vec<UnscheduledPod>,
    new_pod_tx: &mpsc::UnboundedSender<UnscheduledPod>,
) {
    for x in to_retry {
        tracing::info!(pod_name = x.resource.name, "retrying unscheduled pod");
        new_pod_tx
            .send(x)
            .expect("could not send to unbounded channel");
    }
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
async fn schedule_pod(
    etcd_config: &etcd::Config,
    node_state: &SharedNodeState,
    my_name: &str,
    unscheduled_pod: UnscheduledPod,
) -> ScheduleResult {
    // for logging
    let pod_name = unscheduled_pod.resource.name.clone();

    let workers = node_state.get_healthy_workers().await;
    if workers.is_empty() {
        tracing::warn!(pod_name, "there are no healthy workers");
        return ScheduleResult::RetryWithoutPenalty;
    }

    if let Some(worker_name) = choose_worker_for_pod(&workers, &unscheduled_pod.resource) {
        match apply_pod_schedule(etcd_config, my_name, &worker_name, unscheduled_pod).await {
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
    workers: &HashMap<String, NodeState>,
    pod: &PodResource,
) -> Option<String> {
    let limits = pod.spec.aggregate_resources();
    let mut candidates = Vec::with_capacity(workers.len());
    for (name, node) in workers {
        let worker_name = name.to_owned();
        if pod.spec.is_worker_acceptable(&worker_name)
            && limits.cpu_ok(node.available_cpu)
            && limits.memory_ok(node.available_memory)
        {
            candidates.push(worker_name);
        }
    }
    candidates.choose(&mut rand::thread_rng()).cloned()
}

/// Assign a pod to a worker.
async fn apply_pod_schedule(
    etcd_config: &etcd::Config,
    my_name: &str,
    worker_name: &str,
    unscheduled_pod: UnscheduledPod,
) -> Result<bool, Error> {
    txn_check_and_schedule(
        etcd_config,
        Some(worker_name),
        unscheduled_pod.mod_revision,
        unscheduled_pod
            .resource
            .with_state(PodState::Scheduled)
            .with_metadata("scheduledBy", my_name.to_owned())
            .with_metadata("workedBy", worker_name.to_owned()),
    )
    .await
}

/// Mark a pod as abandoned, so no scheduler tries to take it.
async fn abandon_pod(
    etcd_config: &etcd::Config,
    my_name: &str,
    unscheduled_pod: UnscheduledPod,
) -> Result<bool, Error> {
    txn_check_and_schedule(
        etcd_config,
        None,
        unscheduled_pod.mod_revision,
        unscheduled_pod
            .resource
            .with_state(PodState::Abandoned)
            .with_metadata("scheduledBy", my_name.into()),
    )
    .await
}

/// Atomically check if a pod is still unscheduled and if so: delete the
/// unscheduled flag and write the new resources.
async fn txn_check_and_schedule(
    etcd_config: &etcd::Config,
    worker_name: Option<&str>,
    mod_revision: i64,
    pod: PodResource,
) -> Result<bool, Error> {
    let unscheduled_pod_key = format!(
        "{prefix}{pod_name}",
        prefix = prefix::unscheduled_pods(etcd_config),
        pod_name = pod.name
    );

    let compare = vec![Compare {
        result: compare::CompareResult::Equal.into(),
        target: compare::CompareTarget::Mod.into(),
        key: unscheduled_pod_key.clone().into(),
        target_union: Some(compare::TargetUnion::ModRevision(mod_revision)),
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
