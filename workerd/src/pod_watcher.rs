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
use nodelib::etcd::leaser::LeaseId;
use nodelib::etcd::pb::etcdserverpb::request_op;
use nodelib::etcd::pb::etcdserverpb::{DeleteRangeRequest, PutRequest, RequestOp, TxnRequest};
use nodelib::etcd::pb::mvccpb::{event::EventType, Event};
use nodelib::etcd::prefix;
use nodelib::etcd::watcher;
use nodelib::util;
use nodelib::Error;

/// Exit code in case the claimer channel closes.
pub static EXIT_CODE_CLAIMER_FAILED: i32 = 1;

/// Interval to retry pods that could not be scheduled.
pub static RETRY_INTERVAL: u64 = 60;

/// Set up a watcher for new assigned pods, and the background tasks to start
/// the pods.
pub async fn initialise(
    etcd_config: etcd::Config,
    my_name: String,
    lease_id: LeaseId,
) -> Result<(), Error> {
    let (new_pod_tx, new_pod_rx) = mpsc::channel(128);

    let state = Arc::new(RwLock::new(WatchState {
        etcd_config: etcd_config.clone(),
        my_name: my_name.clone(),
        pending_pods: HashMap::new(),
        new_pod_tx,
    }));

    watcher::setup_watcher(
        &etcd_config,
        state.clone(),
        prefix::worker_inbox(&etcd_config, &my_name),
    )
    .await?;

    tokio::spawn(claim_task(state.clone(), lease_id, new_pod_rx));
    tokio::spawn(retry_task(state.clone()));

    Ok(())
}

/// State to schedule pods.
#[derive(Debug)]
struct WatchState {
    pub my_name: String,
    pub etcd_config: etcd::Config,
    pub pending_pods: HashMap<String, Value>,
    pub new_pod_tx: Sender<String>,
}

impl watcher::Watcher for WatchState {
    async fn apply_event(&mut self, event: Event) {
        let prefix = prefix::worker_inbox(&self.etcd_config, &self.my_name);
        let is_create = event.r#type() == EventType::Put;
        let kv = event.kv.unwrap();
        let key = String::from_utf8(kv.key).unwrap();

        if let Some((_, name)) = key.split_once(&prefix) {
            if is_create {
                if let Some(value) = util::bytes_to_json(kv.value) {
                    tracing::info!(name, "found new pod");
                    self.pending_pods.insert(name.to_owned(), value);
                    if let Err(error) = self.new_pod_tx.try_send(name.to_owned()) {
                        tracing::warn!(name, ?error, "could not trigger claimer");
                    }
                } else {
                    tracing::warn!(?key, "could not parse pod definition");
                }
            } else if self.pending_pods.remove(name).is_some() {
                tracing::info!(name, "deleted pending pod");
            }
        } else {
            tracing::warn!(?key, "unexpected watch key");
        }
    }
}

/// Background task to queue up all pending pods every `RETRY_INTERVAL`
/// seconds.
async fn retry_task(state: Arc<RwLock<WatchState>>) {
    loop {
        tokio::time::sleep(Duration::from_secs(RETRY_INTERVAL)).await;

        let pods_to_retry = {
            let r = state.read().await;
            r.pending_pods.keys().cloned().collect::<Vec<_>>()
        };

        if !pods_to_retry.is_empty() {
            tracing::info!(count = ?pods_to_retry.len(), "retrying pending pods");
            let w = state.write().await;
            for name in pods_to_retry {
                tracing::info!(name, "retrying pending pod");
                if let Err(error) = w.new_pod_tx.try_send(name.clone()) {
                    tracing::warn!(name, ?error, "could not trigger claimer");
                }
            }
        }
    }
}

/// Background task to claim pods.
async fn claim_task(
    state: Arc<RwLock<WatchState>>,
    lease_id: LeaseId,
    mut new_pod_rx: Receiver<String>,
) {
    while let Some(pod_name) = new_pod_rx.recv().await {
        let mut w = state.write().await;
        if let Some(resource) = w.pending_pods.remove(&pod_name) {
            tracing::info!(pod_name, "got claim request");
            if let Err(error) = claim_pod(&w, pod_name.to_owned(), lease_id, resource.clone()).await
            {
                tracing::warn!(pod_name, ?error, "could not claim pod, retrying...");
                w.pending_pods.insert(pod_name.to_owned(), resource);
            }
        } else {
            tracing::warn!(pod_name, "got claim request for missing pod");
        }
    }

    tracing::error!("claimer channel unexpectedly closed, termianting...");
    process::exit(EXIT_CODE_CLAIMER_FAILED);
}

/// Update a pod's state to "accepted".
///
/// TODO trigger something to start the pod process.
async fn claim_pod(
    state: &WatchState,
    pod_name: String,
    lease_id: LeaseId,
    mut pod_resource: Value,
) -> Result<(), Error> {
    pod_resource["state"] = "accepted".into();

    let mut kv_client = state.etcd_config.kv_client().await?;
    kv_client
        .txn(Request::new(TxnRequest {
            compare: Vec::new(),
            success: vec![
                RequestOp {
                    request: Some(request_op::Request::RequestDeleteRange(
                        DeleteRangeRequest {
                            key: format!(
                                "{prefix}{pod_name}",
                                prefix = prefix::worker_inbox(&state.etcd_config, &state.my_name)
                            )
                            .into(),
                            ..Default::default()
                        },
                    )),
                },
                RequestOp {
                    request: Some(request_op::Request::RequestPut(PutRequest {
                        key: format!(
                            "{prefix}{pod_name}",
                            prefix = prefix::claimed_pods(&state.etcd_config)
                        )
                        .into(),
                        value: state.my_name.clone().into(),
                        lease: lease_id.0,
                        ..Default::default()
                    })),
                },
                RequestOp {
                    request: Some(request_op::Request::RequestPut(PutRequest {
                        key: format!(
                            "{prefix}{pod_name}",
                            prefix = prefix::resource(&state.etcd_config, "pod")
                        )
                        .into(),
                        value: pod_resource.to_string().into(),
                        ..Default::default()
                    })),
                },
            ],
            failure: Vec::new(),
        }))
        .await?;

    Ok(())
}
