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
use nodelib::etcd::pb::etcdserverpb::request_op;
use nodelib::etcd::pb::etcdserverpb::{DeleteRangeRequest, PutRequest, RequestOp, TxnRequest};
use nodelib::etcd::pb::mvccpb::{event::EventType, Event};
use nodelib::etcd::watcher;
use nodelib::util;
use nodelib::Error;

/// Exit code in case the claimer channel closes.
pub static EXIT_CODE_CLAIMER_FAILED: i32 = 1;

/// Interval to retry pods that could not be scheduled.
pub static RETRY_INTERVAL: u64 = 60;

/// Set up a watcher for new assigned pods, and the background tasks to start
/// the pods.
pub async fn initialise(etcd_config: etcd::Config, my_name: String) -> Result<(), Error> {
    let (new_pod_tx, new_pod_rx) = mpsc::channel(16);

    let state = Arc::new(RwLock::new(WatchState {
        etcd_config: etcd_config.clone(),
        my_name: my_name.clone(),
        pending_pods: HashMap::new(),
        new_pod_tx,
    }));
    let prefix = inbox_prefix(&etcd_config, &my_name);

    watcher::setup_watcher(&etcd_config, state.clone(), prefix).await?;

    tokio::spawn(claim_task(state.clone(), new_pod_rx));
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
        let prefix = inbox_prefix(&self.etcd_config, &self.my_name);
        let is_create = event.r#type() == EventType::Put;
        let kv = event.kv.unwrap();
        let key = String::from_utf8(kv.key).unwrap();

        if let Some((_, name)) = key.split_once(&prefix) {
            if is_create {
                if let Some(value) = util::bytes_to_json(kv.value) {
                    tracing::info!(name, "found new pod");
                    self.pending_pods.insert(name.to_owned(), value);
                    if let Err(error) = self.new_pod_tx.send(name.to_owned()).await {
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
                if let Err(error) = w.new_pod_tx.send(name.clone()).await {
                    tracing::warn!(name, ?error, "could not trigger claimer");
                }
            }
        }
    }
}

/// Background task to claim pods.
async fn claim_task(state: Arc<RwLock<WatchState>>, mut new_pod_rx: Receiver<String>) {
    while let Some(pod_name) = new_pod_rx.recv().await {
        let mut w = state.write().await;
        if let Some(resource) = w.pending_pods.remove(&pod_name) {
            tracing::info!(pod_name, "got claim request");
            if let Err(error) = claim_pod(&w, pod_name.to_owned(), resource.clone()).await {
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
    mut pod_resource: Value,
) -> Result<(), Error> {
    let etcd_prefix = &state.etcd_config.prefix;
    let my_name = &state.my_name;

    pod_resource["state"] = "accepted".into();

    let mut kv_client = state.etcd_config.kv_client().await?;
    kv_client
        .txn(Request::new(TxnRequest {
            compare: Vec::new(),
            success: vec![
                RequestOp {
                    request: Some(request_op::Request::RequestDeleteRange(
                        DeleteRangeRequest {
                            key: format!("{etcd_prefix}/worker-inbox/{my_name}/{pod_name}").into(),
                            ..Default::default()
                        },
                    )),
                },
                RequestOp {
                    request: Some(request_op::Request::RequestPut(PutRequest {
                        key: format!("{etcd_prefix}/resource/pod/{pod_name}").into(),
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

/// Prefix under which new pods are written.
fn inbox_prefix(etcd_config: &etcd::Config, my_name: &str) -> String {
    format!(
        "{etcd_prefix}/worker-inbox/{my_name}/",
        etcd_prefix = etcd_config.prefix
    )
}
