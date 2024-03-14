use std::collections::HashMap;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tonic::Request;

use nodelib::error::*;
use nodelib::etcd;
use nodelib::etcd::leaser::LeaseId;
use nodelib::etcd::pb::etcdserverpb::compare;
use nodelib::etcd::pb::etcdserverpb::request_op;
use nodelib::etcd::pb::etcdserverpb::{
    Compare, DeleteRangeRequest, PutRequest, RequestOp, TxnRequest,
};
use nodelib::etcd::pb::mvccpb::{event::EventType, Event};
use nodelib::etcd::prefix;
use nodelib::etcd::watcher;
use nodelib::resources::pod::*;

/// Exit code in case the claimer channel closes.
pub static EXIT_CODE_CLAIMER_FAILED: i32 = 1;

/// Interval to retry pods that could not be processed.
pub static RETRY_INTERVAL: u64 = 60;

/// Set up a watcher for new assigned pods, and the background tasks to claim
/// the pods.
pub async fn initialise(
    etcd_config: etcd::Config,
    my_name: String,
    lease_id: LeaseId,
    work_pod_tx: mpsc::UnboundedSender<PodResource>,
) -> Result<Handle, Error> {
    let (new_pod_tx, new_pod_rx) = mpsc::unbounded_channel();

    let state = Arc::new(RwLock::new(WatchState {
        running: true,
        etcd_config: etcd_config.clone(),
        my_name: my_name.clone(),
        unclaimed_pods: HashMap::new(),
        new_pod_tx,
    }));

    watcher::setup_watcher(
        &etcd_config,
        state.clone(),
        vec![prefix::worker_inbox(&etcd_config, &my_name)],
    )
    .await?;

    tokio::spawn(claim_task(state.clone(), lease_id, new_pod_rx, work_pod_tx));
    tokio::spawn(retry_task(state.clone()));

    Ok(Handle { ws: state.clone() })
}

/// To signal that the claimer should stop claiming things.
pub struct Handle {
    ws: Arc<RwLock<WatchState>>,
}

impl Handle {
    /// Stop claiming new pods.
    pub async fn terminate(self) {
        let mut inner = self.ws.write().await;
        inner.running = false;
    }
}

/// State to schedule pods.
#[derive(Debug)]
struct WatchState {
    running: bool,
    my_name: String,
    etcd_config: etcd::Config,
    unclaimed_pods: HashMap<String, PodResource>,
    new_pod_tx: mpsc::UnboundedSender<String>,
}

impl watcher::Watcher for WatchState {
    async fn apply_event(&mut self, event: Event) {
        let prefix = prefix::worker_inbox(&self.etcd_config, &self.my_name);
        let is_create = event.r#type() == EventType::Put;
        let kv = event.kv.unwrap();
        let key = String::from_utf8(kv.key).unwrap();

        if let Some((_, name)) = key.split_once(&prefix) {
            if is_create {
                if let Ok(resource) = PodResource::try_from(kv.value) {
                    tracing::info!(name, "found new pod");
                    self.unclaimed_pods.insert(name.to_owned(), resource);
                    self.new_pod_tx
                        .send(name.to_owned())
                        .expect("could not send to unbounded channel");
                } else {
                    tracing::warn!(?key, "could not parse pod definition");
                }
            } else if self.unclaimed_pods.remove(name).is_some() {
                tracing::info!(name, "deleted unclaimed pod");
            }
        } else {
            tracing::warn!(?key, "unexpected watch key");
        }
    }
}

/// Background task to queue up all unclaimed pods every `RETRY_INTERVAL`
/// seconds.
async fn retry_task(state: Arc<RwLock<WatchState>>) {
    loop {
        tokio::time::sleep(Duration::from_secs(RETRY_INTERVAL)).await;

        let r = state.read().await;
        for name in r.unclaimed_pods.keys() {
            tracing::info!(name, "retrying unclaimed pod");
            r.new_pod_tx
                .send(name.clone())
                .expect("could not send to unbounded channel");
        }
    }
}

/// Background task to claim pods.
async fn claim_task(
    state: Arc<RwLock<WatchState>>,
    lease_id: LeaseId,
    mut new_pod_rx: mpsc::UnboundedReceiver<String>,
    work_pod_tx: mpsc::UnboundedSender<PodResource>,
) {
    while let Some(pod_name) = new_pod_rx.recv().await {
        let mut w = state.write().await;
        if !w.running {
            tracing::info!(pod_name, "got claim request but worker is terminating");
            continue;
        }

        if let Some(resource) = w.unclaimed_pods.remove(&pod_name) {
            tracing::info!(pod_name, "got claim request");
            match claim_pod(&w, lease_id, resource.clone()).await {
                Ok(Some(resource)) => {
                    work_pod_tx
                        .send(resource.clone())
                        .expect("could not send to unbounded channel");
                }
                Ok(None) => {
                    tracing::info!(pod_name, "pod killed before claim");
                }
                Err(error) => {
                    tracing::warn!(pod_name, ?error, "could not claim pod, retrying...");
                    w.unclaimed_pods.insert(pod_name.to_owned(), resource);
                }
            }
        } else {
            tracing::warn!(pod_name, "got claim request for missing pod");
        }
    }

    tracing::error!("claimer channel unexpectedly closed, termianting...");
    process::exit(EXIT_CODE_CLAIMER_FAILED);
}

/// Update a pod's state to "accepted" and remove it from the inbox.
async fn claim_pod(
    state: &WatchState,
    lease_id: LeaseId,
    pod: PodResource,
) -> Result<Option<PodResource>, Error> {
    let pod = pod.with_state(PodState::Accepted);

    // check if the pod resource still exists - if it does, remove it from our
    // inbox and claim it; if the pod resource has been deleted, just remove it
    // from our inbox but don't claim it
    let mut kv_client = state.etcd_config.kv_client().await?;
    let delete_from_inbox = RequestOp {
        request: Some(request_op::Request::RequestDeleteRange(
            DeleteRangeRequest {
                key: format!(
                    "{prefix}{pod_name}",
                    prefix = prefix::worker_inbox(&state.etcd_config, &state.my_name),
                    pod_name = pod.name
                )
                .into(),
                ..Default::default()
            },
        )),
    };
    let res = kv_client
        .txn(Request::new(TxnRequest {
            // create revision (ie `Create`) != 0 => the key exists
            compare: vec![Compare {
                result: compare::CompareResult::NotEqual.into(),
                target: compare::CompareTarget::Create.into(),
                key: pod.key(&state.etcd_config).into(),
                ..Default::default()
            }],
            // success: pod still exists
            success: vec![
                delete_from_inbox.clone(),
                RequestOp {
                    request: Some(request_op::Request::RequestPut(PutRequest {
                        key: format!(
                            "{prefix}{pod_name}",
                            prefix = prefix::claimed_pods(&state.etcd_config),
                            pod_name = pod.name
                        )
                        .into(),
                        value: state.my_name.clone().into(),
                        lease: lease_id.0,
                        ..Default::default()
                    })),
                },
                RequestOp {
                    request: Some(request_op::Request::RequestPut(
                        pod.clone().to_put_request(&state.etcd_config),
                    )),
                },
            ],
            // failure: pod does not exist
            failure: vec![delete_from_inbox],
        }))
        .await?;

    if res.into_inner().succeeded {
        Ok(Some(pod))
    } else {
        Ok(None)
    }
}
