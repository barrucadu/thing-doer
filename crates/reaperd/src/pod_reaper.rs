use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tonic::Request;

use nodelib::error::*;
use nodelib::etcd;
use nodelib::etcd::pb::etcdserverpb::compare;
use nodelib::etcd::pb::etcdserverpb::kv_client;
use nodelib::etcd::pb::etcdserverpb::request_op;
use nodelib::etcd::pb::etcdserverpb::{Compare, PutRequest, RangeRequest, RequestOp, TxnRequest};
use nodelib::etcd::pb::mvccpb::{event::EventType, Event};
use nodelib::etcd::prefix;
use nodelib::etcd::watcher;
use nodelib::resources::pod::{PodResource, PodState};

/// Interval to retry pod that could not be reaped.
pub static RETRY_INTERVAL: u64 = 60;

/// Set up a watcher for new dead pods, and the background tasks to reap them.
pub async fn initialise(
    etcd_config: etcd::Config,
    my_name: String,
) -> Result<mpsc::UnboundedSender<String>, Error> {
    let (reap_pod_tx, reap_pod_rx) = mpsc::unbounded_channel();

    let state = Arc::new(RwLock::new(WatchState {
        etcd_config: etcd_config.clone(),
        reap_pod_tx: reap_pod_tx.clone(),
    }));

    watcher::setup_watcher(
        &etcd_config,
        state.clone(),
        prefix::claimed_pods(&etcd_config),
    )
    .await?;

    tokio::spawn(task(etcd_config, my_name, reap_pod_rx, reap_pod_tx.clone()));

    Ok(reap_pod_tx)
}

/// State to reap pods.
#[derive(Debug)]
struct WatchState {
    pub etcd_config: etcd::Config,
    pub reap_pod_tx: mpsc::UnboundedSender<String>,
}

impl watcher::Watcher for WatchState {
    async fn apply_event(&mut self, event: Event) {
        let prefix = prefix::claimed_pods(&self.etcd_config);
        let is_delete = event.r#type() == EventType::Delete;
        let kv = event.kv.unwrap();
        let key = String::from_utf8(kv.key).unwrap();

        if let Some((_, name)) = key.split_once(&prefix) {
            if is_delete {
                tracing::info!(name, "found reapable pod");
                self.reap_pod_tx
                    .send(name.to_owned())
                    .expect("could not send to unbounded channel");
            }
        } else {
            tracing::warn!(?key, "unexpected watch key");
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Background task to reap dead pods.
async fn task(
    etcd_config: etcd::Config,
    my_name: String,
    mut reap_pod_rx: mpsc::UnboundedReceiver<String>,
    reap_pod_tx: mpsc::UnboundedSender<String>,
) {
    let mut to_retry = Vec::new();
    loop {
        tokio::select! {
            msg = reap_pod_rx.recv() => {
                let name = msg.unwrap();
                if !reap_pod(&etcd_config, &my_name, &name).await {
                    to_retry.push(name);
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(RETRY_INTERVAL)) => {
                enqueue_retries(to_retry, &reap_pod_tx).await;
                to_retry = Vec::new();
            }
        }
    }
}

/// Reap a single pod.  Returns `false` if it needs to retry.
async fn reap_pod(etcd_config: &etcd::Config, my_name: &str, pod_name: &str) -> bool {
    tracing::info!(pod_name, "got reap request");
    match mark_pod_as_dead(etcd_config, my_name, pod_name).await {
        Ok(true) => {
            tracing::info!(pod_name, "reaped pod");
            true
        }
        Ok(false) => {
            tracing::info!(pod_name, "pod already reaped");
            true
        }
        Err(error) => {
            tracing::warn!(pod_name, ?error, "could not reap pod, retrying...");
            false
        }
    }
}

/// Queue up all of the pods in need of retrying.
async fn enqueue_retries(to_retry: Vec<String>, reap_pod_tx: &mpsc::UnboundedSender<String>) {
    for pod_name in to_retry.into_iter() {
        tracing::info!(pod_name, "retrying unreaped pod");
        reap_pod_tx
            .send(pod_name)
            .expect("could not send to unbounded channel");
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Mark a pod as dead - if it's not in a terminal state, and nobody else has
/// reaped it yet.
async fn mark_pod_as_dead(
    etcd_config: &etcd::Config,
    my_name: &str,
    pod_name: &str,
) -> Result<bool, Error> {
    let key = format!(
        "{prefix}{pod_name}",
        prefix = prefix::resource(etcd_config, "pod")
    );

    let mut kv_client = etcd_config.kv_client().await?;
    let res = kv_client
        .range(Request::new(RangeRequest {
            key: key.clone().into(),
            limit: 1,
            ..Default::default()
        }))
        .await?
        .into_inner();

    if res.kvs.is_empty() {
        return Ok(false);
    }

    let version = res.kvs[0].version;
    let bytes = res.kvs[0].value.clone();
    match PodResource::try_from(bytes) {
        Ok(pod) => {
            if pod.metadata.contains_key("reapedBy") || pod.state.unwrap().is_terminal() {
                Ok(false)
            } else {
                txn_check_and_set(
                    kv_client,
                    version,
                    pod.with_state(PodState::Dead)
                        .with_metadata("reapedBy", my_name.to_owned())
                        .to_put_request(etcd_config),
                )
                .await
            }
        }
        Err(error) => {
            tracing::error!(
                pod_name,
                ?error,
                "could not parse pod resource definition, abandoning..."
            );
            Ok(false)
        }
    }
}

/// Atomically check if a resource hasn't been updated since we checked it and,
/// if so, update it.
async fn txn_check_and_set(
    mut kv_client: kv_client::KvClient<Channel>,
    version: i64,
    req: PutRequest,
) -> Result<bool, Error> {
    let compare = vec![Compare {
        result: compare::CompareResult::Equal.into(),
        target: compare::CompareTarget::Version.into(),
        key: req.key.clone(),
        target_union: Some(compare::TargetUnion::Version(version)),
        ..Default::default()
    }];

    let success = vec![RequestOp {
        request: Some(request_op::Request::RequestPut(req)),
    }];

    let res = kv_client
        .txn(Request::new(TxnRequest {
            compare,
            success,
            failure: Vec::new(),
        }))
        .await?;

    Ok(res.into_inner().succeeded)
}
