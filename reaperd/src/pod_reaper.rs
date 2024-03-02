use serde_json::Value;
use std::collections::HashSet;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tonic::Request;

use nodelib::etcd;
use nodelib::etcd::pb::etcdserverpb::compare;
use nodelib::etcd::pb::etcdserverpb::kv_client;
use nodelib::etcd::pb::etcdserverpb::request_op;
use nodelib::etcd::pb::etcdserverpb::{Compare, PutRequest, RangeRequest, RequestOp, TxnRequest};
use nodelib::etcd::pb::mvccpb::{event::EventType, Event};
use nodelib::etcd::prefix;
use nodelib::etcd::watcher;
use nodelib::util;
use nodelib::Error;

/// Exit code in case the reaper channel closes.
pub static EXIT_CODE_REAPER_FAILED: i32 = 1;

/// Interval to retry pod that could not be reaped.
pub static RETRY_INTERVAL: u64 = 60;

/// Set up a watcher for new dead pods, and the background tasks to reap them.
pub async fn initialise(etcd_config: etcd::Config, my_name: String) -> Result<(), Error> {
    let (reap_pod_tx, reap_pod_rx) = mpsc::channel(128);

    let state = Arc::new(RwLock::new(WatchState {
        etcd_config: etcd_config.clone(),
        my_name,
        reap_pod_tx,
        unreaped_pods: HashSet::new(),
    }));

    watcher::setup_watcher(
        &etcd_config,
        state.clone(),
        prefix::claimed_pods(&etcd_config),
    )
    .await?;

    tokio::spawn(reap_task(state.clone(), reap_pod_rx));
    tokio::spawn(retry_task(state.clone()));

    Ok(())
}

/// State to reap nodes.
#[derive(Debug)]
struct WatchState {
    pub my_name: String,
    pub etcd_config: etcd::Config,
    pub reap_pod_tx: Sender<String>,
    pub unreaped_pods: HashSet<String>,
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
                self.unreaped_pods.insert(name.to_owned());
                if let Err(error) = self.reap_pod_tx.try_send(name.to_owned()) {
                    tracing::warn!(name, ?error, "could not trigger reaper");
                }
            }
        } else {
            tracing::warn!(?key, "unexpected watch key");
        }
    }
}

/// Background task to queue up all unreaped pods every `RETRY_INTERVAL`
/// seconds.
async fn retry_task(state: Arc<RwLock<WatchState>>) {
    loop {
        tokio::time::sleep(Duration::from_secs(RETRY_INTERVAL)).await;

        let pods_to_retry = {
            let r = state.read().await;
            r.unreaped_pods.iter().cloned().collect::<Vec<_>>()
        };

        if !pods_to_retry.is_empty() {
            tracing::info!(count = ?pods_to_retry.len(), "retrying unreaped pods");
            let w = state.write().await;
            for name in pods_to_retry {
                tracing::info!(name, "retrying unreaped pod");
                if let Err(error) = w.reap_pod_tx.try_send(name.clone()) {
                    tracing::warn!(name, ?error, "could not trigger reaper");
                }
            }
        }
    }
}

/// Background task to reap pods.
async fn reap_task(state: Arc<RwLock<WatchState>>, mut reap_pod_rx: Receiver<String>) {
    while let Some(pod_name) = reap_pod_rx.recv().await {
        let mut w = state.write().await;
        tracing::info!(pod_name, "got reap request");
        match reap_pod(&mut w, pod_name.to_owned()).await {
            Ok(true) => {
                tracing::info!(pod_name, "reaped pod");
                w.unreaped_pods.remove(&pod_name);
            }
            Ok(false) => {
                tracing::info!(pod_name, "pod already reaped");
                w.unreaped_pods.remove(&pod_name);
            }
            Err(error) => {
                tracing::warn!(pod_name, ?error, "could not reap pod, retrying...");
            }
        }
    }

    tracing::error!("reaper channel unexpectedly closed, termianting...");
    process::exit(EXIT_CODE_REAPER_FAILED);
}

/// Mark a pod as dead
async fn reap_pod(state: &mut WatchState, pod_name: String) -> Result<bool, Error> {
    let key = format!(
        "{prefix}{pod_name}",
        prefix = prefix::resource(&state.etcd_config, "pod")
    );

    let mut kv_client = state.etcd_config.kv_client().await?;
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
    match util::bytes_to_json(bytes) {
        Some(mut pod_resource) => {
            if pod_resource["metadata"]["reapedBy"].as_str().is_some() {
                Ok(false)
            } else {
                pod_resource["state"] = "dead".into();
                pod_resource["metadata"]["reapedBy"] = state.my_name.clone().into();
                txn_check_and_set(kv_client, version, key, pod_resource).await
            }
        }
        None => {
            tracing::error!(
                pod_name,
                "could not parse pod resource definition, abandoning..."
            );
            Ok(false)
        }
    }
}

/// Atomically check if a key hasn't been updated since we checked it and, if
/// so, update it.
async fn txn_check_and_set(
    mut kv_client: kv_client::KvClient<Channel>,
    version: i64,
    key: String,
    value: Value,
) -> Result<bool, Error> {
    let compare = vec![Compare {
        result: compare::CompareResult::Equal.into(),
        target: compare::CompareTarget::Version.into(),
        key: key.clone().into(),
        target_union: Some(compare::TargetUnion::Version(version)),
        ..Default::default()
    }];

    let success = vec![RequestOp {
        request: Some(request_op::Request::RequestPut(PutRequest {
            key: key.into(),
            value: value.to_string().into(),
            ..Default::default()
        })),
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
