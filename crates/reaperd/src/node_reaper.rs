use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tonic::Request;

use nodelib::error::*;
use nodelib::etcd;
use nodelib::etcd::pb::etcdserverpb::range_request::{SortOrder, SortTarget};
use nodelib::etcd::pb::etcdserverpb::{DeleteRangeRequest, RangeRequest};
use nodelib::etcd::pb::mvccpb::{event::EventType, Event};
use nodelib::etcd::prefix;
use nodelib::etcd::watcher;
use nodelib::resources::node::NodeType;

/// Interval to retry pod that could not be reaped.
pub static RETRY_INTERVAL: u64 = 60;

/// Set up a watcher for new dead pods, and the background tasks to reap them.
pub async fn initialise(
    etcd_config: etcd::Config,
    reap_pod_tx: mpsc::UnboundedSender<String>,
) -> Result<(), Error> {
    let (reap_node_tx, reap_node_rx) = mpsc::unbounded_channel();

    let state = Arc::new(RwLock::new(WatchState {
        etcd_config: etcd_config.clone(),
        reap_node_tx: reap_node_tx.clone(),
    }));

    watcher::setup_watcher(
        &etcd_config,
        state.clone(),
        vec![prefix::node_heartbeat_alive(&etcd_config, NodeType::Worker)],
    )
    .await?;

    tokio::spawn(task(etcd_config, reap_node_rx, reap_node_tx, reap_pod_tx));

    Ok(())
}

/// State to reap nodes.
#[derive(Debug)]
struct WatchState {
    pub etcd_config: etcd::Config,
    pub reap_node_tx: mpsc::UnboundedSender<String>,
}

impl watcher::Watcher for WatchState {
    async fn apply_event(&mut self, event: Event) {
        let prefix = prefix::node_heartbeat_alive(&self.etcd_config, NodeType::Worker);
        let is_delete = event.r#type() == EventType::Delete;
        let kv = event.kv.unwrap();
        let key = String::from_utf8(kv.key).unwrap();

        if let Some((_, node_name)) = key.split_once(&prefix) {
            if is_delete {
                tracing::info!(node_name, "found reapable node");
                self.reap_node_tx
                    .send(node_name.to_owned())
                    .expect("could not send to unbounded channel");
            }
        } else {
            tracing::warn!(?key, "unexpected watch key");
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Background task to reap dead nodes.
async fn task(
    etcd_config: etcd::Config,
    mut reap_node_rx: mpsc::UnboundedReceiver<String>,
    reap_node_tx: mpsc::UnboundedSender<String>,
    reap_pod_tx: mpsc::UnboundedSender<String>,
) {
    let mut to_retry = Vec::new();
    loop {
        tokio::select! {
            msg = reap_node_rx.recv() => {
                let node_name = msg.unwrap();
                if !reap_node_pods(&etcd_config, &reap_pod_tx, &node_name).await {
                    to_retry.push(node_name);
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(RETRY_INTERVAL)) => {
                enqueue_retries(to_retry, &reap_node_tx).await;
                to_retry = Vec::new();
            }
        }
    }
}

/// Queue up all a node's pods to reap.  Returns `false` if it needs to retry.
async fn reap_node_pods(
    etcd_config: &etcd::Config,
    reap_pod_tx: &mpsc::UnboundedSender<String>,
    node_name: &str,
) -> bool {
    tracing::info!(node_name, "got reap request");
    match drain_node_inbox(etcd_config, reap_pod_tx.clone(), node_name).await {
        Ok(true) => {
            tracing::info!(node_name, "reaped node");
            true
        }
        Ok(false) => {
            tracing::info!(node_name, "node already reaped");
            true
        }
        Err(error) => {
            tracing::warn!(node_name, ?error, "could not reap node, retrying...");
            false
        }
    }
}

/// Queue up all of the nodes in need of retrying.
async fn enqueue_retries(to_retry: Vec<String>, reap_node_tx: &mpsc::UnboundedSender<String>) {
    for node_name in to_retry.into_iter() {
        tracing::info!(node_name, "retrying unreaped node");
        reap_node_tx
            .send(node_name)
            .expect("could not send to unbounded channel");
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Enqueue all pods in a node's inbox to be marked as dead.
pub async fn drain_node_inbox(
    etcd_config: &etcd::Config,
    reap_pod_tx: mpsc::UnboundedSender<String>,
    node_name: &str,
) -> Result<bool, Error> {
    let to_reap = get_inbox_for_node(etcd_config, node_name).await?;

    if to_reap.is_empty() {
        return Ok(false);
    }

    let mut kv_client = etcd_config.kv_client().await?;
    for pod_name in to_reap {
        tracing::info!(node_name, pod_name, "found reapable pod");
        reap_pod_tx
            .send(pod_name.to_owned())
            .expect("could not send to unbounded channel");
        kv_client
            .delete_range(Request::new(DeleteRangeRequest {
                key: format!(
                    "{prefix}{pod_name}",
                    prefix = prefix::worker_inbox(etcd_config, node_name)
                )
                .into(),
                ..Default::default()
            }))
            .await?;
    }

    Ok(true)
}

/// Get all pods in a worker's inbox (i.e. pods that have been scheduled but not
/// picked up).
async fn get_inbox_for_node(
    etcd_config: &etcd::Config,
    node_name: &str,
) -> Result<Vec<String>, Error> {
    let mut kv_client = etcd_config.kv_client().await?;

    let key_prefix = prefix::worker_inbox(etcd_config, node_name);
    let range_end = prefix::range_end(&key_prefix);

    let mut to_reap = Vec::with_capacity(128);
    let mut key = key_prefix.as_bytes().to_vec();
    let mut revision = 0;
    loop {
        let response = kv_client
            .range(Request::new(RangeRequest {
                key,
                range_end: range_end.clone(),
                revision,
                sort_order: SortOrder::Ascend.into(),
                sort_target: SortTarget::Key.into(),
                ..Default::default()
            }))
            .await?
            .into_inner();
        revision = response.header.unwrap().revision;

        for kv in &response.kvs {
            let pod_key = String::from_utf8(kv.key.clone()).unwrap();
            let (_, pod_name) = pod_key.split_once(&key_prefix).unwrap();
            to_reap.push(pod_name.to_owned());
        }

        if response.more {
            let idx = response.kvs.len() - 1;
            key = response.kvs[idx].key.clone();
        } else {
            break;
        }
    }

    Ok(to_reap)
}
