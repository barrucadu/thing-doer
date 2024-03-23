use std::time::Duration;
use tokio::sync::mpsc;
use tonic::Request;

use nodelib::error::*;
use nodelib::etcd;
use nodelib::etcd::pb::etcdserverpb::DeleteRangeRequest;
use nodelib::etcd::prefix;

use crate::types::*;

/// Interval to retry pod that could not be reaped.
pub static RETRY_INTERVAL: u64 = 60;

/// Background task to reap dead nodes.
pub async fn task(
    etcd_config: etcd::Config,
    reap_node_tx: mpsc::UnboundedSender<NodeName>,
    mut reap_node_rx: mpsc::UnboundedReceiver<NodeName>,
    reap_pod_tx: mpsc::UnboundedSender<PodName>,
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
            msg = reap_node_rx.recv() => {
                let node_name = msg.unwrap();
                if !reap_node_pods(&etcd_config, &reap_pod_tx, &node_name).await {
                    to_retry.push(node_name);
                }
            }
            _ = retry_rx.recv() => {
                enqueue_retries(to_retry, &reap_node_tx);
                to_retry = Vec::new();
            }
        }
    }
}

/// Queue up all a node's pods to reap.  Returns `false` if it needs to retry.
async fn reap_node_pods(
    etcd_config: &etcd::Config,
    reap_pod_tx: &mpsc::UnboundedSender<PodName>,
    node_name: &NodeName,
) -> bool {
    tracing::info!(node_name = node_name.0, "got reap request");
    match drain_node_inbox(etcd_config, reap_pod_tx.clone(), node_name).await {
        Ok(true) => {
            tracing::info!(node_name = node_name.0, "reaped node");
            true
        }
        Ok(false) => {
            tracing::info!(node_name = node_name.0, "node already reaped");
            true
        }
        Err(error) => {
            tracing::warn!(
                node_name = node_name.0,
                ?error,
                "could not reap node, retrying..."
            );
            false
        }
    }
}

/// Queue up all of the nodes in need of retrying.
fn enqueue_retries(to_retry: Vec<NodeName>, reap_node_tx: &mpsc::UnboundedSender<NodeName>) {
    for node_name in to_retry {
        tracing::info!(node_name = node_name.0, "retrying unreaped node");
        reap_node_tx
            .send(node_name)
            .expect("could not send to unbounded channel");
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Enqueue all pods in a node's inbox to be marked as dead.
pub async fn drain_node_inbox(
    etcd_config: &etcd::Config,
    reap_pod_tx: mpsc::UnboundedSender<PodName>,
    node_name: &NodeName,
) -> Result<bool, Error> {
    let to_reap = get_inbox_for_node(etcd_config, node_name).await?;

    if to_reap.is_empty() {
        return Ok(false);
    }

    let mut kv_client = etcd_config.kv_client().await?;
    for pod_name in to_reap {
        tracing::info!(
            node_name = node_name.0,
            pod_name = pod_name.0,
            "found reapable pod"
        );
        reap_pod_tx
            .send(pod_name.clone())
            .expect("could not send to unbounded channel");
        kv_client
            .delete_range(Request::new(DeleteRangeRequest {
                key: format!(
                    "{prefix}{pod_name}",
                    prefix = prefix::worker_inbox(etcd_config, &node_name.0),
                    pod_name = pod_name.0
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
    node_name: &NodeName,
) -> Result<Vec<PodName>, Error> {
    let key_prefix = prefix::worker_inbox(etcd_config, &node_name.0);
    let (kvs, _) = etcd::util::list_kvs(etcd_config, key_prefix.clone(), None).await?;

    let mut to_reap = Vec::with_capacity(kvs.len());
    for kv in kvs {
        let pod_key = String::from_utf8(kv.key).unwrap();
        let (_, pod_name) = pod_key.split_once(&key_prefix).unwrap();
        to_reap.push(PodName::from(pod_name));
    }

    Ok(to_reap)
}
