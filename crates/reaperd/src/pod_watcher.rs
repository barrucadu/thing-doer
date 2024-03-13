use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::RwLock;

use nodelib::error::*;
use nodelib::etcd;
use nodelib::etcd::pb::mvccpb::{event::EventType, Event};
use nodelib::etcd::prefix;
use nodelib::etcd::watcher;

use crate::reaper::mark_pod_as_dead;

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

/// State to reap nodes.
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
