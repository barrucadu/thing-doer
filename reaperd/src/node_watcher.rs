use std::collections::HashSet;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;

use nodelib::etcd;
use nodelib::etcd::pb::mvccpb::{event::EventType, Event};
use nodelib::etcd::prefix;
use nodelib::etcd::watcher;
use nodelib::Error;

use crate::reaper::reap_node_inbox;

/// Exit code in case the reaper channel closes.
pub static EXIT_CODE_REAPER_FAILED: i32 = 1;

/// Interval to retry pod that could not be reaped.
pub static RETRY_INTERVAL: u64 = 60;

/// Set up a watcher for new dead pods, and the background tasks to reap them.
pub async fn initialise(
    etcd_config: etcd::Config,
    reap_pod_tx: Sender<String>,
) -> Result<(), Error> {
    let (reap_node_tx, reap_node_rx) = mpsc::channel(128);

    let state = Arc::new(RwLock::new(WatchState {
        etcd_config: etcd_config.clone(),
        reap_node_tx,
        reap_pod_tx,
        unreaped_nodes: HashSet::new(),
    }));

    watcher::setup_watcher(
        &etcd_config,
        state.clone(),
        prefix::node_heartbeat_alive(&etcd_config),
    )
    .await?;

    tokio::spawn(reap_task(state.clone(), reap_node_rx));
    tokio::spawn(retry_task(state.clone()));

    Ok(())
}

/// State to reap nodes.
#[derive(Debug)]
struct WatchState {
    pub etcd_config: etcd::Config,
    pub reap_node_tx: Sender<String>,
    pub reap_pod_tx: Sender<String>,
    pub unreaped_nodes: HashSet<String>,
}

impl watcher::Watcher for WatchState {
    async fn apply_event(&mut self, event: Event) {
        let prefix = prefix::node_heartbeat_alive(&self.etcd_config);
        let is_delete = event.r#type() == EventType::Delete;
        let kv = event.kv.unwrap();
        let key = String::from_utf8(kv.key).unwrap();

        if let Some((_, name)) = key.split_once(&prefix) {
            if is_delete {
                tracing::info!(name, "found reapable node");
                self.unreaped_nodes.insert(name.to_owned());
                if let Err(error) = self.reap_node_tx.try_send(name.to_owned()) {
                    tracing::warn!(name, ?error, "could not trigger reaper");
                }
            }
        } else {
            tracing::warn!(?key, "unexpected watch key");
        }
    }
}

/// Background task to queue up all unreaped nodes every `RETRY_INTERVAL`
/// seconds.
async fn retry_task(state: Arc<RwLock<WatchState>>) {
    loop {
        tokio::time::sleep(Duration::from_secs(RETRY_INTERVAL)).await;

        let to_retry = {
            let r = state.read().await;
            r.unreaped_nodes.iter().cloned().collect::<Vec<_>>()
        };

        if !to_retry.is_empty() {
            tracing::info!(count = ?to_retry.len(), "retrying unreaped nodes");
            let w = state.write().await;
            for name in to_retry {
                tracing::info!(name, "retrying unreaped node");
                if let Err(error) = w.reap_node_tx.try_send(name.clone()) {
                    tracing::warn!(name, ?error, "could not trigger reaper");
                }
            }
        }
    }
}

/// Background task to reap nodes.
async fn reap_task(state: Arc<RwLock<WatchState>>, mut reap_node_rx: Receiver<String>) {
    while let Some(node_name) = reap_node_rx.recv().await {
        let mut w = state.write().await;
        tracing::info!(node_name, "got reap request");
        match reap_node_inbox(&w.etcd_config, w.reap_pod_tx.clone(), &node_name).await {
            Ok(true) => {
                tracing::info!(node_name, "reaped node");
                w.unreaped_nodes.remove(&node_name);
            }
            Ok(false) => {
                tracing::info!(node_name, "node already reaped");
                w.unreaped_nodes.remove(&node_name);
            }
            Err(error) => {
                tracing::warn!(node_name, ?error, "could not reap node, retrying...");
            }
        }
    }

    tracing::error!("reaper channel unexpectedly closed, termianting...");
    process::exit(EXIT_CODE_REAPER_FAILED);
}
