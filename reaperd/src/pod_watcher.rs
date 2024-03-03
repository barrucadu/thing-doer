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

use crate::reaper::reap_pod;

/// Exit code in case the reaper channel closes.
pub static EXIT_CODE_REAPER_FAILED: i32 = 1;

/// Interval to retry pod that could not be reaped.
pub static RETRY_INTERVAL: u64 = 60;

/// Set up a watcher for new dead pods, and the background tasks to reap them.
pub async fn initialise(
    etcd_config: etcd::Config,
    my_name: String,
) -> Result<Sender<String>, Error> {
    let (reap_pod_tx, reap_pod_rx) = mpsc::channel(128);

    let state = Arc::new(RwLock::new(WatchState {
        etcd_config: etcd_config.clone(),
        my_name,
        reap_pod_tx: reap_pod_tx.clone(),
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

    Ok(reap_pod_tx)
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

        let to_retry = {
            let r = state.read().await;
            r.unreaped_pods.iter().cloned().collect::<Vec<_>>()
        };

        if !to_retry.is_empty() {
            tracing::info!(count = ?to_retry.len(), "retrying unreaped pods");
            let w = state.write().await;
            for name in to_retry {
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
        match reap_pod(&w.etcd_config, &w.my_name, &pod_name).await {
            Ok(reaped) => {
                if reaped {
                    tracing::info!(pod_name, "reaped pod");
                }
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
