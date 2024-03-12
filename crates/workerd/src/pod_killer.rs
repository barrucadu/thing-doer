use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::RwLock;

use nodelib::error::*;
use nodelib::etcd;
use nodelib::etcd::pb::mvccpb::{event::EventType, Event};
use nodelib::etcd::prefix;
use nodelib::etcd::watcher;

/// Interval to retry pods that could not be killed.
pub static RETRY_INTERVAL: u64 = 5;

/// Set up a watcher and background tasks to kill our pods if the claims are deleted.
pub async fn initialise(
    etcd_config: etcd::Config,
    kill_pod_tx: mpsc::Sender<String>,
) -> Result<(), Error> {
    let state = Arc::new(RwLock::new(WatchState {
        etcd_config: etcd_config.clone(),
        unkilled_pods: Vec::new(),
        kill_pod_tx,
    }));

    watcher::setup_watcher(
        &etcd_config,
        state.clone(),
        prefix::claimed_pods(&etcd_config),
    )
    .await?;

    tokio::spawn(retry_task(state.clone()));

    Ok(())
}

/// State to schedule pods.
#[derive(Debug)]
struct WatchState {
    pub etcd_config: etcd::Config,
    pub unkilled_pods: Vec<String>,
    pub kill_pod_tx: mpsc::Sender<String>,
}

impl watcher::Watcher for WatchState {
    async fn apply_event(&mut self, event: Event) {
        let prefix = prefix::claimed_pods(&self.etcd_config);
        let is_delete = event.r#type() == EventType::Delete;
        let kv = event.kv.unwrap();
        let key = String::from_utf8(kv.key).unwrap();

        if let Some((_, name)) = key.split_once(&prefix) {
            if is_delete {
                if let Err(error) = self.kill_pod_tx.try_send(name.to_owned()) {
                    tracing::warn!(name, ?error, "could not trigger killer");
                    self.unkilled_pods.push(name.to_owned());
                }
            }
        } else {
            tracing::warn!(?key, "unexpected watch key");
        }
    }
}

/// Background task to queue up all unkilled pods every `RETRY_INTERVAL`
/// seconds.
async fn retry_task(state: Arc<RwLock<WatchState>>) {
    loop {
        tokio::time::sleep(Duration::from_secs(RETRY_INTERVAL)).await;

        let count = {
            let r = state.read().await;
            r.unkilled_pods.len()
        };

        if count > 0 {
            tracing::info!(?count, "retrying unkilled pods");
            let mut w = state.write().await;
            let mut new = Vec::with_capacity(w.unkilled_pods.len());
            for name in &w.unkilled_pods {
                tracing::info!(name, "retrying unclaimed pod");
                if let Err(error) = w.kill_pod_tx.try_send(name.to_owned()) {
                    tracing::warn!(name, ?error, "could not trigger killer");
                    new.push(name.to_owned());
                }
            }
            w.unkilled_pods = new;
        }
    }
}
