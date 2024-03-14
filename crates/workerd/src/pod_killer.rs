use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;

use nodelib::error::*;
use nodelib::etcd;
use nodelib::etcd::pb::mvccpb::{event::EventType, Event};
use nodelib::etcd::prefix;
use nodelib::etcd::watcher;
use nodelib::resources::pod::PodType;

/// Set up a watcher and background tasks to kill our pods if the claims are deleted.
pub async fn initialise(
    etcd_config: etcd::Config,
    kill_pod_tx: mpsc::UnboundedSender<String>,
) -> Result<(), Error> {
    let state = Arc::new(RwLock::new(WatchState {
        etcd_config: etcd_config.clone(),
        kill_pod_tx,
    }));

    watcher::setup_watcher(
        &etcd_config,
        state.clone(),
        vec![prefix::resource(&etcd_config, &PodType::Pod.to_string())],
    )
    .await?;

    Ok(())
}

/// State to schedule pods.
#[derive(Debug)]
struct WatchState {
    etcd_config: etcd::Config,
    kill_pod_tx: mpsc::UnboundedSender<String>,
}

impl watcher::Watcher for WatchState {
    async fn apply_event(&mut self, event: Event) {
        let pod_resource_prefix = prefix::resource(&self.etcd_config, &PodType::Pod.to_string());
        let is_delete = event.r#type() == EventType::Delete;
        let kv = event.kv.unwrap();
        let key = String::from_utf8(kv.key).unwrap();

        if let Some((_, name)) = key.split_once(&pod_resource_prefix) {
            if is_delete {
                self.kill_pod_tx
                    .send(name.to_owned())
                    .expect("could not send to unbounded channel");
            }
        } else {
            tracing::warn!(?key, "unexpected watch key");
        }
    }
}
