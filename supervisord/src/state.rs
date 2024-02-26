use grpc_etcd::etcdserverpb::range_request::{SortOrder, SortTarget};
use grpc_etcd::etcdserverpb::watch_request::RequestUnion;
use grpc_etcd::etcdserverpb::{RangeRequest, WatchCreateRequest, WatchRequest, WatchResponse};
use grpc_etcd::mvccpb::{event::EventType, Event};
use serde_json::Value;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Streaming};

use crate::args::EtcdConfig;
use crate::{Error, StreamingError};

/// A handle to the shared state.
#[derive(Debug, Clone)]
pub struct State(Arc<RwLock<InnerState>>);

impl Default for State {
    fn default() -> Self {
        Self(Arc::new(RwLock::new(InnerState::default())))
    }
}

impl State {
    /// Get all known supervisor nodes and their latest state.
    pub async fn get_supervisor_nodes(&self) -> HashMap<String, NodeState> {
        let inner = self.0.read().await;
        inner.get_nodes(&inner.supervisor_nodes)
    }

    /// Get all known worker nodes and their latest state.
    pub async fn get_worker_nodes(&self) -> HashMap<String, NodeState> {
        let inner = self.0.read().await;
        inner.get_nodes(&inner.worker_nodes)
    }
}

/// The internal state, only visible to the update task and query methods.
#[derive(Debug, Default)]
struct InnerState {
    pub supervisor_nodes: HashMap<String, SocketAddr>,
    pub worker_nodes: HashMap<String, SocketAddr>,
    pub node_is_healthy: HashMap<String, bool>,
    pub node_is_alive: HashMap<String, bool>,
}

impl InnerState {
    /// Get the states of a collection of nodes.
    fn get_nodes(&self, nodes: &HashMap<String, SocketAddr>) -> HashMap<String, NodeState> {
        nodes
            .iter()
            .map(|(name, address)| (name.to_owned(), self.get_node_state(name, *address)))
            .collect()
    }

    /// Get the state of a single node.
    fn get_node_state(&self, name: &str, address: SocketAddr) -> NodeState {
        NodeState {
            address,
            is_healthy: self.node_is_healthy.get(name).copied(),
            is_alive: self.node_is_alive.get(name).copied(),
        }
    }

    /// Apply the state change that a watch event corresponds to.
    fn apply_event(&mut self, etcd_prefix: &str, event: Event) {
        let is_create = event.r#type() == EventType::Put;
        let kv = event.kv.unwrap();
        let key = String::from_utf8(kv.key).unwrap();

        if let Some((_, suffix)) = key.split_once(&format!("{etcd_prefix}/node/")) {
            if let Some((name, _)) = suffix.rsplit_once("/healthy") {
                tracing::info!(name, is_healthy = is_create, "node healthy check changed");
                self.node_is_healthy.insert(name.to_owned(), is_create);
            } else if let Some((name, _)) = suffix.rsplit_once("/alive") {
                tracing::info!(name, is_alive = is_create, "node alive check changed");
                self.node_is_alive.insert(name.to_owned(), is_create);
            } else {
                tracing::warn!(?key, "unexpected watch key");
            }
        } else if let Some((_, suffix)) = key.split_once(&format!("{etcd_prefix}/resource/node.")) {
            let json = String::from_utf8(kv.value).unwrap();
            let value: Value = serde_json::from_str(&json).unwrap();
            let address = value["spec"]["address"].as_str().unwrap().parse().unwrap();
            if let Some((_, name)) = suffix.split_once("supervisor/") {
                tracing::info!(name, ?address, "found supervisor node");
                self.supervisor_nodes.insert(name.to_owned(), address);
            } else if let Some((_, name)) = suffix.split_once("worker/") {
                tracing::info!(name, ?address, "found worker node");
                self.worker_nodes.insert(name.to_owned(), address);
            } else {
                tracing::warn!(?key, "unexpected watch key");
            }
        } else {
            tracing::warn!(?key, "unexpected watch key");
        }
    }
}

/// The state of a single node.
#[derive(Debug)]
pub struct NodeState {
    pub address: SocketAddr,
    pub is_healthy: Option<bool>,
    pub is_alive: Option<bool>,
}

///////////////////////////////////////////////////////////////////////////////

/// The maximum number of retries in case of failure to watch.
pub static MAXIMUM_RETRIES: u32 = 10;

/// Fetch the current state of the cluster from etcd, and establish watches to
/// keep the state up to date.
///
/// If a connection cannot be established to any of the configured etcd hosts,
/// this raises an error and terminates the process.
pub async fn initialise(etcd_config: EtcdConfig) -> Result<State, Error> {
    let state = State::default();

    let start_revision = scan_initial_state(&etcd_config, &state).await?;

    tokio::spawn(watcher_task(
        etcd_config.clone(),
        state.clone(),
        format!("{prefix}/node/", prefix = etcd_config.prefix),
        start_revision,
    ));
    tokio::spawn(watcher_task(
        etcd_config.clone(),
        state.clone(),
        format!("{prefix}/resource/node.", prefix = etcd_config.prefix),
        start_revision,
    ));

    Ok(state)
}

/// Scan through the existing node data in etcd, storing any we don't know about
/// in the state.
async fn scan_initial_state(etcd_config: &EtcdConfig, state: &State) -> Result<i64, Error> {
    let mut kv_client = etcd_config.kv_client().await?;
    let mut revision = 0;

    let prefixes = &[
        format!("{prefix}/node/", prefix = etcd_config.prefix),
        format!("{prefix}/resource/node.", prefix = etcd_config.prefix),
    ];

    for prefix in prefixes {
        let range_end = prefix_range_end(prefix);
        let mut key = prefix.as_bytes().to_vec();

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

            let mut inner = state.0.write().await;
            for kv in &response.kvs {
                inner.apply_event(
                    &etcd_config.prefix,
                    Event {
                        r#type: EventType::Put.into(),
                        kv: Some(kv.clone()),
                        ..Default::default()
                    },
                );
            }

            if response.more {
                let idx = response.kvs.len() - 1;
                key = response.kvs[idx].key.clone();
            } else {
                break;
            }
        }
    }

    Ok(revision)
}

/// Monitor for the creation and destruction of keys under the prefix and update
/// the `State`.
async fn watcher_task(
    etcd_config: EtcdConfig,
    state: State,
    key_prefix: String,
    mut start_revision: i64,
) {
    let mut retries: u32 = 0;
    while let Err((rev, error)) =
        watcher_loop(&etcd_config, &state, &key_prefix, start_revision).await
    {
        if rev > start_revision {
            retries = 0;
        } else {
            retries += 1;
        }

        start_revision = rev;

        if retries < MAXIMUM_RETRIES {
            tracing::warn!(?error, ?retries, "error in etcd watch");
            tokio::time::sleep(Duration::from_secs(2_u64.pow(retries))).await;
        } else {
            tracing::error!(
                ?error,
                ?retries,
                "could not establish watch with etcd, terminating..."
            );
            process::exit(crate::EXIT_CODE_WATCH_FAILED);
        }
    }

    // The above is an infinite loop.
    unreachable!();
}

/// Set up a watch and respond to events, abort on failure and let `watcher`
/// handle the retry logic.
async fn watcher_loop(
    etcd_config: &EtcdConfig,
    state: &State,
    key_prefix: &str,
    start_revision: i64,
) -> Result<(), (i64, Error)> {
    let mut rev = start_revision;
    let (_tx, mut response_stream) = setup_watch(etcd_config, key_prefix, start_revision)
        .await
        .map_err(|e| (rev, e))?;

    loop {
        match response_stream.next().await {
            Some(Ok(response)) => {
                if response.canceled {
                    return Err((rev, Error::Streaming(StreamingError::Ended)));
                }
                let mut inner = state.0.write().await;
                for event in response.events {
                    inner.apply_event(&etcd_config.prefix, event);
                }
                rev = response.header.unwrap().revision;
            }
            Some(Err(error)) => return Err((rev, error.into())),
            None => return Err((rev, Error::Streaming(StreamingError::Ended))),
        }
    }
}

/// Set up a watch for a key prefix
async fn setup_watch(
    etcd_config: &EtcdConfig,
    key_prefix: &str,
    start_revision: i64,
) -> Result<(Sender<WatchRequest>, Streaming<WatchResponse>), Error> {
    let mut watch_client = etcd_config.watch_client().await?;

    let (tx, rx) = mpsc::channel(16);

    tracing::info!(key_prefix, "establishing watch");
    tx.send(WatchRequest {
        request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
            key: key_prefix.into(),
            range_end: prefix_range_end(key_prefix),
            start_revision,
            ..Default::default()
        })),
    })
    .await
    .map_err(|_| Error::Streaming(StreamingError::CannotSend))?;

    let response_stream = watch_client
        .watch(Request::new(ReceiverStream::new(rx)))
        .await?
        .into_inner();

    Ok((tx, response_stream))
}

/// Get a `range_end` to cover all keys under the given prefix.
fn prefix_range_end(key_prefix: &str) -> Vec<u8> {
    // "If the range_end is one bit larger than the given key, then all keys
    // with the prefix (the given key) will be watched."
    let mut range_end: Vec<u8> = key_prefix.into();
    let idx = range_end.len() - 1;
    range_end[idx] += 1;

    range_end
}
