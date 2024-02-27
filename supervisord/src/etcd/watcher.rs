use std::process;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Streaming};

use crate::etcd::config::Config;
use crate::etcd::pb::etcdserverpb::range_request::{SortOrder, SortTarget};
use crate::etcd::pb::etcdserverpb::watch_request::RequestUnion;
use crate::etcd::pb::etcdserverpb::{
    RangeRequest, WatchCreateRequest, WatchRequest, WatchResponse,
};
use crate::etcd::pb::mvccpb::{event::EventType, Event};
use crate::{Error, StreamingError};

/// The maximum number of retries in case of failure to watch.
pub static MAXIMUM_RETRIES: u32 = 10;

/// Exit code in case the watch process loses connection to etcd and cannot
/// reestablish the watch.
pub static EXIT_CODE_WATCH_FAILED: i32 = 4;

/// Trait for things which can respond to changes in the etcd state.
pub trait Watcher {
    /// Apply the state change that a watch event corresponds to.
    fn apply_event(&mut self, event: Event);
}

/// Fetch the current state of the cluster from etcd, and establish a watch.
///
/// If a watch cannot be established, or gets dropped and cannot be
/// reestablished, this terminates the process.
pub async fn setup_watcher<W: Watcher + Send + Sync + 'static>(
    config: &Config,
    watcher: Arc<RwLock<W>>,
    key_prefix: String,
) -> Result<(), Error> {
    let start_revision = scan_initial_state(config, &watcher, &key_prefix).await?;

    tokio::spawn(watcher_task(
        config.clone(),
        watcher,
        key_prefix,
        start_revision,
    ));

    Ok(())
}

///////////////////////////////////////////////////////////////////////////////

/// Scan through the existing cluster state in etcd, updating our watcher.
async fn scan_initial_state<W: Watcher>(
    config: &Config,
    watcher: &Arc<RwLock<W>>,
    key_prefix: &str,
) -> Result<i64, Error> {
    let mut kv_client = config.kv_client().await?;
    let mut revision = 0;

    let range_end = prefix_range_end(key_prefix);
    let mut key = key_prefix.as_bytes().to_vec();

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

        let mut inner = watcher.write().await;
        for kv in &response.kvs {
            inner.apply_event(Event {
                r#type: EventType::Put.into(),
                kv: Some(kv.clone()),
                ..Default::default()
            });
        }

        if response.more {
            let idx = response.kvs.len() - 1;
            key = response.kvs[idx].key.clone();
        } else {
            break;
        }
    }

    Ok(revision)
}

/// Monitor for the creation and destruction of keys under the prefix and update
/// the watcher.
async fn watcher_task<W: Watcher + Send + Sync>(
    config: Config,
    watcher: Arc<RwLock<W>>,
    key_prefix: String,
    mut start_revision: i64,
) {
    let mut retries: u32 = 0;
    while let Err((rev, error)) = watcher_loop(&config, &watcher, &key_prefix, start_revision).await
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
            process::exit(EXIT_CODE_WATCH_FAILED);
        }
    }

    // The above is an infinite loop.
    unreachable!();
}

/// Set up a watch and respond to events, abort on failure and let `watcher`
/// handle the retry logic.
async fn watcher_loop<W: Watcher>(
    config: &Config,
    watcher: &Arc<RwLock<W>>,
    key_prefix: &str,
    start_revision: i64,
) -> Result<(), (i64, Error)> {
    let mut rev = start_revision;
    let (_tx, mut response_stream) = setup_watch(config, key_prefix, start_revision)
        .await
        .map_err(|e| (rev, e))?;

    loop {
        match response_stream.next().await {
            Some(Ok(response)) => {
                if response.canceled {
                    return Err((rev, Error::Streaming(StreamingError::Ended)));
                }
                let mut inner = watcher.write().await;
                for event in response.events {
                    inner.apply_event(event);
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
    config: &Config,
    key_prefix: &str,
    start_revision: i64,
) -> Result<(Sender<WatchRequest>, Streaming<WatchResponse>), Error> {
    let mut watch_client = config.watch_client().await?;

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
