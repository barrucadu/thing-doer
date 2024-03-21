use std::process;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};
use tonic::{Request, Streaming};

use crate::error::{Error, StreamingError};
use crate::etcd;
use crate::etcd::config::Config;
use crate::etcd::pb::etcdserverpb::watch_request::RequestUnion;
use crate::etcd::pb::etcdserverpb::{WatchCreateRequest, WatchRequest, WatchResponse};
use crate::etcd::pb::mvccpb::{event::EventType, Event};
use crate::etcd::prefix;

/// The maximum number of retries in case of failure to watch.
pub static MAXIMUM_RETRIES: u32 = 10;

/// Exit code in case the watch process loses connection to etcd and cannot
/// reestablish the watch.
pub static EXIT_CODE_WATCH_FAILED: i32 = 4;

/// Trait for things which can respond to changes in the etcd state.
pub trait Watcher {
    /// Apply the state change that a watch event corresponds to.
    fn apply_event(&mut self, event: Event) -> impl std::future::Future<Output = ()> + Send;
}

/// Fetch the current state of the cluster from etcd, and establish a watch.
///
/// If a watch cannot be established, or gets dropped and cannot be
/// reestablished, this terminates the process.
pub async fn setup_watcher<W: Watcher + Send + Sync + 'static>(
    config: &Config,
    watcher: Arc<RwLock<W>>,
    key_prefixes: Vec<String>,
) -> Result<(), Error> {
    let mut start_revision = 0;
    for key_prefix in &key_prefixes {
        start_revision =
            scan_initial_state(config, &watcher, key_prefix.clone(), start_revision).await?;
    }

    tokio::spawn(watcher_task(
        config.clone(),
        watcher,
        key_prefixes,
        start_revision,
    ));

    Ok(())
}

///////////////////////////////////////////////////////////////////////////////

/// Scan through the existing cluster state in etcd, updating our watcher.
async fn scan_initial_state<W: Watcher>(
    config: &Config,
    watcher: &Arc<RwLock<W>>,
    key_prefix: String,
    revision: i64,
) -> Result<i64, Error> {
    let (kvs, revision) = etcd::util::list_kvs(config, key_prefix, revision).await?;

    let mut inner = watcher.write().await;
    for kv in kvs {
        inner
            .apply_event(Event {
                r#type: EventType::Put.into(),
                kv: Some(kv),
                ..Default::default()
            })
            .await;
    }

    Ok(revision)
}

/// Monitor for the creation and destruction of keys under the prefix and update
/// the watcher.
async fn watcher_task<W: Watcher + Send + Sync>(
    config: Config,
    watcher: Arc<RwLock<W>>,
    key_prefixes: Vec<String>,
    mut start_revision: i64,
) {
    let mut retries: u32 = 0;
    while let Err((rev, error)) =
        watcher_loop(&config, &watcher, &key_prefixes, start_revision).await
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
    key_prefixes: &Vec<String>,
    start_revision: i64,
) -> Result<(), (i64, Error)> {
    let mut rev = start_revision;
    let mut response_stream = setup_watch(config, key_prefixes, start_revision)
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
                    inner.apply_event(event).await;
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
    key_prefixes: &Vec<String>,
    start_revision: i64,
) -> Result<Streaming<WatchResponse>, Error> {
    let mut watch_client = config.watch_client().await?;

    let (tx, rx) = mpsc::unbounded_channel();

    for key_prefix in key_prefixes {
        tracing::info!(key_prefix, "establishing watch");
        tx.send(WatchRequest {
            request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
                key: key_prefix.clone().into(),
                range_end: prefix::range_end(key_prefix),
                start_revision,
                ..Default::default()
            })),
        })
        .expect("could not send to unbounded channel");
    }

    let response_stream = watch_client
        .watch(Request::new(UnboundedReceiverStream::new(rx)))
        .await?
        .into_inner();

    Ok(response_stream)
}
