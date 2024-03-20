use std::time::Duration;
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tonic::Request;

use nodelib::error::*;
use nodelib::etcd;
use nodelib::etcd::pb::etcdserverpb::compare;
use nodelib::etcd::pb::etcdserverpb::kv_client;
use nodelib::etcd::pb::etcdserverpb::request_op;
use nodelib::etcd::pb::etcdserverpb::{Compare, PutRequest, RangeRequest, RequestOp, TxnRequest};
use nodelib::etcd::prefix;
use nodelib::resources::pod::{PodResource, PodState};

use crate::types::*;

/// Interval to retry pod that could not be reaped.
pub static RETRY_INTERVAL: u64 = 60;

/// Background task to reap dead pods.
pub async fn task(
    etcd_config: etcd::Config,
    my_name: String,
    reap_pod_tx: mpsc::UnboundedSender<PodName>,
    mut reap_pod_rx: mpsc::UnboundedReceiver<PodName>,
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
            msg = reap_pod_rx.recv() => {
                let name = msg.unwrap();
                if !reap_pod(&etcd_config, &my_name, &name).await {
                    to_retry.push(name);
                }
            }
            _ = retry_rx.recv() => {
                enqueue_retries(to_retry, &reap_pod_tx);
                to_retry = Vec::new();
            }
        }
    }
}

/// Reap a single pod.  Returns `false` if it needs to retry.
async fn reap_pod(etcd_config: &etcd::Config, my_name: &str, pod_name: &PodName) -> bool {
    tracing::info!(pod_name = pod_name.0, "got reap request");
    match mark_pod_as_dead(etcd_config, my_name, pod_name).await {
        Ok(true) => {
            tracing::info!(pod_name = pod_name.0, "reaped pod");
            true
        }
        Ok(false) => {
            tracing::info!(pod_name = pod_name.0, "pod already reaped");
            true
        }
        Err(error) => {
            tracing::warn!(
                pod_name = pod_name.0,
                ?error,
                "could not reap pod, retrying..."
            );
            false
        }
    }
}

/// Queue up all of the pods in need of retrying.
fn enqueue_retries(to_retry: Vec<PodName>, reap_pod_tx: &mpsc::UnboundedSender<PodName>) {
    for pod_name in to_retry {
        tracing::info!(pod_name = pod_name.0, "retrying unreaped pod");
        reap_pod_tx
            .send(pod_name)
            .expect("could not send to unbounded channel");
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Mark a pod as dead - if it's not in a terminal state, and nobody else has
/// reaped it yet.
async fn mark_pod_as_dead(
    etcd_config: &etcd::Config,
    my_name: &str,
    pod_name: &PodName,
) -> Result<bool, Error> {
    let key = format!(
        "{prefix}{pod_name}",
        prefix = prefix::resource(etcd_config, "pod"),
        pod_name = pod_name.0,
    );

    let mut kv_client = etcd_config.kv_client().await?;
    let res = kv_client
        .range(Request::new(RangeRequest {
            key: key.clone().into(),
            limit: 1,
            ..Default::default()
        }))
        .await?
        .into_inner();

    if res.kvs.is_empty() {
        return Ok(false);
    }

    let mod_revision = res.kvs[0].mod_revision;
    let bytes = res.kvs[0].value.clone();
    match PodResource::try_from(bytes) {
        Ok(pod) => {
            if pod.metadata.contains_key("reapedBy") || pod.state.unwrap().is_terminal() {
                Ok(false)
            } else {
                txn_check_and_set(
                    kv_client,
                    mod_revision,
                    pod.with_state(PodState::Dead)
                        .with_metadata("reapedBy", my_name.to_owned())
                        .to_put_request(etcd_config),
                )
                .await
            }
        }
        Err(error) => {
            tracing::error!(
                pod_name = pod_name.0,
                ?error,
                "could not parse pod resource definition, abandoning..."
            );
            Ok(false)
        }
    }
}

/// Atomically check if a resource hasn't been updated since we checked it and,
/// if so, update it.
async fn txn_check_and_set(
    mut kv_client: kv_client::KvClient<Channel>,
    mod_revision: i64,
    put_req: PutRequest,
) -> Result<bool, Error> {
    let compare = vec![Compare {
        result: compare::CompareResult::Equal.into(),
        target: compare::CompareTarget::Mod.into(),
        key: put_req.key.clone(),
        target_union: Some(compare::TargetUnion::ModRevision(mod_revision)),
        ..Default::default()
    }];

    let success = vec![RequestOp {
        request: Some(request_op::Request::RequestPut(put_req)),
    }];

    let res = kv_client
        .txn(Request::new(TxnRequest {
            compare,
            success,
            failure: Vec::new(),
        }))
        .await?;

    Ok(res.into_inner().succeeded)
}
