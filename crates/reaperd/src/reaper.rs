use tokio::sync::mpsc;
use tonic::transport::Channel;
use tonic::Request;

use nodelib::error::*;
use nodelib::etcd;
use nodelib::etcd::pb::etcdserverpb::compare;
use nodelib::etcd::pb::etcdserverpb::kv_client;
use nodelib::etcd::pb::etcdserverpb::range_request::{SortOrder, SortTarget};
use nodelib::etcd::pb::etcdserverpb::request_op;
use nodelib::etcd::pb::etcdserverpb::{
    Compare, DeleteRangeRequest, PutRequest, RangeRequest, RequestOp, TxnRequest,
};
use nodelib::etcd::prefix;
use nodelib::resources::pod::*;

/// Mark all inboxed pods on a node as dead
pub async fn drain_node_inbox(
    etcd_config: &etcd::Config,
    reap_pod_tx: mpsc::UnboundedSender<String>,
    node_name: &str,
) -> Result<bool, Error> {
    let to_reap = get_inbox_for_node(etcd_config, node_name).await?;

    if to_reap.is_empty() {
        return Ok(false);
    }

    let mut kv_client = etcd_config.kv_client().await?;
    for pod_name in to_reap {
        tracing::info!(node_name, pod_name, "found reapable pod");
        reap_pod_tx
            .send(pod_name.to_owned())
            .expect("could not send to unbounded channel");
        kv_client
            .delete_range(Request::new(DeleteRangeRequest {
                key: format!(
                    "{prefix}{pod_name}",
                    prefix = prefix::worker_inbox(etcd_config, node_name)
                )
                .into(),
                ..Default::default()
            }))
            .await?;
    }

    Ok(true)
}

/// Mark a pod as dead
pub async fn mark_pod_as_dead(
    etcd_config: &etcd::Config,
    my_name: &str,
    pod_name: &str,
) -> Result<bool, Error> {
    let key = format!(
        "{prefix}{pod_name}",
        prefix = prefix::resource(etcd_config, "pod")
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

    let version = res.kvs[0].version;
    let bytes = res.kvs[0].value.clone();
    match PodResource::try_from(bytes) {
        Ok(pod) => {
            if pod.metadata.contains_key("reapedBy") || pod.state.unwrap().is_terminal() {
                Ok(false)
            } else {
                txn_check_and_set(
                    kv_client,
                    version,
                    pod.with_state(PodState::Dead)
                        .with_metadata("reapedBy", my_name.to_owned())
                        .to_put_request(etcd_config),
                )
                .await
            }
        }
        Err(error) => {
            tracing::error!(
                pod_name,
                ?error,
                "could not parse pod resource definition, abandoning..."
            );
            Ok(false)
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Get all pods in a worker's inbox (i.e. pods that have been scheduled but not
/// picked up).
async fn get_inbox_for_node(
    etcd_config: &etcd::Config,
    node_name: &str,
) -> Result<Vec<String>, Error> {
    let mut kv_client = etcd_config.kv_client().await?;

    let key_prefix = prefix::worker_inbox(etcd_config, node_name);
    let range_end = prefix::range_end(&key_prefix);

    let mut to_reap = Vec::with_capacity(128);
    let mut key = key_prefix.as_bytes().to_vec();
    let mut revision = 0;
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

        for kv in &response.kvs {
            let pod_key = String::from_utf8(kv.key.clone()).unwrap();
            let (_, pod_name) = pod_key.split_once(&key_prefix).unwrap();
            to_reap.push(pod_name.to_owned());
        }

        if response.more {
            let idx = response.kvs.len() - 1;
            key = response.kvs[idx].key.clone();
        } else {
            break;
        }
    }

    Ok(to_reap)
}

/// Atomically check if a resource hasn't been updated since we checked it and,
/// if so, update it.
async fn txn_check_and_set(
    mut kv_client: kv_client::KvClient<Channel>,
    version: i64,
    req: PutRequest,
) -> Result<bool, Error> {
    let compare = vec![Compare {
        result: compare::CompareResult::Equal.into(),
        target: compare::CompareTarget::Version.into(),
        key: req.key.clone(),
        target_union: Some(compare::TargetUnion::Version(version)),
        ..Default::default()
    }];

    let success = vec![RequestOp {
        request: Some(request_op::Request::RequestPut(req)),
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
