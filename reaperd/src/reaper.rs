use serde_json::Value;
use tokio::sync::mpsc::Sender;
use tonic::transport::Channel;
use tonic::Request;

use nodelib::etcd;
use nodelib::etcd::pb::etcdserverpb::compare;
use nodelib::etcd::pb::etcdserverpb::kv_client;
use nodelib::etcd::pb::etcdserverpb::range_request::{SortOrder, SortTarget};
use nodelib::etcd::pb::etcdserverpb::request_op;
use nodelib::etcd::pb::etcdserverpb::{
    Compare, DeleteRangeRequest, PutRequest, RangeRequest, RequestOp, TxnRequest,
};
use nodelib::etcd::prefix;
use nodelib::util;
use nodelib::Error;

/// Mark all inboxed pods on a node as dead
pub async fn reap_node_inbox(
    etcd_config: &etcd::Config,
    reap_pod_tx: Sender<String>,
    node_name: &str,
) -> Result<bool, Error> {
    let to_reap = get_inbox_for_node(etcd_config, node_name).await?;

    if to_reap.is_empty() {
        return Ok(false);
    }

    let mut kv_client = etcd_config.kv_client().await?;
    for name in to_reap {
        tracing::info!(name, "found reapable pod");
        let inbox_key = format!(
            "{prefix}{name}",
            prefix = prefix::worker_inbox(etcd_config, node_name)
        );
        match reap_pod_tx.try_send(name.to_owned()) {
            Ok(_) => {
                kv_client
                    .delete_range(Request::new(DeleteRangeRequest {
                        key: inbox_key.into(),
                        ..Default::default()
                    }))
                    .await?;
            }
            Err(error) => {
                tracing::warn!(name, ?error, "could not trigger reaper");
            }
        }
    }

    Ok(true)
}

/// Mark a pod as dead
pub async fn reap_pod(
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
    match util::bytes_to_json(bytes) {
        Some(mut pod_resource) => {
            if pod_resource["metadata"]["reapedBy"].as_str().is_some() {
                Ok(false)
            } else {
                pod_resource["state"] = "dead".into();
                pod_resource["metadata"]["reapedBy"] = my_name.to_owned().into();
                txn_check_and_set(kv_client, version, key, pod_resource).await
            }
        }
        None => {
            tracing::error!(
                pod_name,
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

/// Atomically check if a key hasn't been updated since we checked it and, if
/// so, update it.
async fn txn_check_and_set(
    mut kv_client: kv_client::KvClient<Channel>,
    version: i64,
    key: String,
    value: Value,
) -> Result<bool, Error> {
    let compare = vec![Compare {
        result: compare::CompareResult::Equal.into(),
        target: compare::CompareTarget::Version.into(),
        key: key.clone().into(),
        target_union: Some(compare::TargetUnion::Version(version)),
        ..Default::default()
    }];

    let success = vec![RequestOp {
        request: Some(request_op::Request::RequestPut(PutRequest {
            key: key.into(),
            value: value.to_string().into(),
            ..Default::default()
        })),
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
