use tonic::Request;

use crate::error::Error;
use crate::etcd;
use crate::etcd::pb::etcdserverpb::compare;
use crate::etcd::pb::etcdserverpb::range_request::{SortOrder, SortTarget};
use crate::etcd::pb::etcdserverpb::request_op;
use crate::etcd::pb::etcdserverpb::{
    Compare, DeleteRangeRequest, RangeRequest, RequestOp, TxnRequest,
};
use crate::etcd::pb::mvccpb::KeyValue;
use crate::etcd::prefix;

/// List all key-value pairs under the given prefix.  Everything is fetched at
/// the given revision (or the latest revision, if 0 is given), which is returned.
pub async fn list_kvs(
    config: &etcd::Config,
    key_prefix: String,
    mut revision: i64,
) -> Result<(Vec<KeyValue>, i64), Error> {
    let mut out = Vec::new();

    let mut kv_client = config.kv_client().await?;

    let range_end = prefix::range_end(&key_prefix);
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

        for kv in &response.kvs {
            out.push(kv.clone());
        }

        if response.more {
            let idx = response.kvs.len() - 1;
            key = response.kvs[idx].key.clone();
        } else {
            break;
        }
    }

    Ok((out, revision))
}

/// Delete a key.  Returns `false` if the key does not exist.
pub async fn delete_if_exists(config: &etcd::Config, key: String) -> Result<bool, Error> {
    let mut kv_client = config.kv_client().await?;
    let res = kv_client
        .txn(Request::new(TxnRequest {
            // create revision (ie `Create`) != 0 => the key exists
            compare: vec![Compare {
                result: compare::CompareResult::NotEqual.into(),
                target: compare::CompareTarget::Create.into(),
                key: key.clone().into(),
                ..Default::default()
            }],
            success: vec![RequestOp {
                request: Some(request_op::Request::RequestDeleteRange(
                    DeleteRangeRequest {
                        key: key.into(),
                        ..Default::default()
                    },
                )),
            }],
            failure: Vec::new(),
        }))
        .await?;

    Ok(res.into_inner().succeeded)
}
