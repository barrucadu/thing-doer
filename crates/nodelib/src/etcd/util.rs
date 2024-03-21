use tonic::Request;

use crate::error::Error;
use crate::etcd;
use crate::etcd::pb::etcdserverpb::compare;
use crate::etcd::pb::etcdserverpb::request_op;
use crate::etcd::pb::etcdserverpb::{Compare, DeleteRangeRequest, RequestOp, TxnRequest};

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
