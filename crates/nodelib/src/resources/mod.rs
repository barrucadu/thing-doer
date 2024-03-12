pub mod node;
pub mod pod;
pub mod types;

use serde::Serialize;
use std::fmt::Debug;
use tonic::Request;

use crate::error::Error;
use crate::etcd;
use crate::etcd::pb::etcdserverpb::compare;
use crate::etcd::pb::etcdserverpb::request_op;
use crate::etcd::pb::etcdserverpb::{Compare, RequestOp, TxnRequest};

// convenience re-exports
pub use crate::resources::node::NodeResource;
pub use crate::resources::pod::PodResource;
pub use crate::resources::types::{GenericResource, Resource};

/// Persist a resource to etcd.
///
/// Returns `false` if the resource already exists - unless `allow_overwrite` is
/// set to `true`.
///
/// The resource json must have `type`, `name`, and `spec` fields, and the
/// `name` must be a valid DNS label:
///
/// - 1 to 63 characters
/// - Contains only ASCII letters, digits, and hyphens
/// - Starts with a letter
/// - Does not end with a hyphen
///
/// TODO: validate the resource against a type-specific schema.
pub async fn create_or_replace<
    TypeT: Clone + Debug + Serialize,
    StateT: Clone + Debug + Serialize,
    SpecT: Clone + Debug + Serialize,
>(
    etcd_config: &etcd::Config,
    allow_overwrite: bool,
    resource: GenericResource<TypeT, StateT, SpecT>,
) -> Result<bool, Error> {
    resource.validate()?;

    let compare = if allow_overwrite {
        Vec::new()
    } else {
        // create revision (ie `Create`) = 0 => the key does not exist
        vec![Compare {
            result: compare::CompareResult::Equal.into(),
            target: compare::CompareTarget::Create.into(),
            key: resource.key(etcd_config).into(),
            ..Default::default()
        }]
    };

    let mut kv_client = etcd_config.kv_client().await?;
    let res = kv_client
        .txn(Request::new(TxnRequest {
            compare,
            success: vec![RequestOp {
                request: Some(request_op::Request::RequestPut(
                    resource.clone().to_put_request(etcd_config),
                )),
            }],
            failure: Vec::new(),
        }))
        .await;

    match res {
        Ok(r) => Ok(r.into_inner().succeeded),
        Err(error) => {
            tracing::warn!(?error, ?resource, "could not put resource");
            Err(error.into())
        }
    }
}
