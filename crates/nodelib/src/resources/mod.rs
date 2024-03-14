pub mod node;
pub mod pod;
pub mod types;

use serde::Serialize;
use std::fmt::Debug;
use tonic::Request;

use crate::error::{Error, ResourceError};
use crate::etcd;
use crate::etcd::pb::etcdserverpb::compare;
use crate::etcd::pb::etcdserverpb::range_request::{SortOrder, SortTarget};
use crate::etcd::pb::etcdserverpb::request_op;
use crate::etcd::pb::etcdserverpb::{
    Compare, DeleteRangeRequest, PutRequest, RangeRequest, RequestOp, TxnRequest,
};
use crate::etcd::prefix;

// convenience re-exports
pub use crate::resources::node::NodeResource;
pub use crate::resources::pod::{PodResource, PodType};
pub use crate::resources::types::{GenericResource, Resource};

/// Get a resource by name, if it exists.
pub async fn get(
    etcd_config: &etcd::Config,
    rtype: &str,
    rname: &str,
) -> Result<Option<Resource>, Error> {
    let mut kv_client = etcd_config.kv_client().await?;
    let res = kv_client
        .range(Request::new(RangeRequest {
            key: format!(
                "{prefix}{rname}",
                prefix = prefix::resource(etcd_config, rtype),
            )
            .into(),
            ..Default::default()
        }))
        .await?
        .into_inner();

    if res.kvs.is_empty() {
        Ok(None)
    } else {
        let kv = res.kvs[0].clone();
        let resource = Resource::try_from(kv.value)?;
        Ok(Some(resource))
    }
}

/// List all resources of the given type.
pub async fn list(etcd_config: &etcd::Config, rtype: &str) -> Result<Vec<Resource>, Error> {
    let mut out = Vec::new();

    let mut kv_client = etcd_config.kv_client().await?;
    let mut revision = 0;

    let key_prefix = prefix::resource(etcd_config, rtype);
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
            let resource = Resource::try_from(kv.clone().value)?;
            out.push(resource);
        }

        if response.more {
            let idx = response.kvs.len() - 1;
            key = response.kvs[idx].key.clone();
        } else {
            break;
        }
    }

    Ok(out)
}

///////////////////////////////////////////////////////////////////////////////

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

/// Create a pod resource and also immediately schedule it.
///
/// Returns `false` if the pod already exists.
///
/// Returns `ResourceError::BadState` if the pod's state is not
/// `PodState::Created`.
///
/// TODO: can schedulerd be made to do the right thing from just the resource
/// being deleted?
pub async fn create_and_schedule_pod(
    etcd_config: &etcd::Config,
    resource: PodResource,
) -> Result<bool, Error> {
    if resource.state != Some(pod::PodState::Created) {
        return Err(Error::Resource(ResourceError::BadState));
    }

    let mut kv_client = etcd_config.kv_client().await?;
    let res = kv_client
        .txn(Request::new(TxnRequest {
            // create revision (ie `Create`) = 0 => the key does not exist
            compare: vec![Compare {
                result: compare::CompareResult::Equal.into(),
                target: compare::CompareTarget::Create.into(),
                key: resource.key(etcd_config).into(),
                ..Default::default()
            }],
            success: vec![
                RequestOp {
                    request: Some(request_op::Request::RequestPut(
                        resource.clone().to_put_request(etcd_config),
                    )),
                },
                RequestOp {
                    request: Some(request_op::Request::RequestPut(PutRequest {
                        key: format!(
                            "{prefix}{pod_name}",
                            prefix = prefix::unscheduled_pods(etcd_config),
                            pod_name = resource.name,
                        )
                        .into(),
                        value: resource.clone().to_json_string().into(),
                        ..Default::default()
                    })),
                },
            ],
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

///////////////////////////////////////////////////////////////////////////////

/// Delete a resource.  If this is a pod it does NOT kill the pod - use
/// `delete_and_kill_pod` for that.
///
/// Returns `false` if the resource does not exist.
pub async fn delete(etcd_config: &etcd::Config, rtype: &str, rname: &str) -> Result<bool, Error> {
    let resource_key = format!(
        "{prefix}{rname}",
        prefix = prefix::resource(etcd_config, rtype),
    );

    let mut kv_client = etcd_config.kv_client().await?;
    let res = kv_client
        .txn(Request::new(TxnRequest {
            // create revision (ie `Create`) != 0 => the key exists
            compare: vec![Compare {
                result: compare::CompareResult::NotEqual.into(),
                target: compare::CompareTarget::Create.into(),
                key: resource_key.clone().into(),
                ..Default::default()
            }],
            success: vec![RequestOp {
                request: Some(request_op::Request::RequestDeleteRange(
                    DeleteRangeRequest {
                        key: resource_key.into(),
                        ..Default::default()
                    },
                )),
            }],
            failure: Vec::new(),
        }))
        .await;

    match res {
        Ok(r) => Ok(r.into_inner().succeeded),
        Err(error) => {
            tracing::warn!(?error, rtype, rname, "could not delete resource");
            Err(error.into())
        }
    }
}

/// Delete a pod resource and signal that it should be unscheduled / killed.
///
/// Returns `false` if the resource does not exist.
///
/// TODO: can schedulerd be made to do the right thing from just the resource
/// being deleted?
pub async fn delete_and_kill_pod(
    etcd_config: &etcd::Config,
    pod_name: &str,
) -> Result<bool, Error> {
    let resource_key = format!(
        "{prefix}{pod_name}",
        prefix = prefix::resource(etcd_config, &PodType::Pod.to_string()),
    );
    let unscheduled_pod_key = format!(
        "{prefix}{pod_name}",
        prefix = prefix::unscheduled_pods(etcd_config),
    );

    let mut kv_client = etcd_config.kv_client().await?;
    let res = kv_client
        .txn(Request::new(TxnRequest {
            // create revision (ie `Create`) != 0 => the key exists
            compare: vec![Compare {
                result: compare::CompareResult::NotEqual.into(),
                target: compare::CompareTarget::Create.into(),
                key: resource_key.clone().into(),
                ..Default::default()
            }],
            success: vec![
                RequestOp {
                    request: Some(request_op::Request::RequestDeleteRange(
                        DeleteRangeRequest {
                            key: resource_key.into(),
                            ..Default::default()
                        },
                    )),
                },
                RequestOp {
                    request: Some(request_op::Request::RequestDeleteRange(
                        DeleteRangeRequest {
                            key: unscheduled_pod_key.into(),
                            ..Default::default()
                        },
                    )),
                },
            ],
            failure: Vec::new(),
        }))
        .await;

    match res {
        Ok(r) => Ok(r.into_inner().succeeded),
        Err(error) => {
            tracing::warn!(?error, pod_name, "could not kill pod");
            Err(error.into())
        }
    }
}
