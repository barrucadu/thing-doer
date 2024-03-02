use serde_json::Value;
use tonic::Request;

use crate::etcd;
use crate::etcd::pb::etcdserverpb::PutRequest;
use crate::etcd::prefix;
use crate::util::is_valid_resource_name;
use crate::{Error, ResourceError};

/// Persist a new resource to etcd.
///
/// The resource json must have `type`, `name`, and `spec` fields, and the
/// `name` must be a valid DNS label:
///
/// - 1 to 63 characters
/// - Contains only ASCII letters, digits, and hyphens
/// - Starts with a letter
/// - Does not end with a hyphen
pub async fn put(etcd_config: &etcd::Config, json: Value) -> Result<(), Error> {
    if !json["spec"].is_object() {
        return Err(ResourceError::BadStructure.into());
    }

    match (json["type"].as_str(), json["name"].as_str()) {
        (Some(res_type), Some(res_name)) => {
            if is_valid_resource_name(res_name) {
                let res = put_unchecked(etcd_config, res_type, res_name, &json).await;
                if let Err(ref error) = res {
                    tracing::warn!(?error, ?json, "could not put resource");
                }
                res
            } else {
                Err(ResourceError::BadName.into())
            }
        }
        _ => Err(ResourceError::BadStructure.into()),
    }
}

/// Like `put` but don't do any validation.
async fn put_unchecked(
    etcd_config: &etcd::Config,
    res_type: &str,
    res_name: &str,
    json: &Value,
) -> Result<(), Error> {
    let mut kv_client = etcd_config.kv_client().await?;

    kv_client
        .put(Request::new(PutRequest {
            key: format!(
                "{prefix}{res_name}",
                prefix = prefix::resource(etcd_config, res_type)
            )
            .into(),
            value: json.to_string().into(),
            ..Default::default()
        }))
        .await?;

    Ok(())
}
