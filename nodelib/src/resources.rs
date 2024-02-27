use serde_json::Value;
use tonic::Request;

use crate::etcd;
use crate::etcd::pb::etcdserverpb::PutRequest;
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
            if validate_name(res_name) {
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
            key: resource_key(etcd_config, res_type, res_name).into(),
            value: json.to_string().into(),
            ..Default::default()
        }))
        .await?;

    Ok(())
}

/// Check that a name is a valid DNS label.
fn validate_name(name: &str) -> bool {
    let valid_character = |c: char| c.is_ascii_alphanumeric() || c == '-';

    !name.is_empty()
        && name.len() <= 63
        && name.chars().all(valid_character)
        && name.starts_with(|c: char| c.is_ascii_alphabetic())
        && !name.ends_with(|c: char| c == '-')
}

/// The key to use for a resource.
fn resource_key(etcd_config: &etcd::Config, res_type: &str, res_name: &str) -> String {
    format!(
        "{prefix}/resource/{res_type}/{res_name}",
        prefix = etcd_config.prefix
    )
}
