pub mod pod;
pub mod types;

use tonic::Request;

use crate::etcd;
use crate::types::Error;

// convenience re-exports
pub use crate::resources::pod::PodResource;
pub use crate::resources::types::{GenericResource, Resource};

/// Persist a new resource to etcd.
///
/// The resource json must have `type`, `name`, and `spec` fields, and the
/// `name` must be a valid DNS label:
///
/// - 1 to 63 characters
/// - Contains only ASCII letters, digits, and hyphens
/// - Starts with a letter
/// - Does not end with a hyphen
pub async fn put(etcd_config: &etcd::Config, resource: Resource) -> Result<(), Error> {
    resource.validate()?;

    let mut kv_client = etcd_config.kv_client().await?;
    let res = kv_client
        .put(Request::new(resource.clone().to_put_request(etcd_config)))
        .await;

    match res {
        Ok(_) => Ok(()),
        Err(error) => {
            tracing::warn!(?error, ?resource, "could not put resource");
            Err(error.into())
        }
    }
}
