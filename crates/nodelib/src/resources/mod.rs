pub mod node;
pub mod pod;
pub mod types;

use serde::Serialize;
use std::fmt::Debug;
use tonic::Request;

use crate::error::Error;
use crate::etcd;

// convenience re-exports
pub use crate::resources::node::NodeResource;
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
pub async fn put<
    TypeT: Clone + Debug + Serialize,
    StateT: Clone + Debug + Serialize,
    SpecT: Clone + Debug + Serialize,
>(
    etcd_config: &etcd::Config,
    resource: GenericResource<TypeT, StateT, SpecT>,
) -> Result<(), Error> {
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
