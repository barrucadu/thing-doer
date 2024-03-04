use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tonic::Request;

use crate::etcd;
use crate::etcd::pb::etcdserverpb::PutRequest;
use crate::etcd::prefix;
use crate::types::{Error, PodLimits, ResourceError};
use crate::util::{is_valid_resource_name, is_valid_resource_type};

/// A generic resource.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Resource {
    /// Name - must be a valid DNS label and globally unique for the type.
    pub name: String,
    /// Type - must consist of just alphanums and dots.
    #[serde(rename = "type")]
    pub rtype: String,
    /// State - optional, type-specific.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// Metadata - optional key / value data.
    #[serde(default)]
    pub metadata: HashMap<String, String>,
    /// Spec - type-specific resource definition.
    pub spec: HashMap<String, Value>,
}

impl Resource {
    /// Construct a resource out of just a name and a type.
    pub fn new(name: String, rtype: String) -> Self {
        Self {
            name,
            rtype,
            state: None,
            metadata: HashMap::new(),
            spec: HashMap::new(),
        }
    }

    /// Set the state, overwriting any existing value.
    pub fn with_state(mut self, state: String) -> Self {
        self.state = Some(state);
        self
    }

    /// Set a metadata key, overwriting any existing value.
    pub fn with_metadata(mut self, key: &str, value: String) -> Self {
        self.metadata.insert(key.to_owned(), value);
        self
    }

    /// Set a spec key, overwriting any existing value.
    pub fn with_spec(mut self, key: &str, value: Value) -> Self {
        self.spec.insert(key.to_owned(), value);
        self
    }

    /// Check if a resource is valid.
    pub fn validate(&self) -> Result<(), ResourceError> {
        if !is_valid_resource_name(&self.name) {
            return Err(ResourceError::BadName);
        }
        if !is_valid_resource_type(&self.rtype) {
            return Err(ResourceError::BadType);
        }

        Ok(())
    }

    /// Get the etcd key for this resource.
    pub fn key(&self, etcd_config: &etcd::Config) -> String {
        format!(
            "{prefix}{res_name}",
            prefix = prefix::resource(etcd_config, &self.rtype),
            res_name = self.name,
        )
    }

    /// Turn a resource into a JSON string.
    pub fn to_json_string(self) -> String {
        serde_json::to_string(&self).unwrap().to_string()
    }

    /// Turn a resource into an etcd PutRequest.
    pub fn to_put_request(self, etcd_config: &etcd::Config) -> PutRequest {
        PutRequest {
            key: self.key(etcd_config).into(),
            value: self.to_json_string().into(),
            ..Default::default()
        }
    }

    /// If a resource is a pod, get its resource spec.
    pub fn pod_limits(&self) -> Option<PodLimits> {
        if self.rtype == *"pod" {
            if let Some(resources) = self.spec.get("resources") {
                let cpu = |v: &Value| v["cpu"].as_f64().and_then(|n| Decimal::try_from(n).ok());
                let mem = |v: &Value| v["memory"].as_u64();

                Some(PodLimits {
                    requested_cpu: cpu(&resources["requests"]),
                    maximum_cpu: cpu(&resources["limits"]),
                    requested_memory: mem(&resources["requests"]),
                    maximum_memory: mem(&resources["limits"]),
                })
            } else {
                Some(PodLimits {
                    requested_cpu: None,
                    maximum_cpu: None,
                    requested_memory: None,
                    maximum_memory: None,
                })
            }
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub enum TryFromError {
    Invalid(ResourceError),
    Serde(serde_json::Error),
}

impl From<ResourceError> for TryFromError {
    fn from(err: ResourceError) -> Self {
        TryFromError::Invalid(err)
    }
}

impl From<serde_json::Error> for TryFromError {
    fn from(err: serde_json::Error) -> Self {
        TryFromError::Serde(err)
    }
}

impl TryFrom<Value> for Resource {
    type Error = TryFromError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let resource = serde_json::from_value::<Self>(value)?;
        resource.validate()?;

        Ok(resource)
    }
}

impl TryFrom<Vec<u8>> for Resource {
    type Error = TryFromError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let resource = serde_json::from_slice::<Self>(&bytes)?;
        resource.validate()?;

        Ok(resource)
    }
}

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
