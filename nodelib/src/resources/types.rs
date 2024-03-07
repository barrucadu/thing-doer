use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::etcd;
use crate::etcd::pb::etcdserverpb::PutRequest;
use crate::etcd::prefix;
use crate::types::ResourceError;
use crate::util::{is_valid_resource_name, is_valid_resource_type};

/// A resource where the spec is a JSON object.
pub type Resource = GenericResource<HashMap<String, Value>>;

/// A resource, generic over the spec type.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GenericResource<T> {
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
    pub spec: T,
}

impl<T: Serialize> GenericResource<T> {
    /// Construct a resource out of just a name and a type.
    pub fn new(name: String, rtype: String, spec: T) -> Self {
        Self {
            name,
            rtype,
            state: None,
            metadata: HashMap::new(),
            spec,
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
