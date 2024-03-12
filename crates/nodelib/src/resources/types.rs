use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::error::ResourceError;
use crate::etcd;
use crate::etcd::pb::etcdserverpb::PutRequest;
use crate::etcd::prefix;

/// A resource where the type and state are strings and the spec is a JSON
/// object.
pub type Resource = GenericResource<String, String, HashMap<String, Value>>;

/// A resource, generic over the spec type.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GenericResource<TypeT, StateT, SpecT> {
    /// Name - must be a valid DNS label and globally unique for the type.
    pub name: String,
    /// Type - must consist of just alphanums and dots.
    #[serde(rename = "type")]
    pub rtype: TypeT,
    /// State - optional, type-specific.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<StateT>,
    /// Metadata - optional key / value data.
    #[serde(default)]
    pub metadata: HashMap<String, String>,
    /// Spec - type-specific resource definition.
    pub spec: SpecT,
}

impl<TypeT: Serialize, SpecT: Serialize, StateT: Serialize> GenericResource<TypeT, StateT, SpecT> {
    /// Construct a resource out of just a name and a type.
    pub fn new(name: String, rtype: TypeT, spec: SpecT) -> Self {
        Self {
            name,
            rtype,
            state: None,
            metadata: HashMap::new(),
            spec,
        }
    }

    /// Set the state, overwriting any existing value.
    pub fn with_state(mut self, state: StateT) -> Self {
        self.state = Some(state);
        self
    }

    /// Set a metadata key, overwriting any existing value.
    pub fn with_metadata(mut self, key: &str, value: String) -> Self {
        self.metadata.insert(key.to_owned(), value);
        self
    }

    /// Get the serialised type name.
    pub fn type_name(&self) -> String {
        serde_json::from_value::<String>(serde_json::json!(self.rtype)).unwrap()
    }

    /// Check if a resource is valid.
    pub fn validate(&self) -> Result<(), ResourceError> {
        if !is_valid_resource_name(&self.name) {
            return Err(ResourceError::BadName);
        }
        if !is_valid_resource_type(&self.type_name()) {
            return Err(ResourceError::BadType);
        }

        Ok(())
    }

    /// Get the etcd key for this resource.
    pub fn key(&self, etcd_config: &etcd::Config) -> String {
        format!(
            "{prefix}{res_name}",
            prefix = prefix::resource(etcd_config, &self.type_name()),
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

impl TryFrom<Value> for Resource {
    type Error = ResourceError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let resource = serde_json::from_value::<Self>(value)?;
        resource.validate()?;

        Ok(resource)
    }
}

impl TryFrom<Vec<u8>> for Resource {
    type Error = ResourceError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let resource = serde_json::from_slice::<Self>(&bytes)?;
        resource.validate()?;

        Ok(resource)
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Check that a name is all lowercase and is a valid DNS label.
fn is_valid_resource_name(name: &str) -> bool {
    let valid_character = |c: char| c.is_ascii_alphanumeric() || c == '-';

    !name.is_empty()
        && name.len() <= 63
        && name.chars().all(valid_character)
        && name.starts_with(|c: char| c.is_ascii_alphabetic())
        && !name.ends_with(|c: char| c == '-')
}

/// Check that a type name is nonempty and of the form
/// `/[a-z0-9][a-z0-9\.]*[a-z0-9]`.
fn is_valid_resource_type(rtype: &str) -> bool {
    let valid_character = |c: char| c.is_ascii_alphanumeric() || c == '.';

    !rtype.is_empty()
        && rtype.chars().all(valid_character)
        && !rtype.starts_with(|c: char| c == '.')
        && !rtype.ends_with(|c: char| c == '.')
}
