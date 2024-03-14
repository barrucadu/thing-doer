/// Wrapper for node names, to disambiguate them from pod names.
#[derive(Clone)]
pub struct NodeName(pub String);

impl From<&str> for NodeName {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

/// Wrapper for pod names, to disambiguate them from node names.
#[derive(Clone)]
pub struct PodName(pub String);

impl From<&str> for PodName {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}
