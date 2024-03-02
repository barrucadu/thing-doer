use crate::etcd::config::Config;

/// Prefix under which pods to schedule are written.  Keys are pod names, values
/// are pod resources.
pub fn unscheduled_pods(config: &Config) -> String {
    format!("{prefix}/pod/unscheduled/", prefix = config.prefix)
}

/// Prefix under which claimed pods are written.  Keys are pod names, values are
/// worker names.
pub fn claimed_pods(config: &Config) -> String {
    format!("{prefix}/pod/claimed/", prefix = config.prefix)
}

/// Prefix under which pods assigned to a worker are written.  Keys are pod
/// names, values are pod resources.
pub fn worker_inbox(config: &Config, worker: &str) -> String {
    format!(
        "{prefix}/pod/worker-inbox/{worker}/",
        prefix = config.prefix
    )
}

/// Prefix under which node "healthy" checks are written.  Keys are node names,
/// values are arbitrary.
pub fn node_heartbeat_healthy(config: &Config) -> String {
    format!("{prefix}/node/heartbeat/healthy/", prefix = config.prefix)
}

/// Prefix under which node "alive" checks are written.  Keys are node names,
/// values are arbitrary.
pub fn node_heartbeat_alive(config: &Config) -> String {
    format!("{prefix}/node/heartbeat/alive/", prefix = config.prefix)
}

/// Prefix under which resources are written.  Keys are resource names, values
/// are resource values.
pub fn resource(config: &Config, res_type: &str) -> String {
    format!("{prefix}/resource/{res_type}/", prefix = config.prefix)
}
