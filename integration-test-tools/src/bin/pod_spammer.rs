use clap::Parser;
use std::time::Duration;
use tonic::Request;

use nodelib::etcd;
use nodelib::etcd::pb::etcdserverpb::PutRequest;
use nodelib::etcd::prefix;
use nodelib::resources;
use nodelib::resources::Resource;
use nodelib::types::PodState;
use nodelib::util;

/// thing-doer integration test tools: pod spammer.
#[derive(Clone, Debug, Parser)]
struct Args {
    /// Prefix to use in pod names and the "createdBy" to add to the pod
    /// metadata.  The full name is this plus a random suffix.
    #[clap(long, default_value = "spam")]
    pub name: String,

    /// How long to wait between creating new pods.
    #[clap(
        long,
        value_parser = |secs: &str| secs.parse().map(Duration::from_secs),
        default_value = "5",
    )]
    pub delay: Duration,

    #[command(flatten)]
    pub etcd: etcd::Config,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().json().init();

    let config = Args::parse();
    let name = format!(
        "{name}-{suffix}",
        name = config.name,
        suffix = util::random_string(8)
    );

    let cmds = &[
        "/run/current-system/sw/bin/no-such-command",
        "/run/current-system/sw/bin/ls",
        "/run/current-system/sw/bin/true",
        "/run/current-system/sw/bin/false",
        "/run/current-system/sw/bin/env",
    ];
    let cpus = &[0.1, 0.3];
    let mems = &[16, 64, 128];
    let mut idx = 0;
    loop {
        let pod_name = format!("{name}-pod-{idx}");
        let cmd = cmds[idx % cmds.len()];
        let cpu = cpus[idx % cpus.len()];
        let mem = mems[idx % mems.len()];
        tracing::info!(pod_name, cmd, ?cpu, ?mem, "create");

        let pod = Resource::new(pod_name, "pod".to_owned())
            .with_state(PodState::Created.to_resource_state())
            .with_metadata("createdBy", name.clone())
            .with_spec("cmd", serde_json::json!(cmd))
            .with_spec("env", serde_json::json!({ "FOO": "1", "BAR": "2" }))
            .with_spec(
                "resources",
                serde_json::json!({ "requests": {
                    "cpu": cpu,
                    "memory": mem,
                }}),
            );

        let mut kv_client = config.etcd.kv_client().await?;
        resources::put(&config.etcd, pod.clone()).await?;
        kv_client
            .put(Request::new(PutRequest {
                key: format!(
                    "{prefix}{pod_name}",
                    prefix = prefix::unscheduled_pods(&config.etcd),
                    pod_name = pod.name
                )
                .into(),
                value: pod.to_json_string().into(),
                ..Default::default()
            }))
            .await?;

        idx += 1;
        tokio::time::sleep(config.delay).await;
    }
}
