use clap::Parser;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::time::Duration;
use tonic::Request;

use nodelib::etcd;
use nodelib::etcd::pb::etcdserverpb::PutRequest;
use nodelib::etcd::prefix;
use nodelib::resources;
use nodelib::resources::pod::*;
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
    let cpus = &[Decimal::new(1, 1), Decimal::new(3, 1)];
    let mems = &[16, 64, 128];
    let mut idx = 0;
    loop {
        let pod_name = format!("{name}-pod-{idx}");
        let cmd = cmds[idx % cmds.len()];
        let cpu = cpus[idx % cpus.len()];
        let mem = mems[idx % mems.len()];
        tracing::info!(pod_name, cmd, ?cpu, ?mem, "create");

        let spec = PodSpec {
            containers: vec![PodContainerSpec {
                image: "no-such-image".to_owned(),
                entrypoint: Some(cmd.to_owned()),
                cmd: Vec::new(),
                env: HashMap::from([
                    ("FOO".to_owned(), "1".to_owned()),
                    ("BAR".to_owned(), "2".to_owned()),
                ]),
                ports: Vec::new(),
                resources: Some(ContainerResourceSpec {
                    requests: Some(ContainerResourceSpecInner {
                        cpu: Some(cpu),
                        memory: Some(mem),
                    }),
                    limits: None,
                }),
            }],
        };
        let specv = serde_json::to_value(spec).unwrap();
        let specm = serde_json::from_value::<HashMap<_, _>>(specv).unwrap();
        let pod = Resource::new(pod_name.clone(), "pod".to_owned(), specm)
            .with_state(PodState::Created.to_resource_state())
            .with_metadata("createdBy", name.clone());

        let mut kv_client = config.etcd.kv_client().await?;
        resources::put(&config.etcd, pod.clone()).await?;
        kv_client
            .put(Request::new(PutRequest {
                key: format!(
                    "{prefix}{pod_name}",
                    prefix = prefix::unscheduled_pods(&config.etcd),
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
