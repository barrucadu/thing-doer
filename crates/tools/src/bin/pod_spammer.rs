use clap::Parser;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::time::Duration;

use nodelib::etcd;
use nodelib::resources;
use nodelib::resources::pod::*;
use nodelib::util;

/// thing-doer integration test tools: pod spammer.
#[derive(Clone, Debug, Parser)]
struct Args {
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

    let cmds = &["no-such-command", "ls", "true", "false", "env"];
    let cpus = &[Decimal::new(1, 1), Decimal::new(3, 1)];
    let mems = &[16, 64, 128];
    let mut idx = 0;
    loop {
        let pod_name = util::random_name();
        let cmd = cmds[idx % cmds.len()];
        let cpu = cpus[idx % cpus.len()];
        let mem = mems[idx % mems.len()];
        tracing::info!(pod_name, cmd, ?cpu, ?mem, "create");

        resources::create_and_schedule_pod(
            &config.etcd,
            PodResource::new(
                pod_name,
                PodType::Pod,
                PodSpec {
                    containers: vec![PodContainerSpec {
                        name: "cmd".to_owned(),
                        image: "docker.io/library/busybox".to_owned(),
                        entrypoint: None,
                        cmd: vec![cmd.to_owned()],
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
                    scheduling_constraints: None,
                },
            )
            .with_state(PodState::Created),
        )
        .await?;

        idx += 1;
        tokio::time::sleep(config.delay).await;
    }
}
