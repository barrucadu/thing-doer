use clap::Parser;
use std::collections::HashMap;

use nodelib::etcd;
use nodelib::resources;
use nodelib::resources::pod::*;
use nodelib::util;

/// thing-doer integration test tools: spawn an nginx hello world container.
#[derive(Clone, Debug, Parser)]
struct Args {
    #[command(flatten)]
    pub etcd: etcd::Config,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().json().init();

    let config = Args::parse();
    resources::create_and_schedule_pod(
        &config.etcd,
        PodResource::new(
            util::random_name(),
            PodType::Pod,
            PodSpec {
                containers: vec![PodContainerSpec {
                    name: "web".to_owned(),
                    image: "docker.io/library/nginx".to_owned(),
                    entrypoint: None,
                    cmd: Vec::new(),
                    env: HashMap::new(),
                    ports: vec![ContainerPortSpec::Expose(80)],
                    resources: None,
                }],
            },
        )
        .with_state(PodState::Created),
    )
    .await?;

    Ok(())
}
