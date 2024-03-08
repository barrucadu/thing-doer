use clap::Parser;
use std::collections::HashMap;
use tonic::Request;

use nodelib::etcd;
use nodelib::etcd::pb::etcdserverpb::PutRequest;
use nodelib::etcd::prefix;
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
    let pod_name = util::random_name();

    let pod = PodResource::new(
        pod_name.clone(),
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
    .with_state(PodState::Created);

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

    Ok(())
}
