use reqwest::blocking::Client;
use reqwest::Url;
use rust_decimal::Decimal;
use std::process;

use nodelib::resources::pod::*;
use nodelib::util::random_name;

#[derive(Debug, clap::Args)]
pub struct RunArgs {
    /// Constrain the set of worker nodes that this pod can be scheduled on.
    #[arg(long)]
    cannot_be_scheduled_on: Vec<String>,

    /// Maximum amount of CPU to use.
    #[arg(long)]
    cpu_limit: Option<Decimal>,

    /// Amount of CPU to request.
    #[arg(long)]
    cpu_request: Option<Decimal>,

    /// Command to override the entrypoint with.
    #[arg(long)]
    entrypoint: Option<String>,

    /// Environment variables, in `key=value` form.
    #[arg(short='e', long, value_parser=parse_env)]
    env: Vec<(String, String)>,

    /// Constrain the set of worker nodes that this pod can be scheduled on.
    #[arg(long)]
    may_be_scheduled_on: Vec<String>,

    /// Maximum amount of memory to use, in MiB.
    #[arg(long)]
    memory_limit: Option<u64>,

    /// Amount of memory to request, in MiB.
    #[arg(long)]
    memory_request: Option<u64>,

    /// Name of the pod.  If not given a random name is assigned.
    #[arg(long)]
    name: Option<String>,

    /// Ports to expose, in `container` or `cluster:container` form.
    #[arg(short='p', long, value_parser=parse_port)]
    publish: Vec<PodPortSpec>,

    /// Image to run.  If no tag is given, `latest` is used.
    image: String,

    /// Command to run.
    command: Vec<String>,
}

/// Launch a single-container pod.  The container inside the pod is called
/// 'run'.
pub fn cmd_run(apid_url: &Url, args: RunArgs) {
    let pod = PodResource::new(
        args.name.unwrap_or(random_name()),
        PodType::Pod,
        PodSpec {
            containers: vec![PodContainerSpec {
                name: "run".to_string(),
                image: args.image,
                entrypoint: args.entrypoint,
                cmd: args.command,
                env: args.env.into_iter().collect(),
                resources: Some(ContainerResourceSpec {
                    requests: Some(ContainerResourceSpecInner {
                        cpu: args.cpu_request,
                        memory: args.memory_request,
                    }),
                    limits: Some(ContainerResourceSpecInner {
                        cpu: args.cpu_limit,
                        memory: args.memory_limit,
                    }),
                }),
            }],
            ports: args.publish,
            scheduling_constraints: Some(PodSchedulingConstraintsSpec {
                cannot_be_scheduled_on: if args.cannot_be_scheduled_on.is_empty() {
                    None
                } else {
                    Some(args.cannot_be_scheduled_on)
                },
                may_be_scheduled_on: if args.may_be_scheduled_on.is_empty() {
                    None
                } else {
                    Some(args.may_be_scheduled_on)
                },
            }),
        },
    )
    .with_state(PodState::Created);

    let post_resources_url = apid_url.join("resources").unwrap();
    match Client::new().post(post_resources_url).json(&pod).send() {
        Ok(response) => {
            if response.status().is_success() {
                println!("{pod_name}", pod_name = pod.name);
            } else {
                eprintln!("status: {status}", status = response.status());
                eprintln!("{body}", body = response.text().unwrap());
                process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("error: {error}");
            process::exit(1);
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

fn parse_env(s: &str) -> Result<(String, String), String> {
    if let Some((key, value)) = s.split_once('=') {
        Ok((key.to_string(), value.to_string()))
    } else {
        Err(format!("invalid key=value: no `=` found in `{s}`"))
    }
}

fn parse_port(s: &str) -> Result<PodPortSpec, String> {
    if let Some((cluster, container)) = s.split_once(':') {
        match (cluster.parse::<u16>(), container.parse::<u16>()) {
            (Ok(cluster), Ok(container)) if cluster > 0 && container > 0 => Ok(PodPortSpec::Map {
                container,
                cluster: Some(cluster),
            }),
            _ => Err(format!("invalid port specification: `{s}`")),
        }
    } else {
        match s.parse::<u16>() {
            Ok(port) if port > 0 => Ok(PodPortSpec::Expose(port)),
            _ => Err(format!("invalid port specification: `{s}`")),
        }
    }
}
