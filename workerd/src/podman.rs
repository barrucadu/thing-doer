use serde_json::Value;
use std::net::Ipv4Addr;
use std::process::Stdio;
use std::str::FromStr;
use std::time::Duration;
use tokio::process::Command;

use nodelib::resources::pod::*;

/// Interval to poll for the container status.
pub static POLL_INTERVAL: u64 = 1;

/// Options to configure the podman pods and containers.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Config {
    /// Name of the podman binary.  Defaults to "podman".
    #[clap(
        long = "podman-command",
        value_parser,
        default_value = "podman",
        env = "PODMAN_CMD"
    )]
    pub podman: String,

    /// Prefix to prepend to pod names so they can be easily identified in
    /// `podman` command output.  If not given defaults to the node name.
    #[clap(
        long = "podman-pod-name-prefix",
        value_parser,
        env = "PODMAN_POD_NAME_PREFIX"
    )]
    pub pod_name_prefix: Option<String>,

    /// Bridge network to use.  This must already exist, and be set up so that
    /// all the pods in the cluster can communicate.
    #[clap(
        long = "podman-bridge-network",
        value_parser,
        env = "PODMAN_BRIDGE_NETWORK"
    )]
    pub bridge_network: String,
}

impl Config {
    fn command(&self) -> Command {
        let mut cmd = Command::new(self.podman.clone());
        cmd.stdin(Stdio::null()).kill_on_drop(true);
        cmd
    }
}

/// State of a created pod.
#[derive(Debug)]
pub struct PodState {
    pub name: String,
    pub hostname: String,
    pub infra_container_id: String,
    pub address: Ipv4Addr,
}

/// Create the pod that will hold the containers.  All containers in the same
/// pod can communicate via localhost, and will have the same external IP.
pub async fn create_pod(
    config: &Config,
    my_name: &str,
    dns_ip: Ipv4Addr,
    pod: &PodResource,
) -> std::io::Result<Option<PodState>> {
    let podman_pod_name = format!(
        "{prefix}-{name}",
        prefix = config
            .pod_name_prefix
            .as_ref()
            .unwrap_or(&my_name.to_owned()),
        name = pod.name,
    );

    let mut cmd = config.command();
    cmd.args([
        "pod",
        "create",
        &format!("--dns={dns_ip}"),
        &format!("--network={network}", network = config.bridge_network),
    ]);
    for container in &pod.spec.containers {
        for port in &container.ports {
            match port {
                ContainerPortSpec::Expose(p) => cmd.arg(format!("--publish={p}")),
                ContainerPortSpec::Map {
                    container,
                    cluster: None,
                } => cmd.arg(format!("--publish={container}")),
                ContainerPortSpec::Map {
                    container,
                    cluster: Some(p),
                } => cmd.arg(format!("--publish={p}:{container}")),
            };
        }
    }
    cmd.arg(&podman_pod_name);

    let exit_status = cmd.spawn()?.wait().await?;
    if exit_status.success() {
        let infra_container_id = get_pod_infra_container_id(config, &podman_pod_name).await?;
        start_container_by_id(config, &infra_container_id).await?;

        let address = get_container_ip_by_id(config, &infra_container_id).await?;
        Ok(Some(PodState {
            name: podman_pod_name,
            hostname: pod.name.clone(),
            infra_container_id,
            address,
        }))
    } else {
        Ok(None)
    }
}

/// Start a container in a pod.  Do not wait for it to terminate, just let it
/// go.
pub async fn start_container(
    config: &Config,
    pod_state: &PodState,
    container: &PodContainerSpec,
) -> std::io::Result<bool> {
    let podman_container_name = format!(
        "{prefix}-{name}",
        prefix = pod_state.name,
        name = container.name,
    );

    let mut cmd = config.command();
    cmd.args([
        "container",
        "run",
        "--detach",
        "--pull=newer",
        &format!("--pod={pod_name}", pod_name = pod_state.name),
        &format!("--name={podman_container_name}"),
    ]);

    if let Some(entrypoint) = &container.entrypoint {
        cmd.arg(format!("--entrypoint={entrypoint}"));
    }
    for (ename, eval) in &container.env {
        cmd.args(["--env", &format!("{ename}={eval}")]);
    }
    if let Some(resources) = &container.resources {
        if let Some(requests) = &resources.requests {
            if let Some(memory) = requests.memory {
                cmd.arg(format!("--memory-reservation={memory}m"));
            }
        }
        if let Some(limits) = &resources.limits {
            if let Some(cpu) = limits.cpu {
                cmd.arg(format!("--cpus={cpu}"));
            }
            if let Some(memory) = limits.memory {
                cmd.arg(format!("--memory={memory}m"));
            }
        }
    }

    cmd.arg(&container.image);
    cmd.args(&container.cmd);

    let exit_status = cmd.spawn()?.wait().await?;
    Ok(exit_status.success())
}

/// Wait for all the containers in the pod to terminate.  Returns `true` if all
/// containers exited successfully.
pub async fn wait_for_containers(config: &Config, pod_state: &PodState) -> std::io::Result<bool> {
    loop {
        if let Some(all_ok) = container_status(config, &pod_state.name).await? {
            return Ok(all_ok);
        } else {
            tokio::time::sleep(Duration::from_secs(POLL_INTERVAL)).await;
        }
    }
}

/// Terminate any running containers in a pod, and delete it.
pub async fn terminate_pod(config: &Config, pod_state: &PodState) -> std::io::Result<()> {
    let mut cmd = config.command();
    cmd.args(["pod", "rm", "-f", &pod_state.name]);

    // TODO: handle exit failure
    let _ = cmd.spawn()?.wait().await?;
    Ok(())
}

///////////////////////////////////////////////////////////////////////////////

/// Check if all the containers in the pod have terminated successfully or not.
///
/// Returns `None` if any of the containers in the pod are still running.
/// Otherwise returns `Some(bool)` with `true` if all the containers exited with
/// status code "0", otherwise `false`.
async fn container_status(config: &Config, podman_pod_name: &str) -> std::io::Result<Option<bool>> {
    let mut running = false;
    let mut all_ok = true;
    for container in podman_ps(config, podman_pod_name).await? {
        if container["IsInfra"].as_bool() == Some(true) {
            continue;
        }

        if container["Exited"].as_bool() == Some(false) {
            running = true;
        } else {
            all_ok = all_ok && (container["ExitCode"].as_u64() == Some(0));
        }
    }

    if running {
        Ok(None)
    } else {
        Ok(Some(all_ok))
    }
}

/// Get the ID of the infrastructure container of the given pod
async fn get_pod_infra_container_id(
    config: &Config,
    podman_pod_name: &str,
) -> std::io::Result<String> {
    let value = podman_inspect(config, "pod", podman_pod_name).await?;
    let field = value["InfraContainerID"].as_str().unwrap();

    Ok(field.to_owned())
}

/// Start a container by ID.
async fn start_container_by_id(config: &Config, container_id: &str) -> std::io::Result<()> {
    let mut cmd = config.command();
    cmd.args(["container", "start", container_id]);

    // TODO: handle exit failure
    let _ = cmd.spawn()?.wait().await?;
    Ok(())
}

/// Get the IP address of a container, which must have been started.
async fn get_container_ip_by_id(config: &Config, container_id: &str) -> std::io::Result<Ipv4Addr> {
    let value = podman_inspect(config, "container", container_id).await?;
    let field = value[0]["NetworkSettings"]["Networks"][&config.bridge_network]["IPAddress"]
        .as_str()
        .unwrap();
    let address = Ipv4Addr::from_str(field).unwrap();

    Ok(address)
}

///////////////////////////////////////////////////////////////////////////////

/// Return the `podman inspect` json for a podman entity.
async fn podman_inspect(config: &Config, etype: &str, ename: &str) -> std::io::Result<Value> {
    let mut cmd = config.command();
    let output = cmd.args([etype, "inspect", ename]).output().await?;
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();

    Ok(value)
}

/// Return the `podman ps` json for a podman pod.
async fn podman_ps(config: &Config, pname: &str) -> std::io::Result<Vec<Value>> {
    let mut cmd = config.command();
    let output = cmd
        .args([
            "ps",
            "--all",
            "--format=json",
            &format!("--filter=pod={pname}"),
        ])
        .output()
        .await?;
    let value = serde_json::from_slice(&output.stdout).unwrap();

    Ok(value)
}
