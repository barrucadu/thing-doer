use serde_json::Value;
use std::net::Ipv4Addr;
use std::process::Stdio;
use std::str::FromStr;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::oneshot;

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

    /// Name of the iptables binary.  Defaults to "iptables".
    #[clap(
        long = "iptables-command",
        value_parser,
        default_value = "iptables",
        env = "IPTABLES_CMD"
    )]
    pub iptables: String,

    /// Name of the iptables chain to create.  If not given defaults to the node name.
    #[clap(long = "iptables-chain", value_parser, env = "IPTABLES_CHAIN")]
    pub iptables_chain: Option<String>,
}

impl Config {
    fn podman(&self) -> Command {
        let mut cmd = Command::new(self.podman.clone());
        cmd.stdin(Stdio::null()).kill_on_drop(true);
        cmd
    }

    fn iptables(&self) -> Command {
        let mut cmd = Command::new(self.iptables.clone());
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
    pub ports: Vec<PodPortSpec>,
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

    let mut cmd = config.podman();
    cmd.args([
        "pod",
        "create",
        &format!("--dns={dns_ip}"),
        &format!("--network={network}", network = config.bridge_network),
    ]);
    cmd.arg(&podman_pod_name);

    let exit_status = cmd.spawn()?.wait().await?;
    if exit_status.success() {
        let infra_container_id = get_pod_infra_container_id(config, &podman_pod_name).await?;
        start_container_by_id(config, &infra_container_id).await?;

        let address = get_container_ip_by_id(config, &infra_container_id).await?;
        create_pod_iptables_rules(config, my_name, address, &pod.spec.ports).await?;
        Ok(Some(PodState {
            name: podman_pod_name,
            hostname: pod.name.clone(),
            infra_container_id,
            address,
            ports: pod.spec.ports.clone(),
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

    let mut cmd = config.podman();
    cmd.args([
        "container",
        "run",
        "--detach",
        "--tty",
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

/// Result of container execution.
pub enum WFC {
    Terminated(bool),
    Signal(oneshot::Sender<()>),
}

/// Wait for all the containers in the pod to terminate.  Returns `true` if all
/// containers exited successfully.
pub async fn wait_for_containers(
    config: &Config,
    pod_state: &PodState,
    mut kill_rx: &mut oneshot::Receiver<oneshot::Sender<()>>,
) -> std::io::Result<WFC> {
    loop {
        if let Some(all_ok) = container_status(config, &pod_state.name).await? {
            return Ok(WFC::Terminated(all_ok));
        }

        tokio::select! {
            () = tokio::time::sleep(Duration::from_secs(POLL_INTERVAL)) => (),
            ch = &mut kill_rx => {
                return Ok(WFC::Signal(ch.unwrap()));
            }
        }
    }
}

/// Terminate any running containers in a pod, and delete it.
pub async fn terminate_pod(
    config: &Config,
    my_name: &str,
    pod_state: &PodState,
) -> std::io::Result<()> {
    delete_pod_iptables_rules(config, my_name, pod_state).await?;

    let mut cmd = config.podman();
    cmd.args(["pod", "rm", "-f", &pod_state.name]);

    // TODO: handle exit failure
    let _ = cmd.spawn()?.wait().await?;
    Ok(())
}

///////////////////////////////////////////////////////////////////////////////

/// Create the iptables `nat` chain and link it to the default `PREROUTING` and
/// `OUTPUT` chains.
pub async fn initialise_iptables(config: &Config, my_name: &str) -> std::io::Result<()> {
    let chain_name = config.iptables_chain.clone().unwrap_or(my_name.to_string());

    let mut create_nat_chain_cmd = config.iptables();
    create_nat_chain_cmd.args(["-t", "nat", "-N", &chain_name]);

    let mut prerouting_jump_cmd = config.iptables();
    prerouting_jump_cmd.args(["-t", "nat", "-A", "PREROUTING", "-j", &chain_name]);

    let mut output_jump_cmd = config.iptables();
    output_jump_cmd.args(["-t", "nat", "-A", "OUTPUT", "-j", &chain_name]);

    // TODO: handle exit failure
    let _ = create_nat_chain_cmd.spawn()?.wait().await?;
    let _ = prerouting_jump_cmd.spawn()?.wait().await?;
    let _ = output_jump_cmd.spawn()?.wait().await?;

    Ok(())
}

/// Delete all the iptables rules and chains.
pub async fn teardown_iptables(config: &Config, my_name: &str) -> std::io::Result<()> {
    let chain_name = config.iptables_chain.clone().unwrap_or(my_name.to_string());

    let mut detach_prerouting_cmd = config.iptables();
    detach_prerouting_cmd.args(["-t", "nat", "-D", "PREROUTING", "-j", &chain_name]);

    let mut detach_output_cmd = config.iptables();
    detach_output_cmd.args(["-t", "nat", "-D", "OUTPUT", "-j", &chain_name]);

    let mut purge_cmd = config.iptables();
    purge_cmd.args(["-t", "nat", "-F", &chain_name]);

    let mut delete_cmd = config.iptables();
    delete_cmd.args(["-t", "nat", "-X", &chain_name]);

    // TODO: handle exit failure
    let _ = detach_prerouting_cmd.spawn()?.wait().await?;
    let _ = detach_output_cmd.spawn()?.wait().await?;
    let _ = purge_cmd.spawn()?.wait().await?;
    let _ = delete_cmd.spawn()?.wait().await?;

    Ok(())
}

/// Create the iptables rules for a pod.
async fn create_pod_iptables_rules(
    config: &Config,
    my_name: &str,
    address: Ipv4Addr,
    ports: &[PodPortSpec],
) -> std::io::Result<()> {
    let chain_name = config.iptables_chain.clone().unwrap_or(my_name.to_string());

    // TODO: add a default filter rule forbidding all ports and specific filter
    // rules allowing the configured cluster ports.
    for rule in generate_nat_rules(address, ports) {
        let mut cmd = config.iptables();
        cmd.args(["-t", "nat", "-A", &chain_name]);
        cmd.args(rule);

        // TODO: handle exit failure
        let _ = cmd.spawn()?.wait().await?;
    }

    Ok(())
}

/// Clean up the iptables rules for a pod.
async fn delete_pod_iptables_rules(
    config: &Config,
    my_name: &str,
    pod_state: &PodState,
) -> std::io::Result<()> {
    let chain_name = config.iptables_chain.clone().unwrap_or(my_name.to_string());

    for rule in generate_nat_rules(pod_state.address, &pod_state.ports) {
        let mut cmd = config.iptables();
        cmd.args(["-t", "nat", "-D", &chain_name]);
        cmd.args(rule);

        // TODO: handle exit failure
        let _ = cmd.spawn()?.wait().await?;
    }

    Ok(())
}

/// Generate the NAT rules for a set of port mappings.
fn generate_nat_rules(address: Ipv4Addr, ports: &[PodPortSpec]) -> Vec<Vec<String>> {
    let mut out = Vec::with_capacity(ports.len());

    for port in ports {
        if let PodPortSpec::Map {
            container,
            cluster: Some(cluster),
        } = port
        {
            out.push(vec![
                "-d".to_string(),
                format!("{address}/32"),
                "-p".to_string(),
                "tcp".to_string(),
                "--dport".to_string(),
                format!("{cluster}"),
                "-j".to_string(),
                "DNAT".to_string(),
                "--to-destination".to_string(),
                format!("{address}:{container}"),
            ]);
        }
    }

    out
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
    let mut cmd = config.podman();
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
    let mut cmd = config.podman();
    let output = cmd.args([etype, "inspect", ename]).output().await?;
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();

    Ok(value)
}

/// Return the `podman ps` json for a podman pod.
async fn podman_ps(config: &Config, pname: &str) -> std::io::Result<Vec<Value>> {
    let mut cmd = config.podman();
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
