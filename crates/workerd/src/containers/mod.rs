pub mod firewall;
pub mod runtime;

use std::net::Ipv4Addr;
use tokio::sync::oneshot;

use nodelib::resources::pod::*;

// convenience re-exports
pub use runtime::{RunningPod, WFC};

/// Options to configure the podman pods and containers.
#[derive(Clone, Debug)]
pub struct Config {
    pub firewall: firewall::Config,
    pub runtime: runtime::Config,
}

///////////////////////////////////////////////////////////////////////////////

/// Set up the firewall.
pub async fn initialise(config: &Config) -> std::io::Result<()> {
    firewall::initialise(&config.firewall).await
}

/// Tear down the firewall.
pub async fn teardown(config: &Config) -> std::io::Result<()> {
    firewall::teardown(&config.firewall).await
}

///////////////////////////////////////////////////////////////////////////////

/// Create the pod that will hold the containers and set up firewall rules.
///
/// All containers in the same pod can communicate via localhost, and will have
/// the same external IP.
///
/// Only the explicitly exposed ports are accessible.
pub async fn create_pod(
    config: &Config,
    dns_ip: Ipv4Addr,
    pod: &PodResource,
) -> std::io::Result<Option<RunningPod>> {
    let res = runtime::create_pod(&config.runtime, dns_ip, pod).await?;

    if let Some(ref running_pod) = res {
        firewall::create_rules_for_pod(&config.firewall, running_pod.address, &running_pod.ports)
            .await?;
    }

    Ok(res)
}

/// Start a container in a pod.  Do not wait for it to terminate, just let it
/// go.
pub async fn start_container(
    config: &Config,
    running_pod: &RunningPod,
    container: &PodContainerSpec,
) -> std::io::Result<bool> {
    runtime::start_container(&config.runtime, running_pod, container).await
}

/// Wait for all the containers in the pod to terminate.  Returns `true` if all
/// containers exited successfully.
pub async fn wait(
    config: &Config,
    running_pod: &RunningPod,
    kill_rx: &mut oneshot::Receiver<oneshot::Sender<()>>,
) -> std::io::Result<WFC> {
    runtime::wait_for_containers(&config.runtime, running_pod, kill_rx).await
}

/// Terminate any running containers in a pod, and delete it.
pub async fn terminate_pod(config: &Config, running_pod: &RunningPod) -> std::io::Result<()> {
    firewall::delete_rules_for_pod(&config.firewall, running_pod.address, &running_pod.ports)
        .await?;
    runtime::terminate_pod(&config.runtime, running_pod).await?;

    Ok(())
}
