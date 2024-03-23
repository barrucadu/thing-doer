use std::fmt;
use std::net::Ipv4Addr;
use std::process::Stdio;
use tokio::process::Command;

use nodelib::resources::pod::*;

/// Options to configure the podman pods and containers.
#[derive(Clone, Debug)]
pub struct Config {
    /// Name of the iptables binary.
    pub command: String,

    /// Name of the iptables chains.
    pub chain: String,
}

///////////////////////////////////////////////////////////////////////////////

/// Create the iptables `filter` chain (prepended to the `FORWARD` and `OUTPUT`
/// chains) and `nat` chain (appended to the `PREROUTING` and `OUTPUT` chains).
pub async fn initialise(config: &Config) -> std::io::Result<()> {
    // nat - append to PREROUTING and OUTPUT
    config.create_chain(Scope::Nat).await?;

    {
        let mut cmd = config.iptables(Scope::Nat);
        cmd.args(["-A", "PREROUTING", "-j", &config.chain]);
        let _ = cmd.spawn()?.wait().await?;
    }

    {
        let mut cmd = config.iptables(Scope::Nat);
        cmd.args(["-A", "OUTPUT", "-j", &config.chain]);
        let _ = cmd.spawn()?.wait().await?;
    }

    // filter - prepend to FORWARD and OUTPUT
    config.create_chain(Scope::Filter).await?;

    {
        let mut cmd = config.iptables(Scope::Filter);
        cmd.args(["-I", "FORWARD", "-j", &config.chain]);
        let _ = cmd.spawn()?.wait().await?;
    }

    {
        let mut cmd = config.iptables(Scope::Filter);
        cmd.args(["-I", "OUTPUT", "-j", &config.chain]);
        let _ = cmd.spawn()?.wait().await?;
    }

    Ok(())
}

/// Delete all the iptables rules and chains.
pub async fn teardown(config: &Config) -> std::io::Result<()> {
    // nat - remove from PREROUTING and OUTPUT
    {
        let mut cmd = config.iptables(Scope::Nat);
        cmd.args(["-D", "PREROUTING", "-j", &config.chain]);
        let _ = cmd.spawn()?.wait().await?;
    }

    {
        let mut cmd = config.iptables(Scope::Nat);
        cmd.args(["-D", "OUTPUT", "-j", &config.chain]);
        let _ = cmd.spawn()?.wait().await?;
    }

    config.drop_chain(Scope::Nat).await?;

    // filter - remove reom FORWARD and OUTPUT
    {
        let mut cmd = config.iptables(Scope::Filter);
        cmd.args(["-D", "FORWARD", "-j", &config.chain]);
        let _ = cmd.spawn()?.wait().await?;
    }

    {
        let mut cmd = config.iptables(Scope::Filter);
        cmd.args(["-D", "OUTPUT", "-j", &config.chain]);
        let _ = cmd.spawn()?.wait().await?;
    }

    config.drop_chain(Scope::Filter).await?;

    Ok(())
}

///////////////////////////////////////////////////////////////////////////////

/// Create the iptables rules for a pod: `nat` rules for port mapping, `filter`
/// rules to block all but the published ports.
pub async fn create_rules_for_pod(
    config: &Config,
    address: Ipv4Addr,
    ports: &[PodPortSpec],
) -> std::io::Result<()> {
    for rule in &generate_nat_rules(address, ports) {
        config.append_rule(Scope::Nat, rule).await?;
    }
    for rule in &generate_filter_rules(address, ports) {
        config.append_rule(Scope::Filter, rule).await?;
    }

    Ok(())
}

/// Clean up the iptables rules for a pod.
pub async fn delete_rules_for_pod(
    config: &Config,
    address: Ipv4Addr,
    ports: &[PodPortSpec],
) -> std::io::Result<()> {
    for rule in &generate_nat_rules(address, ports) {
        config.remove_rule(Scope::Nat, rule).await?;
    }
    for rule in &generate_filter_rules(address, ports) {
        config.remove_rule(Scope::Filter, rule).await?;
    }

    Ok(())
}

///////////////////////////////////////////////////////////////////////////////

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

/// Generate the filter rules for a set of port mappings.
fn generate_filter_rules(address: Ipv4Addr, ports: &[PodPortSpec]) -> Vec<Vec<String>> {
    let mut out = Vec::with_capacity(ports.len() + 1);

    for port in ports {
        let (container, cluster) = match port {
            PodPortSpec::Expose(container) => (*container, None),
            PodPortSpec::Map { container, cluster } => (*container, *cluster),
        };

        let mut args = vec![
            "-d".to_string(),
            format!("{address}/32"),
            "-p".to_string(),
            "tcp".to_string(),
        ];
        if let Some(exposed) = cluster {
            args.append(&mut vec![
                "-m".to_string(),
                "conntrack".to_string(),
                "--ctstate".to_string(),
                "DNAT".to_string(),
                "--ctorigdstport".to_string(),
                format!("{exposed}"),
                "--ctdir".to_string(),
                "ORIGINAL".to_string(),
            ]);
        }
        args.append(&mut vec![
            "--dport".to_string(),
            format!("{container}"),
            "-j".to_string(),
            "RETURN".to_string(),
        ]);
        out.push(args);
    }

    out.push(vec![
        "-d".to_string(),
        format!("{address}/32"),
        "-m".to_string(),
        "conntrack".to_string(),
        "!".to_string(),
        "--ctstate".to_string(),
        "RELATED,ESTABLISHED".to_string(),
        "-j".to_string(),
        "REJECT".to_string(),
    ]);

    out
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Copy, Clone)]
enum Scope {
    Filter,
    Nat,
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Filter => write!(f, "filter"),
            Self::Nat => write!(f, "nat"),
        }
    }
}

impl Config {
    fn iptables(&self, scope: Scope) -> Command {
        let mut cmd = Command::new(self.command.clone());
        cmd.stdin(Stdio::null()).kill_on_drop(true);
        cmd.args(["-t", &format!("{scope}")]);
        cmd
    }

    async fn create_chain(&self, scope: Scope) -> std::io::Result<()> {
        let mut cmd = self.iptables(scope);
        cmd.args(["-N", &self.chain]);
        let _ = cmd.spawn()?.wait().await?;

        Ok(())
    }

    async fn append_rule(&self, scope: Scope, rule: &[String]) -> std::io::Result<()> {
        let mut cmd = self.iptables(scope);
        cmd.args(["-A", &self.chain]);
        cmd.args(rule);
        let _ = cmd.spawn()?.wait().await?;

        Ok(())
    }

    async fn remove_rule(&self, scope: Scope, rule: &[String]) -> std::io::Result<()> {
        let mut cmd = self.iptables(scope);
        cmd.args(["-D", &self.chain]);
        cmd.args(rule);
        let _ = cmd.spawn()?.wait().await?;

        Ok(())
    }

    async fn drop_chain(&self, scope: Scope) -> std::io::Result<()> {
        {
            let mut cmd = self.iptables(scope);
            cmd.args(["-F", &self.chain]);
            let _ = cmd.spawn()?.wait().await?;
        }

        {
            let mut cmd = self.iptables(scope);
            cmd.args(["-X", &self.chain]);
            let _ = cmd.spawn()?.wait().await?;
        }

        Ok(())
    }
}
