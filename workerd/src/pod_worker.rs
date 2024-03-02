use serde_json::Value;
use std::process;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::Request;

use nodelib::etcd;
use nodelib::etcd::pb::etcdserverpb::PutRequest;
use nodelib::etcd::prefix;
use nodelib::Error;

/// Exit code in case the worker channel closes.
pub static EXIT_CODE_WORKER_FAILED: i32 = 1;

/// Start background tasks to work pods.
pub async fn initialise(etcd_config: etcd::Config) -> Result<Sender<(String, Value)>, Error> {
    let (work_pod_tx, work_pod_rx) = mpsc::channel(128);

    tokio::spawn(work_task(etcd_config, work_pod_rx));

    Ok(work_pod_tx)
}

/// Background task to work pods.
async fn work_task(etcd_config: etcd::Config, mut work_pod_rx: Receiver<(String, Value)>) {
    while let Some((pod_name, resource)) = work_pod_rx.recv().await {
        tracing::info!(pod_name, "spawned worker task");
        tokio::spawn(work_pod(etcd_config.clone(), pod_name, resource));
    }

    tracing::error!("worker channel unexpectedly closed, termianting...");
    process::exit(EXIT_CODE_WORKER_FAILED);
}

/// Proof-of-concept pod-worker
async fn work_pod(etcd_config: etcd::Config, pod_name: String, mut pod_resource: Value) {
    pod_resource["state"] = match run_pod_process(&pod_resource).await {
        Ok(true) => "exit-success",
        Ok(false) => "exit-failure",
        Err(error) => {
            tracing::warn!(pod_name, ?error, "pod errored");
            "errored"
        }
    }
    .into();

    while let Err(error) = write_pod_resource(&etcd_config, &pod_name, &pod_resource).await {
        tracing::warn!(pod_name, ?error, "could not update pod state, retrying...");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Run the pod process and return whether it exited successfully or not.
async fn run_pod_process(pod_resource: &Value) -> std::io::Result<bool> {
    let mut pod_cmd = Command::new(pod_resource["spec"]["cmd"].as_str().unwrap());
    let command = pod_cmd
        .env_clear()
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    if let Some(env) = pod_resource["spec"]["env"].as_object() {
        for (ename, eval) in env {
            command.env(ename, eval.as_str().unwrap());
        }
    }

    let exit_status = command.spawn()?.wait().await?;

    Ok(exit_status.success())
}

/// Update the pod resource in etcd.
async fn write_pod_resource(
    etcd_config: &etcd::Config,
    pod_name: &str,
    pod_resource: &Value,
) -> Result<(), Error> {
    let mut kv_client = etcd_config.kv_client().await?;
    kv_client
        .put(Request::new(PutRequest {
            key: format!(
                "{prefix}{pod_name}",
                prefix = prefix::resource(etcd_config, "pod")
            )
            .into(),
            value: pod_resource.to_string().into(),
            ..Default::default()
        }))
        .await?;

    Ok(())
}
