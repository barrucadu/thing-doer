use std::process;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::Request;

use nodelib::etcd;
use nodelib::etcd::pb::etcdserverpb::{DeleteRangeRequest, PutRequest};
use nodelib::etcd::prefix;
use nodelib::resources::Resource;
use nodelib::types::Error;

/// Exit code in case the worker channel closes.
pub static EXIT_CODE_WORKER_FAILED: i32 = 1;

/// Start background tasks to work pods.
pub async fn initialise(etcd_config: etcd::Config) -> Result<Sender<Resource>, Error> {
    let (work_pod_tx, work_pod_rx) = mpsc::channel(128);

    tokio::spawn(work_task(etcd_config, work_pod_rx));

    Ok(work_pod_tx)
}

/// Background task to work pods.
async fn work_task(etcd_config: etcd::Config, mut work_pod_rx: Receiver<Resource>) {
    while let Some(pod) = work_pod_rx.recv().await {
        tracing::info!(pod_name = pod.name, "spawned worker task");
        tokio::spawn(work_pod(etcd_config.clone(), pod));
    }

    tracing::error!("worker channel unexpectedly closed, termianting...");
    process::exit(EXIT_CODE_WORKER_FAILED);
}

/// Proof-of-concept pod-worker
async fn work_pod(etcd_config: etcd::Config, pod: Resource) {
    // for logging
    let pod_name = pod.name.clone();

    let state = match run_pod_process(&pod).await {
        Ok(true) => "exit-success",
        Ok(false) => "exit-failure",
        Err(error) => {
            tracing::warn!(pod_name, ?error, "pod errored");
            "errored"
        }
    };

    let put_req = pod.with_state(state).to_put_request(&etcd_config);
    while let Err(error) = try_put_request(&etcd_config, &put_req).await {
        tracing::warn!(pod_name, ?error, "could not update pod state, retrying...");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    let delete_req = DeleteRangeRequest {
        key: format!(
            "{prefix}{pod_name}",
            prefix = prefix::claimed_pods(&etcd_config)
        )
        .into(),
        ..Default::default()
    };
    while let Err(error) = try_delete_request(&etcd_config, &delete_req).await {
        tracing::warn!(pod_name, ?error, "could not delete pod claim, retrying...");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Run the pod process and return whether it exited successfully or not.
async fn run_pod_process(pod: &Resource) -> std::io::Result<bool> {
    let mut pod_cmd = Command::new(pod.spec["cmd"].as_str().unwrap());
    let command = pod_cmd
        .env_clear()
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    if let Some(env) = pod.spec["env"].as_object() {
        for (ename, eval) in env {
            command.env(ename, eval.as_str().unwrap());
        }
    }

    let exit_status = command.spawn()?.wait().await?;

    Ok(exit_status.success())
}

/// Attempt a put request - `work_pod` retries failures.
async fn try_put_request(etcd_config: &etcd::Config, req: &PutRequest) -> Result<(), Error> {
    let mut kv_client = etcd_config.kv_client().await?;
    kv_client.put(Request::new(req.clone())).await?;

    Ok(())
}

/// Attempt a delete request - `work_pod` retries failures.
async fn try_delete_request(
    etcd_config: &etcd::Config,
    req: &DeleteRangeRequest,
) -> Result<(), Error> {
    let mut kv_client = etcd_config.kv_client().await?;
    kv_client.delete_range(Request::new(req.clone())).await?;

    Ok(())
}
