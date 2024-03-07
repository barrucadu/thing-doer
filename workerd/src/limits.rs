use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::cmp;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tonic::Request;

use nodelib::etcd;
use nodelib::etcd::leaser::LeaseId;
use nodelib::etcd::pb::etcdserverpb::request_op;
use nodelib::etcd::pb::etcdserverpb::{PutRequest, RequestOp, TxnRequest};
use nodelib::etcd::prefix;
use nodelib::resources::PodResource;
use nodelib::types::Error;

/// Exit code in case the limit channel closes.
pub static EXIT_CODE_LIMITER_FAILED: i32 = 1;

/// Default CPU request, if none is specified: 1 milliCPUs.
pub static DEFAULT_MIN_CPU: Decimal = dec!(0.001);

/// Default CPU limit, if none is specified: max(request, 0.25 CPUs).
pub static DEFAULT_MAX_CPU: Decimal = dec!(0.25);

/// Default memory request, if none is specified: 128MiB.
pub static DEFAULT_MIN_MEMORY: u64 = 128;

/// Default memory limit, if none is specified: max(request, 512MiB).
pub static DEFAULT_MAX_MEMORY: u64 = 512;

/// A handle to the shared state.
#[derive(Debug, Clone)]
pub struct State(Arc<RwLock<InnerState>>);

/// Start background task to update current resource limits as pods start and
/// stop.
pub async fn initialise(
    etcd_config: etcd::Config,
    my_name: String,
    lease_id: LeaseId,
    max_cpu: Decimal,
    max_memory: u64,
) -> Result<State, Error> {
    let (tx, rx) = mpsc::channel(128);

    try_set_resource_limits(&etcd_config, &my_name, lease_id, max_cpu, max_memory).await?;

    let inner = Arc::new(RwLock::new(InnerState {
        available_cpu: max_cpu,
        available_memory: max_memory,
        update: tx,
    }));

    tokio::spawn(update_task(
        etcd_config,
        my_name,
        lease_id,
        inner.clone(),
        rx,
    ));

    Ok(State(inner))
}

/// The resources that a running pod has claimed.
#[derive(Debug)]
pub struct CommittedPodResources {
    cpu: Decimal,
    memory: u64,
}

impl State {
    /// Work out how many resources to allocate for a pod (return `None` if
    /// there aren't enough resources available), and update the node state in
    /// etcd.
    pub async fn claim_resources(&mut self, pod: &PodResource) -> Option<CommittedPodResources> {
        let agg = pod.spec.aggregate_resources();
        let requests = agg.requests.unwrap();
        let limits = agg.limits.unwrap();

        let min_cpu = requests.cpu.unwrap_or(DEFAULT_MIN_CPU);
        let max_cpu = limits.cpu.unwrap_or(DEFAULT_MAX_CPU);
        let min_memory = requests.memory.unwrap_or(DEFAULT_MIN_MEMORY);
        let max_memory = limits.memory.unwrap_or(DEFAULT_MAX_MEMORY);

        let mut w = self.0.write().await;
        if min_cpu <= w.available_cpu && min_memory <= w.available_memory {
            let desired_cpu = cmp::max(min_cpu, max_cpu);
            let desired_memory = cmp::max(min_memory, max_memory);
            let committed = CommittedPodResources {
                cpu: cmp::min(w.available_cpu, desired_cpu),
                memory: cmp::min(w.available_memory, desired_memory),
            };
            w.available_cpu -= committed.cpu;
            w.available_memory -= committed.memory;
            let _ = w.update.try_send(());
            Some(committed)
        } else {
            None
        }
    }

    /// Release some resources, and update the node state in etcd.
    pub async fn release_resources(&mut self, claim: CommittedPodResources) {
        let mut w = self.0.write().await;
        w.available_cpu += claim.cpu;
        w.available_memory += claim.memory;
        let _ = w.update.try_send(());
    }
}

/// The internal state, only visible to the update task and query methods.
#[derive(Debug)]
struct InnerState {
    pub available_cpu: Decimal,
    pub available_memory: u64,
    pub update: Sender<()>,
}

/// Background task to update the limit keys.
async fn update_task(
    etcd_config: etcd::Config,
    my_name: String,
    lease_id: LeaseId,
    inner: Arc<RwLock<InnerState>>,
    mut rx: Receiver<()>,
) {
    while let Some(()) = rx.recv().await {
        let (cpu, memory) = {
            let r = inner.read().await;
            (r.available_cpu, r.available_memory)
        };

        tracing::info!(?cpu, ?memory, "got resource limit change");

        while let Err(error) =
            try_set_resource_limits(&etcd_config, &my_name, lease_id, cpu, memory).await
        {
            tracing::warn!(?error, "could not update resource limits, retrying...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    tracing::error!("limit delta channel unexpectedly closed, termianting...");
    process::exit(EXIT_CODE_LIMITER_FAILED);
}

/// Store the current resource limits.  `update_task` handles retries.
async fn try_set_resource_limits(
    etcd_config: &etcd::Config,
    my_name: &str,
    lease_id: LeaseId,
    cpu: Decimal,
    memory: u64,
) -> Result<(), Error> {
    let mut kv_client = etcd_config.kv_client().await?;
    kv_client
        .txn(Request::new(TxnRequest {
            compare: Vec::new(),
            success: vec![
                RequestOp {
                    request: Some(request_op::Request::RequestPut(PutRequest {
                        key: format!(
                            "{prefix}{my_name}",
                            prefix = prefix::node_available_cpu(etcd_config),
                        )
                        .into(),
                        value: cpu.to_string().into(),
                        lease: lease_id.0,
                        ..Default::default()
                    })),
                },
                RequestOp {
                    request: Some(request_op::Request::RequestPut(PutRequest {
                        key: format!(
                            "{prefix}{my_name}",
                            prefix = prefix::node_available_memory(etcd_config),
                        )
                        .into(),
                        value: memory.to_string().into(),
                        lease: lease_id.0,
                        ..Default::default()
                    })),
                },
            ],
            failure: Vec::new(),
        }))
        .await?;

    Ok(())
}
