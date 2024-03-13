use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, patch, post};
use axum::Router;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::process;
use std::str::FromStr;

use nodelib::error::{Error, ResourceError};
use nodelib::etcd;
use nodelib::heartbeat;
use nodelib::resources;
use nodelib::resources::node::*;
use nodelib::resources::pod::*;
use nodelib::resources::Resource;

/// Exit code to use if the server fails to start
pub static EXIT_FAILURE: i32 = 1;

pub async fn serve(etcd_config: etcd::Config, address: Ipv4Addr) {
    if let Err(error) = run(etcd_config, address).await {
        tracing::error!(?error, "could not serve application, terminating...");
        process::exit(EXIT_FAILURE);
    }
}

async fn run(etcd_config: etcd::Config, address: Ipv4Addr) -> std::io::Result<()> {
    let app = Router::new()
        .route("/resources", post(create_resource))
        .route("/resources/:type", get(list_resources))
        .route("/resources/:type/:name", get(get_resource))
        .route("/resources/:type/:name", patch(patch_resource))
        .route("/resources/:type/:name", delete(delete_resource))
        .with_state(etcd_config);

    let listener = tokio::net::TcpListener::bind((address, 80)).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

///////////////////////////////////////////////////////////////////////////////

// TODO: return a `ProblemDetail` if the request fails to decode - see
// https://github.com/tokio-rs/axum/blob/main/examples/customize-extractor-error/src/custom_extractor.rs

/// Create a resource.
async fn create_resource(
    State(etcd_config): State<etcd::Config>,
    Json(payload): Json<Resource>,
) -> impl IntoResponse {
    if is_node_type(&payload.rtype) {
        ProblemDetail::resource_invalid("Node resources cannot be created via the API.")
            .into_response()
    } else {
        let name = payload.name.clone();
        let rtype = payload.rtype.clone();

        let res = if is_pod_type(&rtype) {
            match PodResource::try_from(payload.clone()) {
                Ok(pod) => resources::create_and_schedule_pod(&etcd_config, pod).await,
                Err(error) => Err(Error::Resource(error)),
            }
        } else {
            resources::create_or_replace(&etcd_config, false, payload.clone()).await
        };

        match res {
            Ok(true) => (StatusCode::CREATED, Json(payload)).into_response(),
            Ok(false) => ProblemDetail::resource_already_exists(&rtype, &name).into_response(),
            Err(error) => ProblemDetail::from_error(error).into_response(),
        }
    }
}

/// List all resources of the given type.
async fn list_resources(
    State(etcd_config): State<etcd::Config>,
    Path(rtype): Path<String>,
) -> impl IntoResponse {
    match resources::list(&etcd_config, &rtype).await {
        Ok(rs) => match fix_node_and_pod_states(&etcd_config, rs).await {
            Ok(rs) => (StatusCode::OK, Json(rs)).into_response(),
            Err(error) => ProblemDetail::from_error(error).into_response(),
        },
        Err(error) => ProblemDetail::from_error(error).into_response(),
    }
}

/// Get a specific resource.
async fn get_resource(
    State(etcd_config): State<etcd::Config>,
    Path((rtype, rname)): Path<(String, String)>,
) -> impl IntoResponse {
    match resources::get(&etcd_config, &rtype, &rname).await {
        Ok(Some(r)) => match fix_node_and_pod_states(&etcd_config, vec![r]).await {
            Ok(rs) => (StatusCode::OK, Json(rs[0].clone())).into_response(),
            Err(error) => ProblemDetail::from_error(error).into_response(),
        },
        Ok(None) => ProblemDetail::resource_not_found(&rtype, &rname).into_response(),
        Err(error) => ProblemDetail::from_error(error).into_response(),
    }
}

/// Modify a resource in place - the `type` and `name` cannot be modified.
async fn patch_resource(
    State(etcd_config): State<etcd::Config>,
    Path((rtype, rname)): Path<(String, String)>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    if is_node_type(&rtype) {
        ProblemDetail::modification_not_allowed("Node resources cannot be modified via the API.")
            .into_response()
    } else if is_pod_type(&rtype) {
        ProblemDetail::modification_not_allowed("Pod resources cannot be modified via the API.")
            .into_response()
    } else if payload["type"].is_string() || payload["name"].is_string() {
        ProblemDetail::modification_not_allowed(
            "The 'type' and 'name' fields of a resource cannot be modified after creation.",
        )
        .into_response()
    } else {
        match resources::get(&etcd_config, &rtype, &rname).await {
            Ok(Some(old_resource)) => match apply_patch(old_resource, payload) {
                Ok(new_resource) => {
                    match resources::create_or_replace(&etcd_config, true, new_resource.clone())
                        .await
                    {
                        Ok(_) => (StatusCode::OK, Json(new_resource)).into_response(),
                        Err(error) => ProblemDetail::from_error(error).into_response(),
                    }
                }
                Err(error) => ProblemDetail::from_error(error.into()).into_response(),
            },
            Ok(None) => ProblemDetail::resource_not_found(&rtype, &rname).into_response(),
            Err(error) => ProblemDetail::from_error(error).into_response(),
        }
    }
}

/// Delete a specific resource.
async fn delete_resource(
    State(etcd_config): State<etcd::Config>,
    Path((rtype, rname)): Path<(String, String)>,
) -> impl IntoResponse {
    if is_node_type(&rtype) {
        ProblemDetail::modification_not_allowed("Node resources cannot be deleted via the API.")
            .into_response()
    } else {
        let res = if is_pod_type(&rtype) {
            resources::delete_and_kill_pod(&etcd_config, &rname).await
        } else {
            resources::delete(&etcd_config, &rtype, &rname).await
        };

        match res {
            Ok(true) => (StatusCode::NO_CONTENT, ()).into_response(),
            Ok(false) => ProblemDetail::resource_not_found(&rtype, &rname).into_response(),
            Err(error) => ProblemDetail::from_error(error).into_response(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

/// An RFC7807 "problem detail" object.
#[derive(Serialize)]
struct ProblemDetail {
    #[serde(rename = "type")]
    rtype: String,
    title: String,
    detail: String,
    status: u16,
}

impl ProblemDetail {
    fn from_error(error: Error) -> (StatusCode, Json<Self>) {
        match error {
            Error::Resource(r) => Self::resource_invalid(&format!("{r:?}")),
            Error::EtcdResponse(_)
            | Error::FromUtf8(_)
            | Error::Streaming(_)
            | Error::TonicStatus(_)
            | Error::TonicTransport(_) => Self::internal_server_error(&format!("{error:?}")),
        }
    }

    /// TODO: rtype URI
    fn internal_server_error(detail: &str) -> (StatusCode, Json<Self>) {
        let status = StatusCode::INTERNAL_SERVER_ERROR;
        let pd = Self {
            rtype: "internal-server-error".to_string(),
            title: "Something went badly wrong, check the logs.".to_string(),
            detail: detail.to_string(),
            status: status.as_u16(),
        };

        (status, Json(pd))
    }

    /// TODO: rtype URI
    fn resource_already_exists(rtype: &str, rname: &str) -> (StatusCode, Json<Self>) {
        let status = StatusCode::CONFLICT;
        let pd = Self {
            rtype: "resource-already-exists".to_string(),
            title: "The given resource already exists.".to_string(),
            detail: format!("The resource '{rname}' of type '{rtype}' already exists.  If you want to replace it, delete it first."),
            status: status.as_u16(),
        };

        (status, Json(pd))
    }

    /// TODO: rtype URI
    fn resource_invalid(detail: &str) -> (StatusCode, Json<Self>) {
        let status = StatusCode::UNPROCESSABLE_ENTITY;
        let pd = Self {
            rtype: "resource-invalid".to_string(),
            title: "The given resource is invalid.".to_string(),
            detail: detail.to_string(),
            status: status.as_u16(),
        };

        (status, Json(pd))
    }

    /// TODO: rtype URI
    fn resource_not_found(rtype: &str, rname: &str) -> (StatusCode, Json<Self>) {
        let status = StatusCode::NOT_FOUND;
        let pd = Self {
            rtype: "resource-not-found".to_string(),
            title: "The given resource does not exist.".to_string(),
            detail: format!("The resource '{rname}' of type '{rtype}' does not exist."),
            status: status.as_u16(),
        };

        (status, Json(pd))
    }

    /// TODO: rtype URI
    fn modification_not_allowed(detail: &str) -> (StatusCode, Json<Self>) {
        let status = StatusCode::UNPROCESSABLE_ENTITY;
        let pd = Self {
            rtype: "modification-not-allowed".to_string(),
            title: "The given change is not permitted.".to_string(),
            detail: detail.to_string(),
            status: status.as_u16(),
        };

        (status, Json(pd))
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Apply a patch to a resource.
///
/// TODO: implement some sort of deep merge
fn apply_patch(old: Resource, mut new: Value) -> Result<Resource, ResourceError> {
    new["name"] = serde_json::json!(old.name);
    new["type"] = serde_json::json!(old.rtype);

    if new["state"].is_null() {
        new["state"] = serde_json::json!(old.state);
    }
    if new["metadata"].is_null() {
        new["metadata"] = serde_json::json!(old.metadata);
    }
    if new["spec"].is_null() {
        new["spec"] = serde_json::json!(old.spec);
    }

    Resource::try_from(new)
}

/// Look up node healthchecks and fill in the node and pod states.
async fn fix_node_and_pod_states(
    etcd_config: &etcd::Config,
    mut resources: Vec<Resource>,
) -> Result<Vec<Resource>, Error> {
    let mut node_states = HashMap::new();
    for resource in resources.iter_mut() {
        if is_node_type(&resource.rtype) {
            let node_type = NodeType::from_str(&resource.rtype).unwrap();
            let node_state =
                get_node_state(etcd_config, &mut node_states, node_type, &resource.name).await?;
            resource.state = Some(node_state.to_string());
        } else if is_pod_type(&resource.rtype) {
            // we only want to change the pod state if it is currently being worked
            if let Some(node_name) = resource.metadata.get("workedBy") {
                let rstate = resource.state.clone().unwrap();
                if !PodState::from_str(&rstate).unwrap().is_terminal() {
                    let node_state =
                        get_node_state(etcd_config, &mut node_states, NodeType::Worker, node_name)
                            .await?;
                    resource.state = match node_state {
                        NodeState::Healthy => resource.state.clone(),
                        NodeState::Degraded => Some(PodState::Unhealthy.to_string()),
                        NodeState::Dead => Some(PodState::Dead.to_string()),
                    };
                }
            }
        }
    }
    Ok(resources)
}

/// Look up the state of a node.
async fn get_node_state(
    etcd_config: &etcd::Config,
    node_states: &mut HashMap<(String, NodeType), NodeState>,
    node_type: NodeType,
    node_name: &str,
) -> Result<NodeState, Error> {
    let key = (node_name.to_owned(), node_type);
    if let Some(node_state) = node_states.get(&key) {
        return Ok(*node_state);
    }

    let state = if heartbeat::is_healthy(etcd_config, node_type, node_name).await? {
        NodeState::Healthy
    } else if heartbeat::is_alive(etcd_config, node_type, node_name).await? {
        NodeState::Degraded
    } else {
        NodeState::Dead
    };

    node_states.insert(key, state);
    Ok(state)
}

/// Check if a stringified type is a node type.
fn is_node_type(rtype: &str) -> bool {
    NodeType::from_str(rtype).is_ok()
}

/// Check if a stringified type is a pod type.
fn is_pod_type(rtype: &str) -> bool {
    PodType::from_str(rtype).is_ok()
}
