use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use std::str::FromStr;

use nodelib::dns;
use nodelib::etcd;
use nodelib::util;

use crate::web::errors::ProblemDetail;

/// List all DNS aliases for the given "from" name.
pub async fn list(
    State(etcd_config): State<etcd::Config>,
    Path((from_ns, from_hostname)): Path<(String, String)>,
) -> impl IntoResponse {
    match validate_name(&from_ns, &from_hostname) {
        Ok(from_namespace) => {
            match dns::list_aliases(&etcd_config, &from_namespace, &from_hostname).await {
                Ok(aliases) => (StatusCode::OK, Json(aliases)).into_response(),
                Err(error) => ProblemDetail::from_error(error).into_response(),
            }
        }
        Err(error) => error.into_response(),
    }
}

/// Check if a specific DNS alias exists - returns 201 No Content on success,
/// since an alias has no data associated with it.
pub async fn get(
    State(etcd_config): State<etcd::Config>,
    Path((from_ns, from_hostname, to_ns, to_hostname)): Path<(String, String, String, String)>,
) -> impl IntoResponse {
    match (
        validate_name(&from_ns, &from_hostname),
        validate_name(&to_ns, &to_hostname),
    ) {
        (Ok(from_namespace), Ok(to_namespace)) => match dns::alias_record_exists(
            &etcd_config,
            &from_namespace,
            &from_hostname,
            &to_namespace,
            &to_hostname,
        )
        .await
        {
            Ok(true) => (StatusCode::NO_CONTENT, ()).into_response(),
            Ok(false) => ProblemDetail::dns_alias_not_found(
                &from_namespace,
                &from_hostname,
                &to_namespace,
                &to_hostname,
            )
            .into_response(),
            Err(error) => ProblemDetail::from_error(error).into_response(),
        },
        (Err(error), _) | (_, Err(error)) => error.into_response(),
    }
}

/// Create a DNS alias - this is idempotent, so it doesn't check for
/// nonexistence first.
pub async fn put(
    State(etcd_config): State<etcd::Config>,
    Path((from_ns, from_hostname, to_ns, to_hostname)): Path<(String, String, String, String)>,
) -> impl IntoResponse {
    match validate_modify(&from_ns, &from_hostname, &to_ns, &to_hostname) {
        Ok((from_namespace, to_namespace)) => match dns::append_alias_record(
            &etcd_config,
            None,
            &from_namespace,
            &from_hostname,
            &to_namespace,
            &to_hostname,
        )
        .await
        {
            Ok(()) => (StatusCode::NO_CONTENT, ()).into_response(),
            Err(error) => ProblemDetail::from_error(error).into_response(),
        },
        Err(pb) => pb.into_response(),
    }
}

/// Delete a specific DNS alias.
pub async fn delete(
    State(etcd_config): State<etcd::Config>,
    Path((from_ns, from_hostname, to_ns, to_hostname)): Path<(String, String, String, String)>,
) -> impl IntoResponse {
    match validate_modify(&from_ns, &from_hostname, &to_ns, &to_hostname) {
        Ok((from_namespace, to_namespace)) => match dns::delete_alias_record(
            &etcd_config,
            &from_namespace,
            &from_hostname,
            &to_namespace,
            &to_hostname,
        )
        .await
        {
            Ok(true) => (StatusCode::NO_CONTENT, ()).into_response(),
            Ok(false) => ProblemDetail::dns_alias_not_found(
                &from_namespace,
                &from_hostname,
                &to_namespace,
                &to_hostname,
            )
            .into_response(),
            Err(error) => ProblemDetail::from_error(error).into_response(),
        },
        Err(pb) => pb.into_response(),
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Check both names are valid and that the "from" name is not in a builtin
/// namespace.
fn validate_modify(
    from_ns: &str,
    from_hn: &str,
    to_ns: &str,
    to_hn: &str,
) -> Result<(dns::Namespace, dns::Namespace), (StatusCode, Json<ProblemDetail>)> {
    let from_namespace = validate_name(from_ns, from_hn)?;
    let to_namespace = validate_name(to_ns, to_hn)?;

    if is_builtin(&from_namespace) {
        Err(ProblemDetail::modification_not_allowed(
            "Built-in DNS namespaces cannot be modified via the API.",
        ))
    } else {
        Ok((from_namespace, to_namespace))
    }
}

/// Check that the name is valid.
fn validate_name(
    ns: &str,
    hostname: &str,
) -> Result<dns::Namespace, (StatusCode, Json<ProblemDetail>)> {
    let namespace =
        dns::Namespace::from_str(ns).map_err(|_| ProblemDetail::invalid_dns_namespace(ns))?;

    if util::is_valid_dns_label(hostname) {
        Ok(namespace)
    } else {
        Err(ProblemDetail::invalid_dns_label(hostname))
    }
}

/// Check if the namespace is a builtin one - builtin namespaces cannot be
/// modified via the API.
fn is_builtin(namespace: &dns::Namespace) -> bool {
    !matches!(namespace, dns::Namespace::Custom(_))
}
