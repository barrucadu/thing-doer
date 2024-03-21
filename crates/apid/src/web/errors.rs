use axum::extract::Json;
use axum::http::StatusCode;
use serde::Serialize;

use nodelib::dns::Namespace;
use nodelib::error::Error;

/// An RFC7807 "problem detail" object.
#[derive(Serialize)]
pub struct ProblemDetail {
    #[serde(rename = "type")]
    rtype: String,
    title: String,
    detail: String,
    status: u16,
}

impl ProblemDetail {
    pub fn from_error(error: Error) -> (StatusCode, Json<Self>) {
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
    pub fn internal_server_error(detail: &str) -> (StatusCode, Json<Self>) {
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
    pub fn resource_already_exists(rtype: &str, rname: &str) -> (StatusCode, Json<Self>) {
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
    pub fn resource_invalid(detail: &str) -> (StatusCode, Json<Self>) {
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
    pub fn resource_not_found(rtype: &str, rname: &str) -> (StatusCode, Json<Self>) {
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
    pub fn modification_not_allowed(detail: &str) -> (StatusCode, Json<Self>) {
        let status = StatusCode::UNPROCESSABLE_ENTITY;
        let pd = Self {
            rtype: "modification-not-allowed".to_string(),
            title: "The given change is not permitted.".to_string(),
            detail: detail.to_string(),
            status: status.as_u16(),
        };

        (status, Json(pd))
    }

    /// TODO: rtype URI
    pub fn invalid_dns_namespace(ns: &str) -> (StatusCode, Json<Self>) {
        let status = StatusCode::UNPROCESSABLE_ENTITY;
        let pd = Self {
            rtype: "invalid-dns-namespace".to_string(),
            title: "The given DNS namespace is invalid".to_string(),
            detail: format!("'{ns}' is not a valid DNS namespace: a namespace must be a valid DNS label and not clash with any builtins."),
            status: status.as_u16(),
        };

        (status, Json(pd))
    }

    /// TODO: rtype URI
    pub fn invalid_dns_label(lbl: &str) -> (StatusCode, Json<Self>) {
        let status = StatusCode::UNPROCESSABLE_ENTITY;
        let pd = Self {
            rtype: "invalid-dns-label".to_string(),
            title: "The given DNS label is invalid".to_string(),
            detail: format!("'{lbl}' is not a valid DNS label: a label must be lowercase; at most 63 characters long; consist only of ASCII letters, numbers, and hyphens; start with a letter; and not end with a hyphen."),
            status: status.as_u16(),
        };

        (status, Json(pd))
    }

    /// TODO: rtype URI
    pub fn dns_alias_not_found(
        from_ns: &Namespace,
        from_hn: &str,
        to_ns: &Namespace,
        to_hn: &str,
    ) -> (StatusCode, Json<Self>) {
        let status = StatusCode::NOT_FOUND;
        let pd = Self {
            rtype: "dns-alias-not-found".to_string(),
            title: "The given DNS alias does not exist.".to_string(),
            detail: format!(
                "The DNS alias '{from_ns}/{from_hn}' -> '{to_ns}/{to_hn}' does not exist."
            ),
            status: status.as_u16(),
        };

        (status, Json(pd))
    }
}
