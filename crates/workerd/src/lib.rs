#![warn(clippy::pedantic)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::wildcard_imports)]

pub mod cluster_nameserver;
pub mod limits;
pub mod pod_claimer;
pub mod pod_killer;
pub mod pod_worker;
pub mod podman;
