pub mod dns_aliases;
pub mod errors;
pub mod resources;

use axum::routing::{delete, get, patch, post, put};
use axum::Router;
use std::net::Ipv4Addr;
use std::process;

use nodelib::etcd;

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
        .route(
            "/dns_aliases/:from_namespace/:from_hostname",
            get(dns_aliases::list),
        )
        .route(
            "/dns_aliases/:from_namespace/:from_hostname/:to_namespace/:to_hostname",
            get(dns_aliases::get),
        )
        .route(
            "/dns_aliases/:from_namespace/:from_hostname/:to_namespace/:to_hostname",
            put(dns_aliases::put),
        )
        .route(
            "/dns_aliases/:from_namespace/:from_hostname/:to_namespace/:to_hostname",
            delete(dns_aliases::delete),
        )
        .route("/resources", post(resources::create))
        .route("/resources/:type", get(resources::list))
        .route("/resources/:type/:name", get(resources::get))
        .route("/resources/:type/:name", patch(resources::patch))
        .route("/resources/:type/:name", delete(resources::delete))
        .with_state(etcd_config);

    let listener = tokio::net::TcpListener::bind((address, 80)).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
