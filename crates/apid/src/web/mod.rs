pub mod errors;
pub mod resources;

use axum::routing::{delete, get, patch, post};
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
