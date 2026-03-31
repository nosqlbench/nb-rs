// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Web server setup and configuration.

use axum::{Router, routing::get, routing::post};
use tower_http::compression::CompressionLayer;

use crate::routes;

/// Build the Axum router with all routes.
pub fn build_router() -> Router {
    Router::new()
        // Full pages
        .route("/", get(routes::dashboard))
        .route("/functions", get(routes::functions_page))
        .route("/stdlib", get(routes::stdlib_page))
        .route("/dag", get(routes::dag_page))
        // API fragments (htmx)
        .route("/api/functions", get(routes::functions_api))
        .route("/api/activities", get(routes::activities_api))
        .route("/api/stdlib/{name}", get(routes::stdlib_source))
        .route("/api/dag/render", post(routes::dag_render))
        // Middleware
        .layer(CompressionLayer::new())
}

/// Start the web server on the given port (binds to 0.0.0.0).
pub async fn serve(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    serve_on(std::net::SocketAddr::from(([0, 0, 0, 0], port))).await
}

/// Start the web server on an explicit socket address.
pub async fn serve_on(addr: std::net::SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let app = build_router();
    eprintln!("nbrs web: listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
