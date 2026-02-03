//! Snowflake Emulator Server
//!
//! HTTP server compatible with Snowflake SQL API v2

use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod handlers;
mod state;

use state::AppState;

#[tokio::main]
async fn main() {
    // Initialize logger
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "server=debug,engine=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Application state
    let state = Arc::new(AppState::new());

    // Build router
    let app = Router::new()
        // Snowflake SQL API v2 endpoints
        .route("/api/v2/statements", post(handlers::execute_statement))
        .route(
            "/api/v2/statements/{statementHandle}",
            get(handlers::get_statement_status),
        )
        .route(
            "/api/v2/statements/{statementHandle}/cancel",
            post(handlers::cancel_statement),
        )
        // Health check
        .route("/health", get(handlers::health_check))
        // Authentication endpoint (dummy)
        .route("/session/v1/login-request", post(handlers::login_request))
        // v1 API endpoint (for gosnowflake driver)
        .route(
            "/queries/v1/query-request",
            post(handlers::v1_query_request),
        )
        // Middleware
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state);

    // Start server
    let addr = "0.0.0.0:8080";
    tracing::info!("Snowflake Emulator listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
