//! Snowflake Emulator Server Library

pub mod handlers;
pub mod state;

use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use state::AppState;

/// サーバーを構築
pub fn build_router() -> Router {
    let state = Arc::new(AppState::new());

    Router::new()
        .route("/api/v2/statements", post(handlers::execute_statement))
        .route(
            "/api/v2/statements/{statementHandle}",
            get(handlers::get_statement_status),
        )
        .route(
            "/api/v2/statements/{statementHandle}/cancel",
            post(handlers::cancel_statement),
        )
        .route("/health", get(handlers::health_check))
        .route("/session/v1/login-request", post(handlers::login_request))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}

/// サーバーを起動
pub async fn run(host: &str, port: u16) -> std::io::Result<()> {
    let app = build_router();
    let addr = format!("{}:{}", host, port);

    tracing::info!("Snowflake Emulator listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
