//! Snowflake Emulator Server
//!
//! Snowflake SQL API v2 互換の HTTP サーバー

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
    // ロガー初期化
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "server=debug,engine=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // アプリケーション状態
    let state = Arc::new(AppState::new());

    // ルーター構築
    let app = Router::new()
        // Snowflake SQL API v2 エンドポイント
        .route("/api/v2/statements", post(handlers::execute_statement))
        .route(
            "/api/v2/statements/{statementHandle}",
            get(handlers::get_statement_status),
        )
        .route(
            "/api/v2/statements/{statementHandle}/cancel",
            post(handlers::cancel_statement),
        )
        // ヘルスチェック
        .route("/health", get(handlers::health_check))
        // 認証エンドポイント（ダミー）
        .route("/session/v1/login-request", post(handlers::login_request))
        // ミドルウェア
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state);

    // サーバー起動
    let addr = "0.0.0.0:8080";
    tracing::info!("Snowflake Emulator listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
