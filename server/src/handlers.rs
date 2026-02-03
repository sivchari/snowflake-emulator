//! HTTP Handlers for Snowflake SQL API v2

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use engine::protocol::{ErrorResponse, StatementRequest, V1QueryRequest, V1QueryResponse};

use crate::state::AppState;

/// SQL execution handler
///
/// POST /api/v2/statements
pub async fn execute_statement(
    State(state): State<Arc<AppState>>,
    Json(request): Json<StatementRequest>,
) -> impl IntoResponse {
    tracing::info!("Executing SQL: {}", request.statement);

    // Get executor and execute SQL
    let executor = state.session_manager.executor();

    match executor.execute(&request.statement).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => {
            tracing::error!("SQL execution error: {}", e);
            let error_response = ErrorResponse {
                code: e.error_code().to_string(),
                message: e.to_string(),
                sql_state: e.sql_state().to_string(),
                statement_handle: None,
            };
            (StatusCode::UNPROCESSABLE_ENTITY, Json(error_response)).into_response()
        }
    }
}

/// Statement status handler
///
/// GET /api/v2/statements/{statementHandle}
pub async fn get_statement_status(
    State(_state): State<Arc<AppState>>,
    Path(statement_handle): Path<String>,
) -> impl IntoResponse {
    tracing::info!("Getting status for statement: {}", statement_handle);

    // TODO: Get async execution result
    // Currently only synchronous execution is supported, so return handle not found error
    let error_response = ErrorResponse {
        code: "002014".to_string(),
        message: format!("Statement handle not found: {}", statement_handle),
        sql_state: "42000".to_string(),
        statement_handle: Some(statement_handle),
    };

    (StatusCode::NOT_FOUND, Json(error_response))
}

/// Statement cancel handler
///
/// POST /api/v2/statements/{statementHandle}/cancel
pub async fn cancel_statement(
    State(_state): State<Arc<AppState>>,
    Path(statement_handle): Path<String>,
) -> impl IntoResponse {
    tracing::info!("Cancelling statement: {}", statement_handle);

    // TODO: Cancel async execution
    let response = serde_json::json!({
        "statementHandle": statement_handle,
        "message": "Statement cancelled"
    });

    (StatusCode::OK, Json(response))
}

/// Health check handler
///
/// GET /health
pub async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "healthy",
        "version": env!("CARGO_PKG_VERSION")
    }))
}

/// Login request handler (dummy authentication)
///
/// POST /session/v1/login-request
pub async fn login_request(Json(request): Json<LoginRequest>) -> impl IntoResponse {
    tracing::info!("Login request for account: {:?}", request.data.account_name);

    // Emulator skips authentication and returns dummy response
    let response = LoginResponse {
        data: LoginResponseData {
            token: Uuid::new_v4().to_string(),
            master_token: Uuid::new_v4().to_string(),
            valid_in_seconds: 3600,
            master_valid_in_seconds: 14400,
            display_user_name: request
                .data
                .login_name
                .unwrap_or_else(|| "EMULATOR_USER".to_string()),
            server_version: "8.0.0".to_string(),
            first_login: false,
            parameters: vec![],
        },
        success: true,
        message: None,
    };

    (StatusCode::OK, Json(response))
}

/// v1 query execution handler (for gosnowflake driver)
///
/// POST /queries/v1/query-request
pub async fn v1_query_request(
    State(state): State<Arc<AppState>>,
    Json(request): Json<V1QueryRequest>,
) -> impl IntoResponse {
    tracing::info!("v1 Query request: {}", request.sql_text);

    let executor = state.session_manager.executor();

    match executor.execute(&request.sql_text).await {
        Ok(response) => {
            let v1_response = V1QueryResponse::from_statement_response(&response);
            (StatusCode::OK, Json(v1_response)).into_response()
        }
        Err(e) => {
            tracing::error!("v1 Query execution error: {}", e);
            let error_response =
                V1QueryResponse::error(e.error_code(), &e.to_string(), e.sql_state());
            (StatusCode::OK, Json(error_response)).into_response()
        }
    }
}

/// Login request
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub data: LoginRequestData,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[allow(dead_code)]
pub struct LoginRequestData {
    pub account_name: Option<String>,
    pub login_name: Option<String>,
    pub password: Option<String>,
    pub client_app_id: Option<String>,
    pub client_app_version: Option<String>,
}

/// Login response
#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub data: LoginResponseData,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginResponseData {
    pub token: String,
    pub master_token: String,
    pub valid_in_seconds: i64,
    pub master_valid_in_seconds: i64,
    pub display_user_name: String,
    pub server_version: String,
    pub first_login: bool,
    pub parameters: Vec<SessionParameter>,
}

#[derive(Debug, Serialize)]
pub struct SessionParameter {
    pub name: String,
    pub value: String,
}
