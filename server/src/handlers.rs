//! HTTP Handlers for Snowflake SQL API v2

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use engine::protocol::{
    ErrorResponse, ResultSetMetaData, StatementRequest, V1QueryRequest, V1QueryResponse,
};

use crate::state::{AppState, AsyncQueryState};

/// Query parameters for statement execution
#[derive(Debug, Deserialize, Default)]
pub struct ExecuteStatementParams {
    /// If true, execute asynchronously
    #[serde(default)]
    pub r#async: bool,
}

/// SQL execution handler
///
/// POST /api/v2/statements
/// Query params:
/// - async: If true, execute asynchronously and return statement handle
pub async fn execute_statement(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ExecuteStatementParams>,
    Json(request): Json<StatementRequest>,
) -> impl IntoResponse {
    tracing::info!(
        "Executing SQL (async={}): {}",
        params.r#async,
        request.statement
    );

    // Generate statement handle
    let statement_handle = Uuid::new_v4().to_string();

    if params.r#async {
        // Async execution: spawn task and return immediately
        let cancel_token = state.register_async_query(statement_handle.clone());
        let executor = Arc::clone(state.session_manager.executor());
        let sql = request.statement.clone();
        let state_clone = Arc::clone(&state);
        let handle_clone = statement_handle.clone();

        tokio::spawn(async move {
            tokio::select! {
                result = executor.execute(&sql) => {
                    match result {
                        Ok(response) => {
                            state_clone.complete_async_query(&handle_clone, response);
                        }
                        Err(e) => {
                            state_clone.fail_async_query(
                                &handle_clone,
                                e.error_code().to_string(),
                                e.to_string(),
                            );
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    tracing::info!("Query {} was cancelled", handle_clone);
                }
            }
        });

        // Return accepted response with statement handle
        let response = AsyncAcceptedResponse {
            statement_handle: statement_handle.clone(),
            statement_status_url: format!("/api/v2/statements/{statement_handle}"),
            message: "Statement execution is in progress.".to_string(),
        };

        (StatusCode::ACCEPTED, Json(response)).into_response()
    } else {
        // Sync execution: execute and return result
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
}

/// Response for async statement acceptance
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AsyncAcceptedResponse {
    pub statement_handle: String,
    pub statement_status_url: String,
    pub message: String,
}

/// Statement status handler
///
/// GET /api/v2/statements/{statementHandle}
pub async fn get_statement_status(
    State(state): State<Arc<AppState>>,
    Path(statement_handle): Path<String>,
) -> impl IntoResponse {
    tracing::info!("Getting status for statement: {}", statement_handle);

    match state.get_async_query_state(&statement_handle) {
        Some(AsyncQueryState::Running) => {
            // Still running
            let response = StatementStatusResponse {
                statement_handle: statement_handle.clone(),
                status: "running".to_string(),
                message: Some("Statement execution is in progress.".to_string()),
                data: None,
                result_set_meta_data: None,
            };
            (StatusCode::ACCEPTED, Json(response)).into_response()
        }
        Some(AsyncQueryState::Completed(result)) => {
            // Completed - return the result and cleanup
            state.remove_async_query(&statement_handle);
            (StatusCode::OK, Json(result)).into_response()
        }
        Some(AsyncQueryState::Failed { code, message }) => {
            // Failed - return error and cleanup
            state.remove_async_query(&statement_handle);
            let error_response = ErrorResponse {
                code,
                message,
                sql_state: "42000".to_string(),
                statement_handle: Some(statement_handle),
            };
            (StatusCode::UNPROCESSABLE_ENTITY, Json(error_response)).into_response()
        }
        Some(AsyncQueryState::Cancelled) => {
            // Cancelled
            state.remove_async_query(&statement_handle);
            let response = StatementStatusResponse {
                statement_handle: statement_handle.clone(),
                status: "cancelled".to_string(),
                message: Some("Statement was cancelled.".to_string()),
                data: None,
                result_set_meta_data: None,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        None => {
            // Not found
            let error_response = ErrorResponse {
                code: "002014".to_string(),
                message: format!("Statement handle not found: {statement_handle}"),
                sql_state: "42000".to_string(),
                statement_handle: Some(statement_handle),
            };
            (StatusCode::NOT_FOUND, Json(error_response)).into_response()
        }
    }
}

/// Statement status response
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatementStatusResponse {
    pub statement_handle: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<Vec<Option<String>>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_set_meta_data: Option<ResultSetMetaData>,
}

/// Statement cancel handler
///
/// POST /api/v2/statements/{statementHandle}/cancel
pub async fn cancel_statement(
    State(state): State<Arc<AppState>>,
    Path(statement_handle): Path<String>,
) -> impl IntoResponse {
    tracing::info!("Cancelling statement: {}", statement_handle);

    if state.cancel_async_query(&statement_handle) {
        let response = serde_json::json!({
            "statementHandle": statement_handle,
            "status": "cancelled",
            "message": "Statement cancelled successfully"
        });
        (StatusCode::OK, Json(response)).into_response()
    } else {
        let error_response = ErrorResponse {
            code: "002014".to_string(),
            message: format!("Statement handle not found: {statement_handle}"),
            sql_state: "42000".to_string(),
            statement_handle: Some(statement_handle),
        };
        (StatusCode::NOT_FOUND, Json(error_response)).into_response()
    }
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
///
/// Accepts raw body to handle both JSON and other content types from
/// different Snowflake connector implementations (Go, Python, etc.).
pub async fn login_request(body: axum::body::Bytes) -> impl IntoResponse {
    // Try to parse as JSON; if it fails, use defaults
    let request: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();

    let login_name = request
        .pointer("/data/LOGIN_NAME")
        .or_else(|| request.pointer("/data/login_name"))
        .and_then(|v| v.as_str())
        .unwrap_or("EMULATOR_USER");

    let account_name = request
        .pointer("/data/ACCOUNT_NAME")
        .or_else(|| request.pointer("/data/account_name"))
        .and_then(|v| v.as_str())
        .unwrap_or("test_account");

    tracing::info!("Login request for account: {}", account_name);

    // Session parameters that Python and Go connectors expect
    let parameters = default_session_parameters();

    let response = LoginResponse {
        data: LoginResponseData {
            token: Uuid::new_v4().to_string(),
            master_token: Uuid::new_v4().to_string(),
            valid_in_seconds: 3600,
            master_valid_in_seconds: 14400,
            display_user_name: login_name.to_string(),
            server_version: "8.0.0".to_string(),
            first_login: false,
            parameters,
        },
        success: true,
        message: None,
    };

    (StatusCode::OK, Json(response))
}

/// Return default session parameters expected by Snowflake connectors
fn default_session_parameters() -> Vec<SessionParameter> {
    vec![
        SessionParameter {
            name: "AUTOCOMMIT".to_string(),
            value: serde_json::Value::Bool(true),
        },
        SessionParameter {
            name: "CLIENT_SESSION_KEEP_ALIVE".to_string(),
            value: serde_json::Value::Bool(false),
        },
        SessionParameter {
            name: "CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY".to_string(),
            value: serde_json::Value::Number(3600.into()),
        },
        SessionParameter {
            name: "CLIENT_PREFETCH_THREADS".to_string(),
            value: serde_json::Value::Number(4.into()),
        },
        SessionParameter {
            name: "TIMEZONE".to_string(),
            value: serde_json::Value::String("America/Los_Angeles".to_string()),
        },
        SessionParameter {
            name: "TIMESTAMP_OUTPUT_FORMAT".to_string(),
            value: serde_json::Value::String("YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM".to_string()),
        },
        SessionParameter {
            name: "TIMESTAMP_NTZ_OUTPUT_FORMAT".to_string(),
            value: serde_json::Value::String("YYYY-MM-DD HH24:MI:SS.FF3".to_string()),
        },
        SessionParameter {
            name: "TIMESTAMP_LTZ_OUTPUT_FORMAT".to_string(),
            value: serde_json::Value::String(String::new()),
        },
        SessionParameter {
            name: "TIMESTAMP_TZ_OUTPUT_FORMAT".to_string(),
            value: serde_json::Value::String(String::new()),
        },
        SessionParameter {
            name: "DATE_OUTPUT_FORMAT".to_string(),
            value: serde_json::Value::String("YYYY-MM-DD".to_string()),
        },
        SessionParameter {
            name: "TIME_OUTPUT_FORMAT".to_string(),
            value: serde_json::Value::String("HH24:MI:SS".to_string()),
        },
        SessionParameter {
            name: "BINARY_OUTPUT_FORMAT".to_string(),
            value: serde_json::Value::String("HEX".to_string()),
        },
        SessionParameter {
            name: "CLIENT_TIMESTAMP_TYPE_MAPPING".to_string(),
            value: serde_json::Value::String("TIMESTAMP_LTZ".to_string()),
        },
        SessionParameter {
            name: "ENABLE_STAGE_S3_PRIVATELINK_FOR_US_EAST_1".to_string(),
            value: serde_json::Value::Bool(false),
        },
        SessionParameter {
            name: "CLIENT_RESULT_COLUMN_CASE_INSENSITIVE".to_string(),
            value: serde_json::Value::Bool(false),
        },
        SessionParameter {
            name: "SERVICE_NAME".to_string(),
            value: serde_json::Value::String(String::new()),
        },
        SessionParameter {
            name: "CLIENT_TELEMETRY_ENABLED".to_string(),
            value: serde_json::Value::Bool(false),
        },
        SessionParameter {
            name: "CLIENT_CONSENT_CACHE_ID_TOKEN".to_string(),
            value: serde_json::Value::Bool(false),
        },
        SessionParameter {
            name: "CLIENT_RESULT_CHUNK_SIZE".to_string(),
            value: serde_json::Value::Number(160.into()),
        },
    ]
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
    pub value: serde_json::Value,
}

/// Token renewal handler (no-op for emulator)
///
/// POST /session/token-request
pub async fn token_request() -> impl IntoResponse {
    let response = serde_json::json!({
        "data": {
            "sessionToken": Uuid::new_v4().to_string(),
            "validInSeconds": 3600
        },
        "success": true,
        "message": null
    });
    (StatusCode::OK, Json(response))
}

/// Session delete handler (no-op for emulator)
///
/// DELETE /session
pub async fn session_delete() -> impl IntoResponse {
    let response = serde_json::json!({
        "success": true,
        "message": null
    });
    (StatusCode::OK, Json(response))
}

/// Telemetry handler (no-op, accepts and discards)
///
/// POST /telemetry/send
pub async fn telemetry_noop() -> impl IntoResponse {
    let response = serde_json::json!({
        "success": true
    });
    (StatusCode::OK, Json(response))
}

/// Session heartbeat handler (no-op for emulator)
///
/// POST /session/heartbeat
pub async fn session_heartbeat() -> impl IntoResponse {
    let response = serde_json::json!({
        "success": true,
        "message": null
    });
    (StatusCode::OK, Json(response))
}
