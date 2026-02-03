//! Integration tests for Snowflake Emulator Server

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt;

/// Helper to create a POST request with JSON body
fn post_json(uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

/// Helper to execute SQL and return response JSON
async fn execute_sql(app: &axum::Router, sql: &str) -> (StatusCode, Value) {
    let request = post_json(
        "/api/v2/statements",
        json!({
            "statement": sql
        }),
    );

    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    (status, json)
}

#[tokio::test]
async fn test_health_check() {
    let app = server::build_router();

    let request = Request::builder()
        .method("GET")
        .uri("/health")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "healthy");
}

#[tokio::test]
async fn test_select_literal() {
    let app = server::build_router();

    let (status, json) = execute_sql(&app, "SELECT 42 AS answer").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["resultSetMetaData"]["numRows"], 1);
    assert_eq!(json["data"][0][0], "42");
    assert_eq!(json["resultSetMetaData"]["rowType"][0]["name"], "answer");
}

#[tokio::test]
async fn test_create_table_insert_select() {
    let app = server::build_router();

    // CREATE TABLE
    let (status, _) = execute_sql(&app, "CREATE TABLE test_users (id INT, name VARCHAR)").await;
    assert_eq!(status, StatusCode::OK);

    // INSERT
    let (status, _) = execute_sql(
        &app,
        "INSERT INTO test_users VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // SELECT
    let (status, json) = execute_sql(&app, "SELECT id, name FROM test_users ORDER BY id").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["resultSetMetaData"]["numRows"], 2);

    let data = &json["data"];
    assert_eq!(data[0][0], "1");
    assert_eq!(data[0][1], "Alice");
    assert_eq!(data[1][0], "2");
    assert_eq!(data[1][1], "Bob");
}

#[tokio::test]
async fn test_select_with_where_clause() {
    let app = server::build_router();

    execute_sql(
        &app,
        "CREATE TABLE products (id INT, name VARCHAR, price DOUBLE)",
    )
    .await;

    execute_sql(
        &app,
        "INSERT INTO products VALUES (1, 'Apple', 1.5), (2, 'Banana', 0.5), (3, 'Cherry', 3.0)",
    )
    .await;

    let (status, json) = execute_sql(
        &app,
        "SELECT name FROM products WHERE price > 1.0 ORDER BY name",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["resultSetMetaData"]["numRows"], 2);
    assert_eq!(json["data"][0][0], "Apple");
    assert_eq!(json["data"][1][0], "Cherry");
}

#[tokio::test]
async fn test_aggregate_query() {
    let app = server::build_router();

    execute_sql(&app, "CREATE TABLE orders (amount DOUBLE)").await;
    execute_sql(&app, "INSERT INTO orders VALUES (100), (200), (300)").await;

    let (status, json) = execute_sql(
        &app,
        "SELECT COUNT(*) as cnt, SUM(amount) as total FROM orders",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["resultSetMetaData"]["numRows"], 1);
    assert_eq!(json["data"][0][0], "3");
    // SUM result can be "600" or "600.0" depending on DataFusion
    let total = json["data"][0][1].as_str().unwrap();
    assert!(total == "600" || total == "600.0");
}

#[tokio::test]
async fn test_login_request() {
    let app = server::build_router();

    let request = post_json(
        "/session/v1/login-request",
        json!({
            "data": {
                "ACCOUNT_NAME": "test_account",
                "LOGIN_NAME": "test_user",
                "PASSWORD": "test_password"
            }
        }),
    );

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["success"], true);
    assert!(json["data"]["token"].is_string());
    assert!(json["data"]["masterToken"].is_string());
}

#[tokio::test]
async fn test_sql_error_handling() {
    let app = server::build_router();

    // Invalid SQL
    let (status, json) = execute_sql(&app, "SELEC invalid syntax").await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(json["message"].as_str().unwrap().len() > 0);
    assert!(json["code"].is_string());
    assert!(json["sqlState"].is_string());
}
