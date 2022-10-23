use actix_web::{post, web, HttpResponse, Result};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sf::{parser, plan};
use sqlx::{Column, Row};
use std::sync::Arc;

use crate::server::Context;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct QueryRequest {
    sql_text: String,
    async_exec: bool,
    sequence_id: u32,
    is_internal: bool,
}

#[derive(Serialize)]
struct ExecResponse {
    data: ExecResponseData,
    message: String,
    code: String,
    success: bool,
}

#[derive(Serialize)]
struct ExecResponseData {
    rowtype: Vec<RowType>,
    rowset: Vec<Vec<String>>,
}

#[derive(Serialize)]
struct RowType {
    name: String,
    r#type: String,
}

#[post("/queries/v1/query-request")]
async fn handler(
    ctx: web::Data<Arc<Context>>,
    query_request: web::Json<QueryRequest>,
) -> Result<HttpResponse> {
    // exec sqlite

    // Column Type
    // `hoge` -> column name
    // 'hoge' -> string
    // 1 -> int
    // 1.1 -> float
    let mut headers = vec![];
    let mut rows = sqlx::query("SELECT 1, 1.1, 'a', true").fetch(&ctx.pool.clone());
    while let Some(row) = rows.try_next().await.unwrap() {
        headers = row.columns().iter().map(|c| c.name().to_string()).collect();
        println!("{:?}", headers);
    }

    // fail: following code
    let stmt = parser::query_to_statment(&query_request.sql_text).unwrap();
    let plan = plan::statement_to_plan(&stmt).unwrap();
    let rows = plan.execute_plan().unwrap();
    let exec_response = ExecResponse {
        data: ExecResponseData {
            rowtype: rows
                .rowtypes
                .iter()
                .map(|r| RowType {
                    name: r.name.clone(),
                    r#type: r.r#type.clone(),
                })
                .collect(),
            rowset: rows.rowsets,
        },
        message: format!(
            "{} and {} and {} and {}",
            query_request.sql_text,
            query_request.async_exec,
            query_request.is_internal,
            query_request.sequence_id
        )
        .to_string(),
        code: "".to_string(),
        success: true,
    };
    let body = serde_json::to_string(&exec_response).unwrap();
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(body))
}
