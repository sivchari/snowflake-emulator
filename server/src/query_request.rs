use crate::{parser, plan};
use actix_web::{post, web, HttpRequest, HttpResponse};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct ExecResponse {
    data: ExecResponseData,
    message: String,
    code: String,
    success: bool,
}

#[derive(Serialize)]
struct ExecResponseData {}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct QueryRequest {
    sql_text: String,
    async_exec: bool,
    sequence_id: u32,
    is_internal: bool,
}

#[post("/queries/v1/query-request")]
async fn handler(_req: HttpRequest, query_request: web::Json<QueryRequest>) -> HttpResponse {
    // TODO: refactor
    let parser = parser::parse_to_statment(&query_request.sql_text).unwrap();
    let query = plan::statement_to_plan(&parser).unwrap();
    println!("{:?}", query);
    let exec_response = ExecResponse {
        data: ExecResponseData {},
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
    HttpResponse::Ok()
        .content_type("application/json")
        .body(body)
}
