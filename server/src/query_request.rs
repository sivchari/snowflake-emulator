use crate::{parser, plan};
use actix_web::{post, web, HttpRequest, HttpResponse};
use serde::{Deserialize, Serialize};

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
    r#type: String,
}

#[post("/queries/v1/query-request")]
async fn handler(_req: HttpRequest, query_request: web::Json<QueryRequest>) -> HttpResponse {
    let stmt = parser::query_to_statment(&query_request.sql_text).unwrap();
    let plan = plan::statement_to_plan(&stmt).unwrap();
    let rows = plan.execute_plan().unwrap();
    let mut rowtypes = Vec::new();
    // TODO: refactor (generate rowtypes)
    // TODO: refactor (convert to rowsets)
    // rowtypes: [int, string, bool]
    // [["1", "a", "true"], ["2", "b", "false"]]
    let rowtype = RowType {
        r#type: "text".to_string(),
    };
    rowtypes.push(rowtype);
    let ref mut rowsets = Vec::new();
    rowsets.push(rows.rows);
    let exec_response = ExecResponse {
        data: ExecResponseData {
            rowtype: rowtypes,
            rowset: rowsets.to_vec(),
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
    HttpResponse::Ok()
        .content_type("application/json")
        .body(body)
}
