use crate::parser::parse;
use actix_web::http::*;
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
use serde::Serialize;

#[derive(Serialize)]
struct AuthResponse {
    data: AuthResponseMain,
    message: String,
    code: String,
    success: bool,
}

#[derive(Serialize)]
struct AuthResponseMain {
    token: String,
    master_validity_in_seconds: String,
    display_user_name: String,
    server_version: String,
    first_login: bool,
    rem_me_token: String,
    rem_me_validity_in_seconds: String,
    health_check_interval: String,
    new_client_for_upgrade: String,
    session_id: String,
    parameters: Vec<Parameter>,
    session_info: String,
}

#[derive(Serialize)]
struct Parameter {
    name: String,
    value: String,
}

#[derive(Serialize)]
struct AuthResponseSessionInfo {
    database_name: String,
    schema_name: String,
    ware_house_name: String,
    role_name: String,
}

#[derive(Serialize)]
struct ExecResponse {
    data: ExecResponseData,
    message: String,
    code: String,
    success: bool,
}

#[derive(Serialize)]
struct ExecResponseData {}

async fn authenticate(req: HttpRequest) -> HttpResponse {
    println!("{:?}", req);
    // let query = "SELECT 1;";
    // let stmt = parse(query);
    // stmt

    let auth = AuthResponse {
        data: AuthResponseMain {
            token: "".to_string(),
            master_validity_in_seconds: "".to_string(),
            display_user_name: "".to_string(),
            server_version: "".to_string(),
            first_login: true,
            rem_me_token: "".to_string(),
            rem_me_validity_in_seconds: "".to_string(),
            health_check_interval: "".to_string(),
            new_client_for_upgrade: "".to_string(),
            session_id: "".to_string(),
            parameters: vec![],
            session_info: "".to_string(),
        },
        message: "".to_string(),
        code: "".to_string(),
        success: true,
    };

    let body = serde_json::to_string(&auth).unwrap();

    HttpResponse::Ok()
        .content_type("application/json")
        .body(body)
}

async fn query_request(req: HttpRequest) -> HttpResponse {
    println!("{:?}", req);
    let exec_response = ExecResponse {
        data: ExecResponseData {},
        message: "".to_string(),
        code: "".to_string(),
        success: true,
    };
    let body = serde_json::to_string(&exec_response).unwrap();
    HttpResponse::Ok().status(StatusCode::OK).body(body)
}

#[actix_web::main]
pub async fn run(host: &str, port: u16) -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .service(web::resource("/session/v1/login-request").to(authenticate))
            .service(web::resource("/queries/v1/query-request").to(query_request))
    })
    .bind((host, port))?
    .run()
    .await
}
