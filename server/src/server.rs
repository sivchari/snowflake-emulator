use crate::parser::parse;
use actix_web::{web, App, HttpRequest, HttpServer};

async fn index(req: HttpRequest) -> String {
    println!("{:?}", req);
    let query = "SELECT 1;";
    let stmt = parse(query);
    stmt
}

#[actix_web::main]
pub async fn run(host: &str, port: u16) -> std::io::Result<()> {
    HttpServer::new(|| App::new().service(web::resource("/").to(index)))
        .bind((host, port))?
        .run()
        .await
}
