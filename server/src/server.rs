use crate::{auth, query_request};
use actix_web::{App, HttpServer};

#[actix_web::main]
pub async fn run(host: &str, port: u16) -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .service(auth::handler)
            .service(query_request::handler)
    })
    .bind((host, port))?
    .run()
    .await
}
