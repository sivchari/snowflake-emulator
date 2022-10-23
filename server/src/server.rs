use actix_web::{web, App, HttpServer};
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use std::fs::{remove_file, File};
use std::sync::Arc;

use crate::{auth, query_request};

#[actix_web::main]
pub async fn run(host: &str, port: u16) -> std::io::Result<()> {
    let ctx = Arc::new(Context::new().await);
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(ctx.clone()))
            .service(auth::handler)
            .service(query_request::handler)
    })
    .bind((host, port))?
    .run()
    .await?;
    remove_file("snowflake-emulator")
}

pub struct Context {
    pub pool: SqlitePool,
}

impl Context {
    pub async fn new() -> Self {
        let _ = File::create("snowflake-emulator").unwrap();
        let pool = SqlitePoolOptions::new()
            .connect("snowflake-emulator")
            .await
            .unwrap();
        Self { pool }
    }
}
