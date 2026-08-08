//! wnwfx-worker — AMQP consumer with an axum liveness endpoint on :8082.

mod consumer;

use std::env;

use anyhow::Result;
use axum::{routing::get, Json, Router};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

const HTTP_ADDR: &str = "0.0.0.0:8082";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let amqp_url = env::var("AMQP_URL")?;
    let database_url = env::var("DATABASE_URL")?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    let http = tokio::spawn(serve_health());
    let consume = tokio::spawn(async move { consumer::run(&amqp_url, pool).await });

    tokio::select! {
        result = http => result??,
        result = consume => result??,
    }

    Ok(())
}

async fn serve_health() -> Result<()> {
    let app = Router::new().route(
        "/health",
        get(|| async { Json(json!({ "status": "ok", "service": "wnwfx-worker" })) }),
    );
    let listener = tokio::net::TcpListener::bind(HTTP_ADDR).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
