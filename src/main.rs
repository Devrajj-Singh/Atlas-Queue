use std::net::SocketAddr;

use atlas_queue::api;
use atlas_queue::engine::core::Engine;
use atlas_queue::engine::handler::{HandlerError, TaskHandler, TaskOutput};
use atlas_queue::engine::registry::HandlerRegistry;
use atlas_queue::pool::{WorkerPool, WorkerPoolConfig};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

struct EchoHandler;

#[async_trait::async_trait]
impl TaskHandler for EchoHandler {
    type Payload = Value;

    async fn execute(&self, payload: Self::Payload) -> Result<TaskOutput, HandlerError> {
        Ok(TaskOutput(payload))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let addr = std::env::var("ATLAS_QUEUE_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:3000".to_string())
        .parse::<SocketAddr>()?;
    let database_url = std::env::var("DATABASE_URL")?;
    let db = PgPoolOptions::new().connect(&database_url).await?;
    sqlx::migrate!().run(&db).await?;

    let mut registry = HandlerRegistry::new();
    registry.register("echo", EchoHandler);

    let pool = WorkerPool::spawn(
        Engine::new(db, Duration::from_secs(30)),
        registry,
        WorkerPoolConfig {
            worker_count: 4,
            channel_capacity: 16,
            control_channel_capacity: 64,
        },
    );
    let app = api::router(pool.handle());
    let listener = tokio::net::TcpListener::bind(addr).await?;

    println!("Atlas Queue REST API listening on http://{addr}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    pool.shutdown().await;
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
