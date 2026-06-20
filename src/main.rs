use std::net::SocketAddr;

use atlas_queue::api;
use atlas_queue::engine::core::Engine;
use atlas_queue::engine::handler::{HandlerError, TaskHandler, TaskOutput};
use atlas_queue::engine::registry::HandlerRegistry;
use atlas_queue::pool::{WorkerPool, WorkerPoolConfig};
use serde_json::Value;

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
    let addr = std::env::var("ATLAS_QUEUE_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:3000".to_string())
        .parse::<SocketAddr>()?;

    let mut registry = HandlerRegistry::new();
    registry.register("echo", EchoHandler);

    let pool = WorkerPool::spawn(
        Engine::new(),
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
