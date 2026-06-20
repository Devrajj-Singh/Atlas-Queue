use axum::Router;
use axum::routing::{get, post};

use crate::api::handlers::{get_task, submit_task};
use crate::pool::control::DispatcherHandle;

#[derive(Clone)]
pub struct AppState {
    pub dispatcher: DispatcherHandle,
}

pub fn router(dispatcher: DispatcherHandle) -> Router {
    Router::new()
        .route("/tasks", post(submit_task))
        .route("/tasks/:id", get(get_task))
        .with_state(AppState { dispatcher })
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use serde_json::{Value, json};
    use tokio::time::{Duration, sleep, timeout};
    use tower::ServiceExt;

    use super::*;
    use crate::engine::core::Engine;
    use crate::engine::handler::{HandlerError, TaskHandler, TaskOutput};
    use crate::engine::registry::HandlerRegistry;
    use crate::pool::{WorkerPool, WorkerPoolConfig};

    struct EchoHandler;

    #[async_trait::async_trait]
    impl TaskHandler for EchoHandler {
        type Payload = Value;

        async fn execute(&self, payload: Self::Payload) -> Result<TaskOutput, HandlerError> {
            Ok(TaskOutput(payload))
        }
    }

    fn spawn_test_app() -> (Router, WorkerPool) {
        let mut registry = HandlerRegistry::new();
        registry.register("echo", EchoHandler);
        let pool = WorkerPool::spawn(
            Engine::new(),
            registry,
            WorkerPoolConfig {
                worker_count: 1,
                channel_capacity: 1,
                control_channel_capacity: 8,
            },
        );
        let app = router(pool.handle());

        (app, pool)
    }

    async fn response_json(response: axum::response::Response) -> Value {
        let bytes = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("response body should be readable");
        serde_json::from_slice(&bytes).expect("response should be JSON")
    }

    async fn post_task(app: Router, task_type: &str) -> (StatusCode, Value) {
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/tasks")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "task_type": task_type,
                            "payload": { "hello": "world" }
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("request should complete");
        let status = response.status();
        let body = response_json(response).await;

        (status, body)
    }

    async fn get_task(app: Router, id: &str) -> (StatusCode, Value) {
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/tasks/{id}"))
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should complete");
        let status = response.status();
        let body = response_json(response).await;

        (status, body)
    }

    async fn poll_task_status(app: Router, id: &str, expected_status: &str) -> Value {
        timeout(Duration::from_secs(2), async {
            loop {
                let (status, body) = get_task(app.clone(), id).await;
                if status == StatusCode::OK && body["status"] == expected_status {
                    return body;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("task should reach expected status")
    }

    #[tokio::test]
    async fn submit_then_get_status_full_http_round_trip() {
        let (app, pool) = spawn_test_app();

        let (status, body) = post_task(app.clone(), "echo").await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let id = body["id"].as_str().expect("id should be a string");
        uuid::Uuid::parse_str(id).expect("id should be a uuid");

        let (status, body) = get_task(app.clone(), id).await;
        assert!(
            status == StatusCode::OK || status == StatusCode::NOT_FOUND,
            "status was {status}, body was {body}"
        );
        if status == StatusCode::OK {
            assert!(matches!(
                body["status"].as_str(),
                Some("pending" | "completed" | "failed")
            ));
        } else {
            poll_task_status(app, id, "completed").await;
        }

        pool.shutdown().await;
    }

    #[tokio::test]
    async fn unknown_task_returns_404() {
        let (app, pool) = spawn_test_app();

        let (status, body) = get_task(app, &uuid::Uuid::new_v4().to_string()).await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "task not found");
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn malformed_task_id_returns_400() {
        let (app, pool) = spawn_test_app();

        let (status, body) = get_task(app, "not-a-uuid").await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "malformed task id");
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn unregistered_task_type_submits_then_eventually_fails() {
        let (app, pool) = spawn_test_app();

        let (status, body) = post_task(app.clone(), "missing").await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let id = body["id"].as_str().expect("id should be a string");

        let body = poll_task_status(app, id, "failed").await;
        assert_eq!(body["error"]["kind"], "permanent");
        assert!(
            body["error"]["message"]
                .as_str()
                .expect("error message should be a string")
                .contains("no handler registered")
        );

        pool.shutdown().await;
    }
}
