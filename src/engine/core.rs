use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::engine::handler::{HandlerError, TaskOutput};
use crate::engine::task::{AnyTask, Completed, Failed, Pending, Running, Task, TaskId, WorkerId};

/// Postgres-backed task engine.
///
/// Earlier phases required exclusive `&mut self` access because a `HashMap`
/// was the only task store. Postgres is now the concurrency boundary: its row
/// locks and atomic statements make `Engine` shareable through `&self`, even
/// though the dispatcher still drives calls from one place in this phase.
#[derive(Debug, Clone)]
pub struct Engine {
    pool: PgPool,
    lease_duration: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("task {0} not found")]
    NotFound(TaskId),
    #[error("invalid task status in database: {0}")]
    InvalidStatus(String),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, sqlx::FromRow)]
struct TaskRow {
    id: Uuid,
    task_type: String,
    payload: Value,
    status: String,
    worker_id: Option<Uuid>,
    output: Option<Value>,
    error: Option<Value>,
    created_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    locked_until: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    failed_at: Option<DateTime<Utc>>,
}

impl Engine {
    pub fn new(pool: PgPool, lease_duration: Duration) -> Self {
        Self {
            pool,
            lease_duration,
        }
    }

    pub async fn submit(
        &self,
        task_type: impl Into<String> + Send,
        payload: Value,
    ) -> Result<TaskId, EngineError> {
        let id = TaskId::new();

        sqlx::query("INSERT INTO tasks (id, task_type, payload) VALUES ($1, $2, $3)")
            .bind(id.as_uuid())
            .bind(task_type.into())
            .bind(payload)
            .execute(&self.pool)
            .await?;

        Ok(id)
    }

    /// Atomically claims the oldest pending task, or reclaims an expired lease.
    ///
    /// `FOR UPDATE SKIP LOCKED` means concurrent callers of this statement do
    /// not block each other and cannot select the same row; Postgres skips rows
    /// another caller already locked. The `running AND locked_until < now()`
    /// branch is lease-based crash recovery: work abandoned by a dead worker
    /// becomes claimable again after its lease expires. The outer `UPDATE ...
    /// RETURNING` keeps "select" and "mark running" as one atomic statement, so
    /// there is no interleaving window between finding and claiming a row.
    /// Ordering by `created_at` preserves Phase 1 FIFO behavior in SQL.
    pub async fn next_pending(
        &self,
        worker_id: WorkerId,
    ) -> Result<Option<Task<Running>>, EngineError> {
        let interval = interval_literal(self.lease_duration);
        let row = sqlx::query_as::<_, TaskRow>(
            r#"
            UPDATE tasks
            SET status = 'running',
                worker_id = $1,
                started_at = now(),
                locked_until = now() + $2::interval
            WHERE id = (
                SELECT id FROM tasks
                WHERE status = 'pending'
                   OR (status = 'running' AND locked_until < now())
                ORDER BY created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING id, task_type, payload, status, worker_id, output, error,
                      created_at, started_at, locked_until, finished_at, failed_at
            "#,
        )
        .bind(worker_id.as_uuid())
        .bind(interval)
        .fetch_optional(&self.pool)
        .await?;

        row.map(row_to_running).transpose()
    }

    pub async fn mark_completed(
        &self,
        task: Task<Running>,
        output: TaskOutput,
    ) -> Result<TaskId, EngineError> {
        let id = task.id;
        let output =
            serde_json::to_value(output).expect("TaskOutput serialization should not fail");

        sqlx::query(
            r#"
            UPDATE tasks
            SET status = 'completed',
                output = $2,
                finished_at = now()
            WHERE id = $1
            "#,
        )
        .bind(id.as_uuid())
        .bind(output)
        .execute(&self.pool)
        .await?;

        Ok(id)
    }

    pub async fn mark_failed(
        &self,
        task: Task<Running>,
        error: HandlerError,
    ) -> Result<TaskId, EngineError> {
        let id = task.id;
        let error =
            serde_json::to_value(error).expect("HandlerError serialization should not fail");

        sqlx::query(
            r#"
            UPDATE tasks
            SET status = 'failed',
                error = $2,
                failed_at = now()
            WHERE id = $1
            "#,
        )
        .bind(id.as_uuid())
        .bind(error)
        .execute(&self.pool)
        .await?;

        Ok(id)
    }

    pub async fn requeue(&self, task: Task<Running>) -> Result<TaskId, EngineError> {
        let id = task.id;

        sqlx::query(
            r#"
            UPDATE tasks
            SET status = 'pending',
                worker_id = NULL,
                started_at = NULL,
                locked_until = NULL
            WHERE id = $1
            "#,
        )
        .bind(id.as_uuid())
        .execute(&self.pool)
        .await?;

        Ok(id)
    }

    pub async fn get(&self, id: TaskId) -> Result<AnyTask, EngineError> {
        let row = sqlx::query_as::<_, TaskRow>(
            r#"
            SELECT id, task_type, payload, status, worker_id, output, error,
                   created_at, started_at, locked_until, finished_at, failed_at
            FROM tasks
            WHERE id = $1
            "#,
        )
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;

        row.map(row_to_any_task)
            .transpose()?
            .ok_or(EngineError::NotFound(id))
    }
}

fn interval_literal(duration: Duration) -> String {
    format!("{} milliseconds", duration.as_millis())
}

/// Reconstructs Rust typestates from the database status column.
///
/// This is the boundary where compile-time typestate necessarily gives way to
/// a runtime check: SQL rows carry `status` as data, so Rust must trust and
/// validate that value before rebuilding the matching `AnyTask` variant.
fn row_to_any_task(row: TaskRow) -> Result<AnyTask, EngineError> {
    match row.status.as_str() {
        "pending" => Ok(AnyTask::Pending(Task {
            id: TaskId::from_uuid(row.id),
            task_type: row.task_type,
            payload: row.payload,
            state: Pending {
                created_at: row.created_at,
            },
        })),
        "running" => row_to_running(row).map(AnyTask::Running),
        "completed" => Ok(AnyTask::Completed(Task {
            id: TaskId::from_uuid(row.id),
            task_type: row.task_type,
            payload: row.payload,
            state: Completed {
                finished_at: required(row.finished_at, "finished_at")?,
                output: TaskOutput(required(row.output, "output")?),
            },
        })),
        "failed" => Ok(AnyTask::Failed(Task {
            id: TaskId::from_uuid(row.id),
            task_type: row.task_type,
            payload: row.payload,
            state: Failed {
                failed_at: required(row.failed_at, "failed_at")?,
                error: handler_error_from_value(required(row.error, "error")?),
            },
        })),
        other => Err(EngineError::InvalidStatus(other.to_string())),
    }
}

fn row_to_running(row: TaskRow) -> Result<Task<Running>, EngineError> {
    Ok(Task {
        id: TaskId::from_uuid(row.id),
        task_type: row.task_type,
        payload: row.payload,
        state: Running {
            started_at: required(row.started_at, "started_at")?,
            worker_id: WorkerId::from_uuid(required(row.worker_id, "worker_id")?),
            locked_until: required(row.locked_until, "locked_until")?,
        },
    })
}

fn required<T>(value: Option<T>, column: &'static str) -> Result<T, EngineError> {
    value.ok_or_else(|| EngineError::InvalidStatus(format!("missing {column}")))
}

fn handler_error_from_value(value: Value) -> HandlerError {
    let kind = value
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("permanent");
    let message = value
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("handler failed")
        .to_string();

    match kind {
        "invalid_payload" => HandlerError::InvalidPayload(message),
        "execution_failed" => HandlerError::ExecutionFailed(anyhow::anyhow!(message)),
        "transient" => HandlerError::Transient(message),
        _ => HandlerError::Permanent(message),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use futures::future::join_all;
    use serde_json::json;
    use tokio::time::{Duration, sleep};

    use super::*;

    fn engine(pool: PgPool) -> Engine {
        Engine::new(pool, Duration::from_secs(30))
    }

    #[sqlx::test]
    async fn submit_and_dequeue_marks_row_running(pool: PgPool) {
        let engine = engine(pool.clone());
        let id = engine
            .submit("email.send", json!({ "to": "a@example.com" }))
            .await
            .expect("submit should succeed");

        let running = engine
            .next_pending(WorkerId::new())
            .await
            .expect("dequeue should query")
            .expect("task should start");

        assert_eq!(running.id, id);
        let status: String = sqlx::query_scalar("SELECT status FROM tasks WHERE id = $1")
            .bind(id.as_uuid())
            .fetch_one(&pool)
            .await
            .expect("row should exist");
        assert_eq!(status, "running");
    }

    #[sqlx::test]
    async fn multi_worker_concurrent_dequeue_is_race_free(pool: PgPool) {
        let engine = engine(pool);
        let submitted =
            join_all((0..20).map(|index| engine.submit("work", json!({ "index": index }))))
                .await
                .into_iter()
                .collect::<Result<HashSet<_>, _>>()
                .expect("submits should succeed");

        let handles = (0..5)
            .map(|_| {
                let engine = engine.clone();
                tokio::spawn(async move {
                    let mut claimed = Vec::new();
                    while let Some(task) = engine
                        .next_pending(WorkerId::new())
                        .await
                        .expect("dequeue should query")
                    {
                        claimed.push(task.id);
                    }
                    claimed
                })
            })
            .collect::<Vec<_>>();

        let claimed = join_all(handles)
            .await
            .into_iter()
            .flat_map(|result| result.expect("worker task should not panic"))
            .collect::<Vec<_>>();
        let unique = claimed.iter().copied().collect::<HashSet<_>>();

        assert_eq!(claimed.len(), submitted.len());
        assert_eq!(unique, submitted);
    }

    #[sqlx::test]
    async fn lease_expiry_makes_running_task_reclaimable(pool: PgPool) {
        let engine = Engine::new(pool, Duration::from_millis(50));
        let id = engine
            .submit("recover", json!({}))
            .await
            .expect("submit should succeed");
        let first = engine
            .next_pending(WorkerId::new())
            .await
            .expect("dequeue should query")
            .expect("task should start");

        sleep(Duration::from_millis(80)).await;
        let reclaimed = engine
            .next_pending(WorkerId::new())
            .await
            .expect("dequeue should query")
            .expect("expired task should be reclaimed");

        assert_eq!(first.id, id);
        assert_eq!(reclaimed.id, id);
    }

    #[sqlx::test]
    async fn completed_and_failed_tasks_are_not_reclaimed(pool: PgPool) {
        let engine = Engine::new(pool, Duration::from_millis(20));
        let completed_id = engine
            .submit("done", json!({}))
            .await
            .expect("submit should succeed");
        let failed_id = engine
            .submit("bad", json!({}))
            .await
            .expect("submit should succeed");

        let completed = engine
            .next_pending(WorkerId::new())
            .await
            .expect("dequeue should query")
            .expect("completed task should start");
        engine
            .mark_completed(completed, TaskOutput(json!({ "ok": true })))
            .await
            .expect("complete should succeed");

        let failed = engine
            .next_pending(WorkerId::new())
            .await
            .expect("dequeue should query")
            .expect("failed task should start");
        engine
            .mark_failed(failed, HandlerError::Permanent("bad".into()))
            .await
            .expect("fail should succeed");

        sleep(Duration::from_millis(40)).await;

        assert!(
            engine
                .next_pending(WorkerId::new())
                .await
                .expect("dequeue should query")
                .is_none()
        );
        assert!(matches!(
            engine.get(completed_id).await.expect("task should exist"),
            AnyTask::Completed(_)
        ));
        assert!(matches!(
            engine.get(failed_id).await.expect("task should exist"),
            AnyTask::Failed(_)
        ));
    }

    #[sqlx::test]
    async fn get_returns_status_specific_data(pool: PgPool) {
        let engine = engine(pool);

        let running_id = engine
            .submit("running", json!({}))
            .await
            .expect("submit should succeed");
        let running = engine
            .next_pending(WorkerId::new())
            .await
            .expect("dequeue should query")
            .expect("running should start");
        assert_eq!(running.id, running_id);
        assert!(matches!(
            engine.get(running_id).await.expect("running should exist"),
            AnyTask::Running(_)
        ));

        let completed_id = engine
            .submit("completed", json!({}))
            .await
            .expect("submit should succeed");
        let completed = engine
            .next_pending(WorkerId::new())
            .await
            .expect("dequeue should query")
            .expect("completed should start");
        engine
            .mark_completed(completed, TaskOutput(json!({ "ok": true })))
            .await
            .expect("complete should succeed");
        assert!(matches!(
            engine
                .get(completed_id)
                .await
                .expect("completed should exist"),
            AnyTask::Completed(_)
        ));

        let failed_id = engine
            .submit("failed", json!({}))
            .await
            .expect("submit should succeed");
        let failed = engine
            .next_pending(WorkerId::new())
            .await
            .expect("dequeue should query")
            .expect("failed should start");
        engine
            .mark_failed(failed, HandlerError::Transient("try later".into()))
            .await
            .expect("fail should succeed");
        assert!(matches!(
            engine.get(failed_id).await.expect("failed should exist"),
            AnyTask::Failed(_)
        ));

        let pending_id = engine
            .submit("pending", json!({}))
            .await
            .expect("submit should succeed");
        assert!(matches!(
            engine.get(pending_id).await.expect("pending should exist"),
            AnyTask::Pending(_)
        ));
    }

    #[sqlx::test]
    async fn get_returns_clear_error_for_missing_task(pool: PgPool) {
        let engine = engine(pool);
        let missing = TaskId::new();

        let error = engine
            .get(missing)
            .await
            .expect_err("missing task should fail");

        assert!(matches!(error, EngineError::NotFound(id) if id == missing));
    }
}
