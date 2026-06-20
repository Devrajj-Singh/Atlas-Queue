//! Concurrent worker pool for Atlas Queue.
//!
//! A single dispatcher task drives `Engine` calls in this phase, while the
//! engine itself is backed by shareable Postgres state. Workers communicate with the dispatcher over bounded
//! channels: work channels provide backpressure, and result channels hand the
//! owned `Task<Running>` back so completion and failure preserve Phase 1's
//! typestate proof. Handler panics are isolated per task execution, so the
//! long-lived worker loop can continue.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::engine::core::Engine;
use crate::engine::handler::{HandlerError, TaskOutput};
use crate::engine::registry::HandlerRegistry;
use crate::engine::task::{Running, Task, WorkerId};
use crate::pool::control::{ControlRequest, DispatcherHandle};
use crate::pool::dispatcher::Dispatcher;
use crate::pool::worker::Worker;

pub mod control;
pub mod dispatcher;
pub mod worker;

/// Unit of work sent from the dispatcher to exactly one worker.
///
/// The work item carries the owned `Task<Running>` plus a registry handle so
/// workers can execute without ever touching `Engine`.
pub struct WorkItem {
    pub task: Task<Running>,
    pub registry: Arc<HandlerRegistry>,
}

pub enum TaskResult {
    Completed {
        task: Task<Running>,
        output: TaskOutput,
    },
    Failed {
        task: Task<Running>,
        error: HandlerError,
    },
}

pub struct WorkerPoolConfig {
    pub worker_count: usize,
    pub channel_capacity: usize,
    pub control_channel_capacity: usize,
}

/// Fixed-size worker pool.
///
/// Workers are tracked by `WorkerId` rather than hidden in an unindexed list so
/// future dynamic resizing can add or remove a specific worker without
/// replacing the pool's bookkeeping model.
pub struct WorkerPool {
    shutdown_tx: watch::Sender<bool>,
    handle: DispatcherHandle,
    dispatcher: JoinHandle<()>,
    workers: HashMap<WorkerId, JoinHandle<()>>,
}

impl WorkerPool {
    pub fn spawn(engine: Engine, registry: HandlerRegistry, config: WorkerPoolConfig) -> Self {
        let registry = Arc::new(registry);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        // A worker can only finish one task at a time, so worker_count is the
        // maximum burst of simultaneous result sends the dispatcher must absorb.
        let result_capacity = config.worker_count.max(1);
        let (result_tx, result_rx) = mpsc::channel::<TaskResult>(result_capacity);
        let control_capacity = config.control_channel_capacity.max(1);
        let (control_tx, control_rx) = mpsc::channel::<ControlRequest>(control_capacity);
        let handle = DispatcherHandle::new(control_tx);

        let mut work_txs = Vec::with_capacity(config.worker_count);
        let mut workers = HashMap::with_capacity(config.worker_count);
        for _ in 0..config.worker_count {
            let worker_id = WorkerId::new();
            let (work_tx, work_rx) = mpsc::channel::<WorkItem>(config.channel_capacity);
            let worker = Worker::new(worker_id);
            let result_tx = result_tx.clone();

            work_txs.push(work_tx);
            workers.insert(worker_id, tokio::spawn(worker.run(work_rx, result_tx)));
        }
        drop(result_tx);

        let dispatcher =
            Dispatcher::new(engine, registry).run(work_txs, result_rx, control_rx, shutdown_rx);
        let dispatcher = tokio::spawn(dispatcher);

        Self {
            shutdown_tx,
            handle,
            dispatcher,
            workers,
        }
    }

    pub fn handle(&self) -> DispatcherHandle {
        self.handle.clone()
    }

    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        self.dispatcher
            .await
            .expect("dispatcher task should not panic");

        for (_worker_id, worker) in self.workers {
            worker.await.expect("worker task should not panic");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::{Value, json};
    use sqlx::PgPool;
    use tokio::time::{Duration, sleep, timeout};

    use super::*;
    use crate::engine::handler::{HandlerError, TaskHandler, TaskOutput};
    use crate::engine::task::AnyTask;

    struct EchoHandler {
        completed: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl TaskHandler for EchoHandler {
        type Payload = Value;

        async fn execute(&self, payload: Self::Payload) -> Result<TaskOutput, HandlerError> {
            self.completed.fetch_add(1, Ordering::SeqCst);
            Ok(TaskOutput(payload))
        }
    }

    struct PanicOnPayloadHandler {
        completed: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl TaskHandler for PanicOnPayloadHandler {
        type Payload = Value;

        async fn execute(&self, payload: Self::Payload) -> Result<TaskOutput, HandlerError> {
            if payload.get("panic").and_then(Value::as_bool) == Some(true) {
                panic!("intentional handler panic");
            }

            self.completed.fetch_add(1, Ordering::SeqCst);
            Ok(TaskOutput(payload))
        }
    }

    struct SlowEchoHandler {
        completed: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl TaskHandler for SlowEchoHandler {
        type Payload = Value;

        async fn execute(&self, payload: Self::Payload) -> Result<TaskOutput, HandlerError> {
            sleep(Duration::from_millis(20)).await;
            self.completed.fetch_add(1, Ordering::SeqCst);
            Ok(TaskOutput(payload))
        }
    }

    async fn wait_for_count(counter: &AtomicUsize, expected: usize) {
        timeout(Duration::from_secs(2), async {
            while counter.load(Ordering::SeqCst) < expected {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("expected worker pool to process tasks before timeout");
    }

    fn engine(pool: PgPool) -> Engine {
        Engine::new(pool, Duration::from_secs(30))
    }

    #[sqlx::test]
    async fn happy_path_under_concurrency_completes_all_tasks(pool: PgPool) {
        let completed = Arc::new(AtomicUsize::new(0));
        let mut registry = HandlerRegistry::new();
        registry.register(
            "echo",
            EchoHandler {
                completed: Arc::clone(&completed),
            },
        );
        let engine = engine(pool);
        let ids = futures::future::join_all(
            (0..6).map(|index| engine.submit("echo", json!({ "index": index }))),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("submits should succeed");

        let pool = WorkerPool::spawn(
            engine.clone(),
            registry,
            WorkerPoolConfig {
                worker_count: 2,
                channel_capacity: 2,
                control_channel_capacity: 8,
            },
        );

        wait_for_count(&completed, ids.len()).await;
        pool.shutdown().await;

        for id in ids {
            let AnyTask::Completed(task) = engine.get(id).await.expect("task should be stored")
            else {
                panic!("task should be completed");
            };
            assert_eq!(task.payload, task.state.output.0);
        }
    }

    #[sqlx::test]
    async fn panic_isolation_fails_one_task_and_worker_continues(pool: PgPool) {
        let completed = Arc::new(AtomicUsize::new(0));
        let mut registry = HandlerRegistry::new();
        registry.register(
            "maybe-panic",
            PanicOnPayloadHandler {
                completed: Arc::clone(&completed),
            },
        );
        let engine = engine(pool);
        let panic_id = engine
            .submit("maybe-panic", json!({ "panic": true }))
            .await
            .expect("submit should succeed");
        let normal_id = engine
            .submit("maybe-panic", json!({ "panic": false }))
            .await
            .expect("submit should succeed");

        let pool = WorkerPool::spawn(
            engine.clone(),
            registry,
            WorkerPoolConfig {
                worker_count: 1,
                channel_capacity: 1,
                control_channel_capacity: 8,
            },
        );

        wait_for_count(&completed, 1).await;
        sleep(Duration::from_millis(50)).await;
        pool.shutdown().await;

        let AnyTask::Failed(task) = engine
            .get(panic_id)
            .await
            .expect("panic task should be stored")
        else {
            panic!("panic task should fail");
        };
        assert!(
            matches!(&task.state.error, HandlerError::Permanent(message) if message.contains("panicked"))
        );
        assert!(matches!(
            engine
                .get(normal_id)
                .await
                .expect("normal task should be stored"),
            AnyTask::Completed(_)
        ));
    }

    #[sqlx::test]
    async fn missing_handler_marks_task_failed(pool: PgPool) {
        let registry = HandlerRegistry::new();
        let engine = engine(pool);
        let id = engine
            .submit("missing", json!({}))
            .await
            .expect("submit should succeed");

        let pool = WorkerPool::spawn(
            engine.clone(),
            registry,
            WorkerPoolConfig {
                worker_count: 1,
                channel_capacity: 1,
                control_channel_capacity: 8,
            },
        );

        sleep(Duration::from_millis(50)).await;
        pool.shutdown().await;

        let AnyTask::Failed(task) = engine.get(id).await.expect("task should be stored") else {
            panic!("missing handler task should fail");
        };
        assert!(
            matches!(&task.state.error, HandlerError::Permanent(message) if message.contains("no handler registered"))
        );
    }

    #[sqlx::test]
    async fn backpressure_with_small_capacity_still_drains(pool: PgPool) {
        let completed = Arc::new(AtomicUsize::new(0));
        let mut registry = HandlerRegistry::new();
        registry.register(
            "echo",
            EchoHandler {
                completed: Arc::clone(&completed),
            },
        );
        let engine = engine(pool);
        let ids = futures::future::join_all(
            (0..8).map(|index| engine.submit("echo", json!({ "index": index }))),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("submits should succeed");

        let pool = WorkerPool::spawn(
            engine.clone(),
            registry,
            WorkerPoolConfig {
                worker_count: 2,
                channel_capacity: 1,
                control_channel_capacity: 8,
            },
        );

        wait_for_count(&completed, ids.len()).await;
        pool.shutdown().await;

        for id in ids {
            assert!(matches!(
                engine.get(id).await.expect("task should be stored"),
                AnyTask::Completed(_)
            ));
        }
    }

    #[sqlx::test]
    async fn dispatcher_drains_results_while_waiting_for_backpressured_worker(pool: PgPool) {
        timeout(Duration::from_secs(2), async {
            let completed = Arc::new(AtomicUsize::new(0));
            let mut registry = HandlerRegistry::new();
            registry.register(
                "slow-echo",
                SlowEchoHandler {
                    completed: Arc::clone(&completed),
                },
            );
            let engine = engine(pool);
            let ids = futures::future::join_all(
                (0..5).map(|index| engine.submit("slow-echo", json!({ "index": index }))),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("submits should succeed");

            let pool = WorkerPool::spawn(
                engine.clone(),
                registry,
                WorkerPoolConfig {
                    worker_count: 1,
                    channel_capacity: 1,
                    control_channel_capacity: 8,
                },
            );

            wait_for_count(&completed, ids.len()).await;
            pool.shutdown().await;

            for id in ids {
                assert!(matches!(
                    engine.get(id).await.expect("task should be stored"),
                    AnyTask::Completed(_)
                ));
            }
        })
        .await
        .expect("worker pool deadlocked while dispatcher was waiting on a full work channel");
    }

    #[sqlx::test]
    async fn graceful_shutdown_with_no_pending_work_returns_promptly(pool: PgPool) {
        let pool = WorkerPool::spawn(
            engine(pool),
            HandlerRegistry::new(),
            WorkerPoolConfig {
                worker_count: 2,
                channel_capacity: 1,
                control_channel_capacity: 8,
            },
        );

        timeout(Duration::from_secs(1), pool.shutdown())
            .await
            .expect("shutdown should return promptly with no pending work");
    }
}
