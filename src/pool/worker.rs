use std::panic::AssertUnwindSafe;

use futures::FutureExt;
use tokio::sync::mpsc;

use crate::engine::handler::HandlerError;
use crate::engine::task::WorkerId;
use crate::pool::{TaskResult, WorkItem};

pub struct Worker {
    id: WorkerId,
}

impl Worker {
    pub fn new(id: WorkerId) -> Self {
        Self { id }
    }

    pub async fn run(
        self,
        mut work_rx: mpsc::Receiver<WorkItem>,
        result_tx: mpsc::Sender<TaskResult>,
    ) {
        while let Some(item) = work_rx.recv().await {
            let WorkItem { task, registry } = item;
            let task_type = task.task_type.clone();
            let Some(handler) = registry.get(&task_type) else {
                let error = HandlerError::Permanent(format!(
                    "no handler registered for task type {task_type}"
                ));
                if result_tx
                    .send(TaskResult::Failed { task, error })
                    .await
                    .is_err()
                {
                    break;
                }
                continue;
            };

            let payload = task.payload.clone();
            // The wrapped future captures only the handler call and payload.
            // If it panics, only handler-owned state can be inconsistent; the
            // worker and dispatcher state remain outside this unwind boundary.
            let execution = AssertUnwindSafe(handler.execute_erased(payload)).catch_unwind();

            let result = match execution.await {
                Ok(Ok(output)) => TaskResult::Completed { task, output },
                Ok(Err(error)) => TaskResult::Failed { task, error },
                Err(_) => TaskResult::Failed {
                    task,
                    error: HandlerError::Permanent(format!(
                        "handler panicked while worker {} was executing task type {task_type}",
                        self.id
                    )),
                },
            };

            if result_tx.send(result).await.is_err() {
                break;
            }
        }
    }
}
