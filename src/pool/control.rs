use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};

use crate::engine::core::EngineError;
use crate::engine::handler::{HandlerError, TaskOutput};
use crate::engine::task::{AnyTask, TaskId, WorkerId};

pub enum ControlRequest {
    Submit {
        task_type: String,
        payload: serde_json::Value,
        respond_to: oneshot::Sender<TaskId>,
    },
    GetStatus {
        id: TaskId,
        respond_to: oneshot::Sender<Result<TaskSnapshot, EngineError>>,
    },
}

#[derive(Debug, thiserror::Error)]
#[error("dispatcher is unavailable")]
pub struct DispatcherUnavailable;

#[derive(Debug, thiserror::Error)]
pub enum GetStatusError {
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error(transparent)]
    DispatcherUnavailable(#[from] DispatcherUnavailable),
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TaskSnapshot {
    Pending {
        id: TaskId,
        created_at: DateTime<Utc>,
    },
    Running {
        id: TaskId,
        worker_id: WorkerId,
        started_at: DateTime<Utc>,
    },
    Completed {
        id: TaskId,
        output: TaskOutput,
        finished_at: DateTime<Utc>,
    },
    Failed {
        id: TaskId,
        error: HandlerError,
        failed_at: DateTime<Utc>,
    },
}

impl From<&AnyTask> for TaskSnapshot {
    fn from(task: &AnyTask) -> Self {
        match task {
            AnyTask::Pending(task) => Self::Pending {
                id: task.id,
                created_at: task.state.created_at,
            },
            AnyTask::Running(task) => Self::Running {
                id: task.id,
                worker_id: task.state.worker_id,
                started_at: task.state.started_at,
            },
            AnyTask::Completed(task) => Self::Completed {
                id: task.id,
                output: task.state.output.clone(),
                finished_at: task.state.finished_at,
            },
            AnyTask::Failed(task) => Self::Failed {
                id: task.id,
                error: task.state.error.clone(),
                failed_at: task.state.failed_at,
            },
        }
    }
}

#[derive(Clone)]
pub struct DispatcherHandle {
    control_tx: mpsc::Sender<ControlRequest>,
}

impl DispatcherHandle {
    pub fn new(control_tx: mpsc::Sender<ControlRequest>) -> Self {
        Self { control_tx }
    }

    pub async fn submit(
        &self,
        task_type: String,
        payload: serde_json::Value,
    ) -> Result<TaskId, DispatcherUnavailable> {
        let (respond_to, response_rx) = oneshot::channel();
        self.control_tx
            .send(ControlRequest::Submit {
                task_type,
                payload,
                respond_to,
            })
            .await
            .map_err(|_| DispatcherUnavailable)?;

        response_rx.await.map_err(|_| DispatcherUnavailable)
    }

    pub async fn get_status(&self, id: TaskId) -> Result<TaskSnapshot, GetStatusError> {
        let (respond_to, response_rx) = oneshot::channel();
        self.control_tx
            .send(ControlRequest::GetStatus { id, respond_to })
            .await
            .map_err(|_| DispatcherUnavailable)?;

        Ok(response_rx.await.map_err(|_| DispatcherUnavailable)??)
    }
}
