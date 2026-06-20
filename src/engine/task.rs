use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::engine::handler::{HandlerError, TaskOutput};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct TaskId(Uuid);

impl TaskId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    pub fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl FromStr for TaskId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct WorkerId(Uuid);

impl WorkerId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    pub fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for WorkerId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[derive(Debug)]
pub struct Pending {
    pub created_at: DateTime<Utc>,
}

#[derive(Debug)]
pub struct Running {
    pub started_at: DateTime<Utc>,
    pub worker_id: WorkerId,
    pub locked_until: DateTime<Utc>,
}

#[derive(Debug)]
pub struct Completed {
    pub finished_at: DateTime<Utc>,
    pub output: TaskOutput,
}

#[derive(Debug)]
pub struct Failed {
    pub failed_at: DateTime<Utc>,
    pub error: HandlerError,
}

#[derive(Debug)]
pub struct Task<S> {
    pub id: TaskId,
    pub task_type: String,
    pub payload: serde_json::Value,
    pub state: S,
}

impl Task<Pending> {
    pub fn new(id: TaskId, task_type: impl Into<String>, payload: serde_json::Value) -> Self {
        Self {
            id,
            task_type: task_type.into(),
            payload,
            state: Pending {
                created_at: Utc::now(),
            },
        }
    }

    pub fn start(self, worker_id: WorkerId) -> Task<Running> {
        Task {
            id: self.id,
            task_type: self.task_type,
            payload: self.payload,
            state: Running {
                started_at: Utc::now(),
                worker_id,
                locked_until: Utc::now(),
            },
        }
    }
}

impl Task<Running> {
    pub fn complete(self, output: TaskOutput) -> Task<Completed> {
        Task {
            id: self.id,
            task_type: self.task_type,
            payload: self.payload,
            state: Completed {
                finished_at: Utc::now(),
                output,
            },
        }
    }

    pub fn fail(self, error: HandlerError) -> Task<Failed> {
        Task {
            id: self.id,
            task_type: self.task_type,
            payload: self.payload,
            state: Failed {
                failed_at: Utc::now(),
                error,
            },
        }
    }

    pub fn requeue(self) -> Task<Pending> {
        Task {
            id: self.id,
            task_type: self.task_type,
            payload: self.payload,
            state: Pending {
                created_at: Utc::now(),
            },
        }
    }
}

#[derive(Debug)]
pub enum AnyTask {
    Pending(Task<Pending>),
    Running(Task<Running>),
    Completed(Task<Completed>),
    Failed(Task<Failed>),
}

impl AnyTask {
    pub fn id(&self) -> TaskId {
        match self {
            Self::Pending(task) => task.id,
            Self::Running(task) => task.id,
            Self::Completed(task) => task.id,
            Self::Failed(task) => task.id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn pending_starts_and_running_completes() {
        let task = Task::new(
            TaskId::new(),
            "email.send",
            json!({ "to": "a@example.com" }),
        );

        // COMPILE ERROR: Task<Pending> has no complete method; only Task<Running> can complete.
        // let completed = task.complete(TaskOutput(json!({ "ok": true })));

        let running = task.start(WorkerId::new());
        let completed = running.complete(TaskOutput(json!({ "ok": true })));

        assert_eq!(completed.task_type, "email.send");
    }

    #[test]
    fn running_task_can_be_requeued_to_pending() {
        let task = Task::new(TaskId::new(), "reports.generate", json!({ "id": 42 }));
        let task_id = task.id;

        let pending = task.start(WorkerId::new()).requeue();

        assert_eq!(pending.id, task_id);
    }
}
