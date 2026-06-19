use std::collections::{HashMap, VecDeque};

use crate::engine::handler::{HandlerError, TaskOutput};
use crate::engine::task::{AnyTask, Running, Task, TaskId, WorkerId};

/// Single-process in-memory task engine.
///
/// Completion, failure, and requeue operations take ownership of
/// `Task<Running>` instead of accepting only a `TaskId`. That by-value API makes
/// it structurally impossible to complete a task unless the caller already
/// holds typed proof that it was running.
#[derive(Debug, Default)]
pub struct Engine {
    /// Stored tasks exclude checked-out in-flight tasks.
    ///
    /// See `Engine::next_pending` for why `Task<Running>` lives only with the
    /// caller until it is completed, failed, or requeued.
    tasks: HashMap<TaskId, AnyTask>,
    pending_order: VecDeque<TaskId>,
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("task {0} not found")]
    NotFound(TaskId),
}

impl Engine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn submit(&mut self, task_type: impl Into<String>, payload: serde_json::Value) -> TaskId {
        let id = TaskId::new();
        let task = Task::new(id, task_type, payload);

        self.tasks.insert(id, AnyTask::Pending(task));
        self.pending_order.push_back(id);

        id
    }

    /// Removes the next pending task from the engine and returns it as running.
    ///
    /// The returned owned `Task<Running>` is the only valid handle to the
    /// in-flight task until it is passed back through `mark_completed`,
    /// `mark_failed`, or `requeue`, so this method intentionally does not
    /// re-insert a running copy into `self.tasks`.
    pub fn next_pending(&mut self, worker_id: WorkerId) -> Option<Task<Running>> {
        let id = self.pending_order.pop_front()?;
        let task = match self.tasks.remove(&id)? {
            AnyTask::Pending(task) => task,
            other => {
                self.tasks.insert(id, other);
                return None;
            }
        };

        Some(task.start(worker_id))
    }

    pub fn mark_completed(&mut self, task: Task<Running>, output: TaskOutput) -> TaskId {
        let id = task.id;

        self.tasks
            .insert(id, AnyTask::Completed(task.complete(output)));

        id
    }

    pub fn mark_failed(&mut self, task: Task<Running>, error: HandlerError) -> TaskId {
        let id = task.id;

        self.tasks.insert(id, AnyTask::Failed(task.fail(error)));

        id
    }

    pub fn requeue(&mut self, task: Task<Running>) -> TaskId {
        let id = task.id;

        self.tasks.insert(id, AnyTask::Pending(task.requeue()));
        self.pending_order.push_back(id);

        id
    }

    pub fn get(&self, id: TaskId) -> Result<&AnyTask, EngineError> {
        self.tasks.get(&id).ok_or(EngineError::NotFound(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn full_happy_path_marks_task_completed() {
        let mut engine = Engine::new();
        let id = engine.submit("email.send", json!({ "to": "a@example.com" }));

        let running = engine
            .next_pending(WorkerId::new())
            .expect("pending task should start");
        let completed_id = engine.mark_completed(running, TaskOutput(json!({ "sent": true })));

        assert_eq!(completed_id, id);
        assert!(matches!(
            engine.get(id).expect("task should exist"),
            AnyTask::Completed(_)
        ));
    }

    #[test]
    fn failure_path_marks_task_failed() {
        let mut engine = Engine::new();
        let id = engine.submit("email.send", json!({ "to": "bad" }));

        let running = engine
            .next_pending(WorkerId::new())
            .expect("pending task should start");
        let failed_id = engine.mark_failed(running, HandlerError::Permanent("bad address".into()));

        assert_eq!(failed_id, id);
        assert!(matches!(
            engine.get(id).expect("task should exist"),
            AnyTask::Failed(_)
        ));
    }

    #[test]
    fn requeue_path_returns_task_to_pending_order() {
        let mut engine = Engine::new();
        let id = engine.submit("reports.generate", json!({ "id": 42 }));

        let running = engine
            .next_pending(WorkerId::new())
            .expect("pending task should start");
        let requeued_id = engine.requeue(running);
        let running_again = engine
            .next_pending(WorkerId::new())
            .expect("requeued task should be pending again");

        assert_eq!(requeued_id, id);
        assert_eq!(running_again.id, id);
    }

    #[test]
    fn next_pending_preserves_fifo_order() {
        let mut engine = Engine::new();
        let first = engine.submit("first", json!({}));
        let second = engine.submit("second", json!({}));
        let third = engine.submit("third", json!({}));

        let first_running = engine
            .next_pending(WorkerId::new())
            .expect("first task should start");
        let second_running = engine
            .next_pending(WorkerId::new())
            .expect("second task should start");
        let third_running = engine
            .next_pending(WorkerId::new())
            .expect("third task should start");

        assert_eq!(first_running.id, first);
        assert_eq!(second_running.id, second);
        assert_eq!(third_running.id, third);
    }

    #[test]
    fn in_flight_task_is_not_visible_via_get() {
        let mut engine = Engine::new();
        let id = engine.submit("email.send", json!({ "to": "a@example.com" }));

        let running = engine
            .next_pending(WorkerId::new())
            .expect("pending task should start");
        let error = engine
            .get(id)
            .expect_err("checked-out task should be absent from storage");

        assert_eq!(running.id, id);
        assert!(matches!(error, EngineError::NotFound(missing) if missing == id));
    }

    #[test]
    fn get_returns_clear_error_for_missing_task() {
        let engine = Engine::new();
        let missing = TaskId::new();

        let error = engine.get(missing).expect_err("missing task should fail");

        assert!(matches!(error, EngineError::NotFound(id) if id == missing));
    }
}
