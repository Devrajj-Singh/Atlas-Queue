use std::collections::{HashMap, VecDeque};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(u64);

impl TaskId {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn value(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    InProgress,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    pub id: TaskId,
    pub payload: String,
    pub status: TaskStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueError {
    TaskNotFound(TaskId),
}

impl fmt::Display for QueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TaskNotFound(id) => write!(formatter, "task {} was not found", id.value()),
        }
    }
}

impl std::error::Error for QueueError {}

#[derive(Debug, Default)]
pub struct TaskQueue {
    next_id: u64,
    tasks: HashMap<TaskId, Task>,
    pending: VecDeque<TaskId>,
}

impl TaskQueue {
    pub fn new() -> Self {
        Self {
            next_id: 1,
            tasks: HashMap::new(),
            pending: VecDeque::new(),
        }
    }

    pub fn push(&mut self, payload: impl Into<String>) -> Task {
        let id = TaskId::new(self.next_id);
        self.next_id += 1;

        let task = Task {
            id,
            payload: payload.into(),
            status: TaskStatus::Pending,
        };

        self.tasks.insert(id, task.clone());
        self.pending.push_back(id);
        task
    }

    pub fn pop(&mut self) -> Option<Task> {
        let id = self.pending.pop_front()?;
        let task = self.tasks.get_mut(&id)?;

        task.status = TaskStatus::InProgress;
        Some(task.clone())
    }

    pub fn complete(&mut self, id: TaskId) -> Result<Task, QueueError> {
        let task = self
            .tasks
            .get_mut(&id)
            .ok_or(QueueError::TaskNotFound(id))?;

        task.status = TaskStatus::Completed;
        Ok(task.clone())
    }

    pub fn get(&self, id: TaskId) -> Option<&Task> {
        self.tasks.get(&id)
    }

    pub fn list(&self) -> Vec<Task> {
        let mut tasks = self.tasks.values().cloned().collect::<Vec<_>>();
        tasks.sort_by_key(|task| task.id.value());
        tasks
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_queue_starts_empty() {
        let queue = TaskQueue::new();

        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
        assert!(queue.list().is_empty());
    }

    #[test]
    fn push_creates_pending_task() {
        let mut queue = TaskQueue::new();

        let task = queue.push("send welcome email");

        assert_eq!(task.id.value(), 1);
        assert_eq!(task.payload, "send welcome email");
        assert_eq!(task.status, TaskStatus::Pending);
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn pop_returns_tasks_in_fifo_order() {
        let mut queue = TaskQueue::new();
        let first = queue.push("first");
        let second = queue.push("second");

        let popped_first = queue.pop().expect("expected first task");
        let popped_second = queue.pop().expect("expected second task");

        assert_eq!(popped_first.id, first.id);
        assert_eq!(popped_second.id, second.id);
        assert_eq!(popped_first.status, TaskStatus::InProgress);
        assert_eq!(popped_second.status, TaskStatus::InProgress);
    }

    #[test]
    fn pop_returns_none_when_queue_has_no_pending_tasks() {
        let mut queue = TaskQueue::new();

        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn complete_marks_task_completed() {
        let mut queue = TaskQueue::new();
        let task = queue.push("generate report");

        queue.pop();
        let completed = queue.complete(task.id).expect("task should exist");

        assert_eq!(completed.status, TaskStatus::Completed);
        assert_eq!(
            queue
                .get(task.id)
                .expect("task should still be stored")
                .status,
            TaskStatus::Completed
        );
    }

    #[test]
    fn complete_returns_error_for_missing_task() {
        let mut queue = TaskQueue::new();

        let error = queue
            .complete(TaskId::new(404))
            .expect_err("missing task should fail");

        assert_eq!(error, QueueError::TaskNotFound(TaskId::new(404)));
    }

    #[test]
    fn list_returns_tasks_in_id_order() {
        let mut queue = TaskQueue::new();
        let first = queue.push("first");
        let second = queue.push("second");

        let tasks = queue.list();

        assert_eq!(
            tasks.iter().map(|task| task.id).collect::<Vec<_>>(),
            vec![first.id, second.id]
        );
    }
}
