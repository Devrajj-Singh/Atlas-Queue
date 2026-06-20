use serde::{Deserialize, Serialize};

use crate::engine::task::TaskId;

#[derive(Debug, Deserialize)]
pub struct SubmitTaskRequest {
    pub task_type: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct SubmitTaskResponse {
    pub id: TaskId,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}
