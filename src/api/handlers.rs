use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::api::dto::{ErrorResponse, SubmitTaskRequest, SubmitTaskResponse};
use crate::api::routes::AppState;
use crate::engine::core::EngineError;
use crate::engine::task::TaskId;
use crate::pool::control::{DispatcherUnavailable, GetStatusError, TaskSnapshot};

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("malformed task id")]
    BadTaskId,
    #[error("task not found")]
    TaskNotFound,
    #[error("dispatcher is unavailable")]
    DispatcherUnavailable,
}

impl From<DispatcherUnavailable> for ApiError {
    fn from(_: DispatcherUnavailable) -> Self {
        Self::DispatcherUnavailable
    }
}

impl From<EngineError> for ApiError {
    fn from(error: EngineError) -> Self {
        match error {
            // In Phase 3, `Engine` does not retain running tasks, so in-flight
            // and genuinely unknown ids intentionally share the same 404.
            EngineError::NotFound(_) => Self::TaskNotFound,
        }
    }
}

impl From<GetStatusError> for ApiError {
    fn from(error: GetStatusError) -> Self {
        match error {
            GetStatusError::Engine(error) => error.into(),
            GetStatusError::DispatcherUnavailable(error) => error.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::BadTaskId => StatusCode::BAD_REQUEST,
            Self::TaskNotFound => StatusCode::NOT_FOUND,
            Self::DispatcherUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        };

        let body = Json(ErrorResponse {
            error: self.to_string(),
        });

        (status, body).into_response()
    }
}

pub async fn submit_task(
    State(state): State<AppState>,
    Json(request): Json<SubmitTaskRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let id = state
        .dispatcher
        .submit(request.task_type, request.payload)
        .await?;

    Ok((StatusCode::ACCEPTED, Json(SubmitTaskResponse { id })))
}

pub async fn get_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<TaskSnapshot>, ApiError> {
    let id = id.parse::<TaskId>().map_err(|_| ApiError::BadTaskId)?;
    let snapshot = state.dispatcher.get_status(id).await?;

    Ok(Json(snapshot))
}
