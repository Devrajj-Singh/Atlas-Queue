use serde::de::DeserializeOwned;

#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    #[error("invalid payload: {0}")]
    InvalidPayload(String),
    #[error("handler execution failed: {0}")]
    ExecutionFailed(#[from] anyhow::Error),
    #[error("transient failure, safe to retry: {0}")]
    Transient(String),
    #[error("permanent failure, do not retry: {0}")]
    Permanent(String),
}

#[derive(Debug)]
pub struct TaskOutput(pub serde_json::Value);

/// Type-safe handler API for implementors.
///
/// `TaskHandler` lets each handler declare the concrete payload it expects,
/// while `ErasedHandler` below is the object-safe trait stored as `dyn` in the
/// registry because associated payload types are not suitable for heterogeneous
/// runtime storage.
#[async_trait::async_trait]
pub trait TaskHandler: Send + Sync {
    type Payload: DeserializeOwned + Send;

    async fn execute(&self, payload: Self::Payload) -> Result<TaskOutput, HandlerError>;
}

/// Dyn-compatible handler API used by the registry.
///
/// This trait erases each handler's concrete payload type at the storage
/// boundary. The blanket impl deserializes JSON into the typed payload, then
/// calls the implementor's `TaskHandler::execute`.
#[async_trait::async_trait]
pub trait ErasedHandler: Send + Sync {
    async fn execute_erased(&self, payload: serde_json::Value) -> Result<TaskOutput, HandlerError>;
}

#[async_trait::async_trait]
impl<T> ErasedHandler for T
where
    T: TaskHandler + Send + Sync,
{
    async fn execute_erased(&self, payload: serde_json::Value) -> Result<TaskOutput, HandlerError> {
        let payload = serde_json::from_value::<T::Payload>(payload)
            .map_err(|error| HandlerError::InvalidPayload(error.to_string()))?;

        self.execute(payload).await
    }
}
