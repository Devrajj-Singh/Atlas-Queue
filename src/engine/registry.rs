use std::collections::HashMap;
use std::sync::Arc;

use crate::engine::handler::{ErasedHandler, TaskHandler};

#[derive(Default)]
pub struct HandlerRegistry {
    handlers: HashMap<String, Arc<dyn ErasedHandler>>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<H>(&mut self, task_type: impl Into<String>, handler: H)
    where
        H: TaskHandler + Send + Sync + 'static,
    {
        self.handlers.insert(task_type.into(), Arc::new(handler));
    }

    pub fn get(&self, task_type: &str) -> Option<Arc<dyn ErasedHandler>> {
        self.handlers.get(task_type).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::handler::{HandlerError, TaskOutput};

    struct DummyHandler;

    #[async_trait::async_trait]
    impl TaskHandler for DummyHandler {
        type Payload = serde_json::Value;

        async fn execute(&self, payload: Self::Payload) -> Result<TaskOutput, HandlerError> {
            Ok(TaskOutput(payload))
        }
    }

    #[test]
    fn register_and_retrieve_handler_by_task_type() {
        let mut registry = HandlerRegistry::new();

        registry.register("dummy", DummyHandler);

        assert!(registry.get("dummy").is_some());
        assert!(registry.get("missing").is_none());
    }
}
