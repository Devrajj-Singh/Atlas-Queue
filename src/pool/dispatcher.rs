use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio::time::{self, Duration};

use crate::engine::core::Engine;
use crate::engine::handler::HandlerError;
use crate::engine::registry::HandlerRegistry;
use crate::engine::task::WorkerId;
use crate::pool::control::{ControlRequest, TaskSnapshot};
use crate::pool::{TaskResult, WorkItem};

/// Dispatches work to workers, applies worker results, and
/// serves submit/status control requests from external callers.
pub struct Dispatcher {
    engine: Arc<Engine>,
    registry: Arc<HandlerRegistry>,
    next_worker_index: usize,
    in_flight: usize,
}

impl Dispatcher {
    pub fn new(engine: Engine, registry: Arc<HandlerRegistry>) -> Self {
        Self {
            engine: Arc::new(engine),
            registry,
            next_worker_index: 0,
            in_flight: 0,
        }
    }

    pub async fn run(
        mut self,
        work_txs: Vec<mpsc::Sender<WorkItem>>,
        mut result_rx: mpsc::Receiver<TaskResult>,
        mut control_rx: mpsc::Receiver<ControlRequest>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut pending_work = None;

        loop {
            while let Ok(result) = result_rx.try_recv() {
                self.apply_result(result).await;
            }

            if *shutdown.borrow() && pending_work.is_none() {
                while self.in_flight > 0 {
                    match result_rx.recv().await {
                        Some(result) => self.apply_result(result).await,
                        None => break,
                    }
                }

                return;
            }

            if work_txs.is_empty() {
                self.wait_for_work_or_shutdown(&mut shutdown, &mut result_rx, &mut control_rx)
                    .await;
                continue;
            }

            if pending_work.is_none() {
                match self.engine.next_pending(WorkerId::new()).await {
                    Ok(Some(task)) => {
                        let task_id = task.id;
                        let item = WorkItem {
                            task,
                            registry: Arc::clone(&self.registry),
                        };
                        let worker_index = self.next_worker_index % work_txs.len();
                        self.next_worker_index = (self.next_worker_index + 1) % work_txs.len();

                        pending_work = Some((worker_index, task_id, item));
                    }
                    Ok(None) => {
                        self.wait_for_work_or_shutdown(
                            &mut shutdown,
                            &mut result_rx,
                            &mut control_rx,
                        )
                        .await;
                        continue;
                    }
                    Err(error) => {
                        tracing::error!(%error, "failed to dequeue pending task");
                        self.wait_for_work_or_shutdown(
                            &mut shutdown,
                            &mut result_rx,
                            &mut control_rx,
                        )
                        .await;
                        continue;
                    }
                }
            }

            let Some((worker_index, task_id, item)) = pending_work.take() else {
                continue;
            };

            // Keep control requests ahead of work reservation when both are
            // ready so HTTP submissions/status checks stay responsive even
            // while a previously selected task is waiting for worker capacity.
            let permit = tokio::select! {
                biased;
                request = control_rx.recv() => {
                    if let Some(request) = request {
                        self.handle_control(request).await;
                    }
                    None
                }
                permit = work_txs[worker_index].reserve() => Some(permit),
                result = result_rx.recv(), if self.in_flight > 0 => {
                    if let Some(result) = result {
                        self.apply_result(result).await;
                    }
                    None
                }
                _ = shutdown.changed() => {
                    None
                }
            };

            match permit {
                Some(Ok(permit)) => {
                    permit.send(item);
                    self.in_flight += 1;
                }
                Some(Err(_)) => {
                    let error = HandlerError::Permanent(format!(
                        "worker channel closed before task {task_id} could be dispatched",
                    ));
                    if let Err(error) = self.engine.mark_failed(item.task, error).await {
                        tracing::error!(%error, %task_id, "failed to mark undispatched task failed");
                    }
                }
                None => {
                    pending_work = Some((worker_index, task_id, item));
                }
            }
        }
    }

    async fn handle_control(&mut self, request: ControlRequest) {
        match request {
            ControlRequest::Submit {
                task_type,
                payload,
                respond_to,
            } => {
                let result = self.engine.submit(task_type, payload).await;
                let _ = respond_to.send(result);
            }
            ControlRequest::GetStatus { id, respond_to } => {
                let status = self
                    .engine
                    .get(id)
                    .await
                    .map(|task| TaskSnapshot::from(&task));
                let _ = respond_to.send(status);
            }
        }
    }

    async fn apply_result(&mut self, result: TaskResult) {
        self.in_flight = self.in_flight.saturating_sub(1);

        match result {
            TaskResult::Completed { task, output } => {
                let task_id = task.id;
                if let Err(error) = self.engine.mark_completed(task, output).await {
                    tracing::error!(%error, %task_id, "failed to mark task completed");
                }
            }
            TaskResult::Failed { task, error } => {
                let task_id = task.id;
                if let Err(error) = self.engine.mark_failed(task, error).await {
                    tracing::error!(%error, %task_id, "failed to mark task failed");
                }
            }
        }
    }

    async fn wait_for_work_or_shutdown(
        &mut self,
        shutdown: &mut watch::Receiver<bool>,
        result_rx: &mut mpsc::Receiver<TaskResult>,
        control_rx: &mut mpsc::Receiver<ControlRequest>,
    ) {
        tokio::select! {
            request = control_rx.recv() => {
                if let Some(request) = request {
                    self.handle_control(request).await;
                }
            }
            result = result_rx.recv(), if self.in_flight > 0 => {
                if let Some(result) = result {
                    self.apply_result(result).await;
                }
            }
            _ = shutdown.changed() => {}
            _ = time::sleep(Duration::from_millis(10)) => {}
        }
    }
}
