# Atlas Queue Architecture

## Current Architecture

Phase 1 is intentionally single-process and dependency-light.

```text
Binary
  |
  v
TaskQueue
  |
  +-- HashMap<TaskId, Task> for task storage
  |
  +-- VecDeque<TaskId> for FIFO pending order
```

## Design Decisions

- `TaskQueue` lives in `src/lib.rs` so future binaries, APIs, examples, and
  integration tests can reuse the same domain logic.
- `VecDeque` is used for FIFO behavior because it provides efficient push-back
  and pop-front operations.
- `HashMap` stores tasks by ID so task lookup and completion stay direct.
- The queue is synchronous in Phase 1. Async will be introduced when networking
  and worker concurrency create a real need for it.

## Future Direction

Phase 2 will wrap the core queue in an Axum REST API. At that point the queue
will need shared ownership and synchronization, likely through `Arc<Mutex<_>>`
or an application state abstraction.
