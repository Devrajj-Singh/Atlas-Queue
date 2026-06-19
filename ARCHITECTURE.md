# Atlas Queue Architecture

## Current Architecture

Phase 1 is intentionally single-process and in-memory.

```text
Binary
  |
  v
Engine
  |
  +-- HashMap<TaskId, AnyTask> for task storage
  |
  +-- VecDeque<TaskId> for FIFO pending order
  |
  +-- HandlerRegistry for task-type handler lookup
```

## Design Decisions

- Core logic lives under `src/engine` so future binaries, APIs, examples, and
  integration tests can reuse the same domain model.
- Task state is encoded with typestate: `Task<Pending>`, `Task<Running>`,
  `Task<Completed>`, and `Task<Failed>` are different types.
- Completion, failure, and requeue operations consume `Task<Running>`, which
  makes illegal lifecycle transitions compile-time errors.
- Handlers use a typed `TaskHandler` trait for implementors and an object-safe
  `ErasedHandler` trait for registry storage.
- `VecDeque` is used for FIFO behavior because it provides efficient push-back
  and pop-front operations.
- `HashMap` stores tasks by ID so task lookup and completion stay direct.
- The engine is synchronous in Phase 1. Handler traits are async-ready, but no
  runtime, networking, or worker pool is introduced yet.

## Future Direction

Phase 2 will wrap the core engine in an Axum REST API. At that point the engine
will need shared ownership and synchronization, likely through `Arc<Mutex<_>>`
or an application state abstraction.
