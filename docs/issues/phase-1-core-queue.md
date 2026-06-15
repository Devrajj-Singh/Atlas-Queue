# Phase 1: Core in-memory task queue

## Objective

Build the first runnable Atlas Queue milestone: a single-process in-memory task queue.

## Scope

- Define core task domain types.
- Add push, pop, complete, get, and list behavior.
- Keep the implementation synchronous and dependency-light.
- Add focused Rust unit tests.
- Update project docs for Phase 1.

## Acceptance Criteria

- `cargo test` passes.
- Queue pops tasks in FIFO order.
- Tasks can be completed by ID.
- Missing task completion returns a clear error.
- The code is organized so future API and worker phases can reuse the core queue logic.

## Out of Scope

- REST API.
- Tokio worker pool.
- Persistence.
- Retries.
- Priority scheduling.
