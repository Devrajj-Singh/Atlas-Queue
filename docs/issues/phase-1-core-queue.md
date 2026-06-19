# Phase 1: Core task engine

## Objective

Build the first runnable Atlas Queue milestone: a single-process in-memory task
engine.

## Scope

- Define core typestate task domain types.
- Add submit, next pending, complete, fail, requeue, get, and handler registry
  behavior.
- Keep the implementation synchronous and dependency-light.
- Add focused Rust unit tests.
- Update project docs for Phase 1.

## Acceptance Criteria

- `cargo test` passes.
- Queue pops tasks in FIFO order.
- Running tasks can be completed, failed, or requeued by consuming
  `Task<Running>`.
- Missing task lookup returns a clear error.
- The code is organized so future API and worker phases can reuse the core queue logic.

## Out of Scope

- REST API.
- Tokio worker pool.
- Persistence.
- Retries.
- Priority scheduling.
