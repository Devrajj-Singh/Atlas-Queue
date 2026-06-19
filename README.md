# atlas-queue

High-performance distributed task queue built in Rust.

Atlas Queue is a production-inspired learning project for building queueing,
worker, scheduling, persistence, and distributed systems fundamentals in Rust.

## Current Phase

Phase 1: Core Task Engine

- Submit typed tasks with JSON payloads.
- Track task lifecycle through typestate: pending, running, completed, failed.
- Pull pending tasks in FIFO order for a worker.
- Complete, fail, or requeue running tasks by consuming `Task<Running>`.
- Register task handlers by task-type name.

## Run

```bash
cargo run
```

## Test

```bash
cargo test
```
