# Atlas Queue Roadmap

## Phase 0: Rust Fundamentals

Status: Complete

Focus areas:

- Variables
- Structs
- Enums
- Ownership
- Borrowing
- `Result`
- `Option`

## Phase 1: Core Queue

Status: Complete

Deliverables:

- Typestate task domain model
- In-memory FIFO engine
- Submit, next pending, complete, fail, and requeue operations
- Dyn-compatible handler registry
- Unit tests

## Phase 2: REST API

Status: Planned

Deliverables:

- Axum API
- `POST /tasks`
- `GET /tasks`
- `GET /tasks/{id}`

## Phase 3: Worker Pool

Status: Planned

Deliverables:

- Multiple workers
- Concurrent processing
- Worker lifecycle management

## Phase 4: Persistence

Status: Planned

Deliverables:

- SQLite storage
- Durable task state
- Restart recovery

## Phase 5: Retries

Status: Planned

Deliverables:

- Retry policies
- Failure handling
- Dead-letter queue concepts

## Phase 6: Priority Scheduling

Status: Planned

Deliverables:

- High, normal, and low priorities
- Priority-aware workers

## Phase 7: Metrics

Status: Planned

Deliverables:

- Queue size
- Completed task count
- Failed task count
- Worker statistics

## Phase 8: Distributed Workers

Status: Planned

Deliverables:

- Multiple worker processes
- Central queue coordination
- Fault tolerance discussion and implementation notes
