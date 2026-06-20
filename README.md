# atlas-queue

High-performance distributed task queue built in Rust.

Atlas Queue is a production-inspired learning project for building queueing,
worker, scheduling, persistence, and distributed systems fundamentals in Rust.

## Current Phase

Phase 4: Postgres Persistence & Crash Recovery

- Persist tasks in Postgres.
- Dequeue safely across concurrent workers with `FOR UPDATE SKIP LOCKED`.
- Reclaim running tasks whose leases expire after worker failure.
- Track task lifecycle through typestate at the Rust API boundary.

## Run

```bash
docker compose up -d
set DATABASE_URL=postgres://atlas:atlas@localhost:5432/atlas_queue
cargo run
```

Local Postgres connection string:
`postgres://atlas:atlas@localhost:5432/atlas_queue`

## Test

```bash
cargo test
```
