CREATE TABLE tasks (
    id UUID PRIMARY KEY,
    task_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    worker_id UUID,
    output JSONB,
    error JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    locked_until TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,

    CONSTRAINT valid_status CHECK (status IN ('pending', 'running', 'completed', 'failed'))
);

-- Keep the dequeue index small by indexing only rows workers can claim:
-- fresh pending work and running work whose lease may expire.
CREATE INDEX idx_tasks_dequeue ON tasks (status, locked_until, created_at)
    WHERE status IN ('pending', 'running');
