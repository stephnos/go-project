CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_run ON history_events(run_id, sequence);
CREATE INDEX IF NOT EXISTS idx_tasks_available ON tasks(status, kind, queue_name, available_at);
CREATE INDEX IF NOT EXISTS idx_tasks_leases ON tasks(status, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_tasks_run ON tasks(run_id, updated_at DESC);
