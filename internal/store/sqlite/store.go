package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	_ "modernc.org/sqlite"

	"github.com/spapa/orchid/internal/engine"
	"github.com/spapa/orchid/internal/history"
)

const schema = `
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    status TEXT NOT NULL,
    input_json BLOB NOT NULL,
    output_json BLOB,
    failure TEXT NOT NULL DEFAULT '',
    cancel_reason TEXT NOT NULL DEFAULT '',
    metadata_json BLOB,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS history_events (
    run_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    step_id TEXT NOT NULL DEFAULT '',
    task_id TEXT NOT NULL DEFAULT '',
    payload_json BLOB NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, sequence),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    workflow_name TEXT NOT NULL,
    step_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    phase TEXT NOT NULL,
    activity_name TEXT NOT NULL DEFAULT '',
    queue_name TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    available_at TEXT NOT NULL,
    leased_by TEXT NOT NULL DEFAULT '',
    lease_expires_at TEXT,
    heartbeat_at TEXT,
    timeout_seconds INTEGER NOT NULL,
    input_json BLOB NOT NULL,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_run ON history_events(run_id, sequence);
CREATE INDEX IF NOT EXISTS idx_tasks_available ON tasks(status, kind, queue_name, available_at);
CREATE INDEX IF NOT EXISTS idx_tasks_leases ON tasks(status, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_tasks_run ON tasks(run_id, updated_at DESC);
`

type Store struct {
	db     *sql.DB
	logger *slog.Logger
}

func New(path string, logger *slog.Logger) (*Store, error) {
	if logger == nil {
		logger = slog.Default()
	}
	dsn := fmt.Sprintf("file:%s?_pragma=foreign_keys(ON)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return &Store{db: db, logger: logger}, nil
}

func (s *Store) Initialize(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) CreateRun(ctx context.Context, run engine.RunRecord, events []history.Event, tasks []engine.TaskRecord) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if err := insertRun(tx, run); err != nil {
			return err
		}
		for _, event := range events {
			if err := insertEvent(tx, event); err != nil {
				return err
			}
		}
		for _, task := range tasks {
			if err := insertTask(tx, task); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) GetRun(ctx context.Context, runID string) (engine.RunRecord, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, workflow_name, status, input_json, output_json, failure, cancel_reason, metadata_json, created_at, updated_at, completed_at, version
FROM runs WHERE id = ?`, runID)
	run, err := scanRun(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return engine.RunRecord{}, engine.ErrRunNotFound
		}
		return engine.RunRecord{}, err
	}
	return run, nil
}

func (s *Store) ListRuns(ctx context.Context, limit int) ([]engine.RunRecord, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, workflow_name, status, input_json, output_json, failure, cancel_reason, metadata_json, created_at, updated_at, completed_at, version
FROM runs
ORDER BY updated_at DESC
LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]engine.RunRecord, 0)
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

func (s *Store) GetHistory(ctx context.Context, runID string) ([]history.Event, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT run_id, sequence, event_type, step_id, task_id, payload_json, created_at
FROM history_events
WHERE run_id = ?
ORDER BY sequence ASC`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]history.Event, 0)
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, event)
	}
	return out, rows.Err()
}

func (s *Store) ListTasks(ctx context.Context, runID string) ([]engine.TaskRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, run_id, workflow_name, step_id, kind, phase, activity_name, queue_name, status, attempt, max_attempts,
       available_at, leased_by, lease_expires_at, heartbeat_at, timeout_seconds, input_json, last_error, created_at, updated_at
FROM tasks
WHERE run_id = ?
ORDER BY created_at ASC`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]engine.TaskRecord, 0)
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, task)
	}
	return out, rows.Err()
}

func (s *Store) GetTask(ctx context.Context, taskID string) (engine.TaskRecord, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, run_id, workflow_name, step_id, kind, phase, activity_name, queue_name, status, attempt, max_attempts,
       available_at, leased_by, lease_expires_at, heartbeat_at, timeout_seconds, input_json, last_error, created_at, updated_at
FROM tasks
WHERE id = ?`, taskID)
	task, err := scanTask(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return engine.TaskRecord{}, engine.ErrTaskNotFound
		}
		return engine.TaskRecord{}, err
	}
	return task, nil
}

func (s *Store) AppendRunChanges(ctx context.Context, run engine.RunRecord, expectedVersion int64, events []history.Event, mutations []engine.TaskMutation) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `
UPDATE runs
SET status = ?, input_json = ?, output_json = ?, failure = ?, cancel_reason = ?, metadata_json = ?, updated_at = ?, completed_at = ?, version = ?
WHERE id = ? AND version = ?`,
			run.Status, normalizeJSON(run.Input), nullableBytes(run.Output), run.Failure, run.CancelReason, marshalMetadata(run.Metadata),
			run.UpdatedAt.UTC().Format(time.RFC3339Nano), nullableTime(run.CompletedAt), run.Version, run.ID, expectedVersion)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			return fmt.Errorf("run version conflict for %s", run.ID)
		}
		for _, event := range events {
			if err := insertEvent(tx, event); err != nil {
				return err
			}
		}
		for _, mutation := range mutations {
			switch mutation.Action {
			case engine.TaskMutationInsert:
				if err := insertTask(tx, mutation.Task); err != nil {
					return err
				}
			case engine.TaskMutationUpdate:
				if err := updateTask(tx, mutation.Task); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unsupported task mutation action %q", mutation.Action)
			}
		}
		return nil
	})
}

func (s *Store) LeaseTasks(ctx context.Context, queueName string, workerID string, limit int, leaseTTL time.Duration, now time.Time) ([]engine.TaskLease, error) {
	if limit <= 0 {
		limit = 1
	}
	return s.leaseTasks(ctx, queueName, workerID, limit, leaseTTL, now)
}

func (s *Store) HeartbeatTask(ctx context.Context, taskID string, workerID string, extendBy time.Duration, now time.Time) (engine.HeartbeatReply, error) {
	var reply engine.HeartbeatReply
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		row := tx.QueryRowContext(ctx, `
SELECT t.run_id, t.step_id, t.phase, t.status, t.leased_by, r.status
FROM tasks t
JOIN runs r ON r.id = t.run_id
WHERE t.id = ?`, taskID)
		var runID string
		var stepID string
		var phase string
		var status string
		var leasedBy string
		var runStatus string
		if err := row.Scan(&runID, &stepID, &phase, &status, &leasedBy, &runStatus); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return engine.ErrTaskNotFound
			}
			return err
		}
		if status != string(engine.TaskStatusLeased) || leasedBy != workerID {
			return engine.ErrTaskLeaseLost
		}
		expiresAt := now.Add(extendBy)
		if _, err := tx.ExecContext(ctx, `
UPDATE tasks SET heartbeat_at = ?, lease_expires_at = ?, updated_at = ?
WHERE id = ? AND leased_by = ?`,
			now.UTC().Format(time.RFC3339Nano), expiresAt.UTC().Format(time.RFC3339Nano), now.UTC().Format(time.RFC3339Nano), taskID, workerID); err != nil {
			return err
		}
		event, err := history.New(runID, history.EventTaskHeartbeat, stepID, taskID, history.TaskHeartbeatPayload{
			TaskID:         taskID,
			WorkerID:       workerID,
			LeaseExpiresAt: expiresAt,
		}, now)
		if err != nil {
			return err
		}
		nextSeq, err := nextSequence(ctx, tx, runID)
		if err != nil {
			return err
		}
		event.Sequence = nextSeq
		if err := insertEvent(tx, event); err != nil {
			return err
		}
		if err := bumpRunVersion(ctx, tx, runID, nextSeq, now); err != nil {
			return err
		}
		reply = engine.HeartbeatReply{
			LeaseExpiresAt: expiresAt,
			CancelRequested: phase == string(engine.TaskPhaseForward) &&
				(runStatus == string(engine.RunStatusCancelling) || runStatus == string(engine.RunStatusCancelled)),
		}
		return nil
	})
	return reply, err
}

func (s *Store) AvailableTimers(ctx context.Context, limit int, now time.Time) ([]engine.TaskRecord, error) {
	return s.listTimedTasks(ctx, limit, now, string(engine.TaskKindTimer), string(engine.TaskStatusAvailable), false)
}

func (s *Store) ExpiredLeases(ctx context.Context, limit int, now time.Time) ([]engine.TaskRecord, error) {
	return s.listTimedTasks(ctx, limit, now, "", string(engine.TaskStatusLeased), true)
}

func (s *Store) leaseTasks(ctx context.Context, queueName string, workerID string, limit int, leaseTTL time.Duration, now time.Time) ([]engine.TaskLease, error) {
	out := make([]engine.TaskLease, 0, limit)
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
SELECT id, run_id, workflow_name, step_id, kind, phase, activity_name, queue_name, status, attempt, max_attempts,
       available_at, leased_by, lease_expires_at, heartbeat_at, timeout_seconds, input_json, last_error, created_at, updated_at
FROM tasks
WHERE queue_name = ? AND kind = ? AND status = ? AND available_at <= ?
ORDER BY available_at ASC, created_at ASC
LIMIT ?`,
			queueName, string(engine.TaskKindActivity), string(engine.TaskStatusAvailable), now.UTC().Format(time.RFC3339Nano), limit)
		if err != nil {
			return err
		}
		defer rows.Close()

		leaseExpiresAt := now.Add(leaseTTL)
		for rows.Next() {
			task, err := scanTask(rows)
			if err != nil {
				return err
			}
			result, err := tx.ExecContext(ctx, `
UPDATE tasks
SET status = ?, leased_by = ?, lease_expires_at = ?, heartbeat_at = ?, updated_at = ?
WHERE id = ? AND status = ?`,
				engine.TaskStatusLeased, workerID, leaseExpiresAt.UTC().Format(time.RFC3339Nano), now.UTC().Format(time.RFC3339Nano), now.UTC().Format(time.RFC3339Nano),
				task.ID, engine.TaskStatusAvailable)
			if err != nil {
				return err
			}
			affected, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if affected == 0 {
				continue
			}
			event, err := history.New(task.RunID, history.EventTaskLeased, task.StepID, task.ID, history.TaskLeasedPayload{
				TaskID:         task.ID,
				StepID:         task.StepID,
				WorkerID:       workerID,
				Attempt:        task.Attempt,
				LeaseExpiresAt: leaseExpiresAt,
			}, now)
			if err != nil {
				return err
			}
			nextSeq, err := nextSequence(ctx, tx, task.RunID)
			if err != nil {
				return err
			}
			event.Sequence = nextSeq
			if err := insertEvent(tx, event); err != nil {
				return err
			}
			if err := bumpRunVersion(ctx, tx, task.RunID, nextSeq, now); err != nil {
				return err
			}
			task.Status = engine.TaskStatusLeased
			task.LeaseOwner = workerID
			task.LeaseExpiresAt = &leaseExpiresAt
			task.HeartbeatAt = &now
			task.UpdatedAt = now
			out = append(out, engine.TaskLease{TaskRecord: task})
		}
		return rows.Err()
	})
	return out, err
}

func (s *Store) listTimedTasks(ctx context.Context, limit int, now time.Time, kind string, status string, leaseExpiry bool) ([]engine.TaskRecord, error) {
	if limit <= 0 {
		limit = 32
	}
	query := `
SELECT id, run_id, workflow_name, step_id, kind, phase, activity_name, queue_name, status, attempt, max_attempts,
       available_at, leased_by, lease_expires_at, heartbeat_at, timeout_seconds, input_json, last_error, created_at, updated_at
FROM tasks
WHERE status = ?`
	args := []any{status}
	if kind != "" {
		query += ` AND kind = ?`
		args = append(args, kind)
	}
	if leaseExpiry {
		query += ` AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?`
	} else {
		query += ` AND available_at <= ?`
	}
	query += ` ORDER BY updated_at ASC LIMIT ?`
	args = append(args, now.UTC().Format(time.RFC3339Nano), limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]engine.TaskRecord, 0)
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, task)
	}
	return out, rows.Err()
}

func (s *Store) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if err = fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func insertRun(tx *sql.Tx, run engine.RunRecord) error {
	_, err := tx.Exec(`
INSERT INTO runs (id, workflow_name, status, input_json, output_json, failure, cancel_reason, metadata_json, created_at, updated_at, completed_at, version)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		run.ID, run.WorkflowName, run.Status, normalizeJSON(run.Input), nullableBytes(run.Output), run.Failure, run.CancelReason, marshalMetadata(run.Metadata),
		run.CreatedAt.UTC().Format(time.RFC3339Nano), run.UpdatedAt.UTC().Format(time.RFC3339Nano), nullableTime(run.CompletedAt), run.Version)
	return err
}

func insertEvent(tx *sql.Tx, event history.Event) error {
	_, err := tx.Exec(`
INSERT INTO history_events (run_id, sequence, event_type, step_id, task_id, payload_json, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		event.RunID, event.Sequence, event.Type, event.StepID, event.TaskID, normalizeJSON(event.Payload), event.CreatedAt.UTC().Format(time.RFC3339Nano))
	return err
}

func insertTask(tx *sql.Tx, task engine.TaskRecord) error {
	_, err := tx.Exec(`
INSERT INTO tasks (id, run_id, workflow_name, step_id, kind, phase, activity_name, queue_name, status, attempt, max_attempts,
                   available_at, leased_by, lease_expires_at, heartbeat_at, timeout_seconds, input_json, last_error, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		task.ID, task.RunID, task.WorkflowName, task.StepID, task.Kind, task.Phase, task.ActivityName, task.QueueName, task.Status, task.Attempt, task.MaxAttempts,
		task.AvailableAt.UTC().Format(time.RFC3339Nano), task.LeaseOwner, nullableTime(task.LeaseExpiresAt), nullableTime(task.HeartbeatAt), int(task.Timeout.Seconds()),
		normalizeJSON(task.Input), task.LastError, task.CreatedAt.UTC().Format(time.RFC3339Nano), task.UpdatedAt.UTC().Format(time.RFC3339Nano))
	return err
}

func updateTask(tx *sql.Tx, task engine.TaskRecord) error {
	_, err := tx.Exec(`
UPDATE tasks
SET status = ?, attempt = ?, max_attempts = ?, available_at = ?, leased_by = ?, lease_expires_at = ?, heartbeat_at = ?, timeout_seconds = ?,
    input_json = ?, last_error = ?, updated_at = ?
WHERE id = ?`,
		task.Status, task.Attempt, task.MaxAttempts, task.AvailableAt.UTC().Format(time.RFC3339Nano), task.LeaseOwner, nullableTime(task.LeaseExpiresAt),
		nullableTime(task.HeartbeatAt), int(task.Timeout.Seconds()), normalizeJSON(task.Input), task.LastError, task.UpdatedAt.UTC().Format(time.RFC3339Nano), task.ID)
	return err
}

type scanner interface {
	Scan(dest ...any) error
}

func scanRun(row scanner) (engine.RunRecord, error) {
	var run engine.RunRecord
	var metadata []byte
	var input []byte
	var output []byte
	var createdAt string
	var updatedAt string
	var completedAt sql.NullString
	if err := row.Scan(&run.ID, &run.WorkflowName, &run.Status, &input, &output, &run.Failure, &run.CancelReason, &metadata, &createdAt, &updatedAt, &completedAt, &run.Version); err != nil {
		return engine.RunRecord{}, err
	}
	run.Input = normalizeJSON(input)
	run.Output = output
	run.Metadata = unmarshalMetadata(metadata)
	run.CreatedAt = mustParseTime(createdAt)
	run.UpdatedAt = mustParseTime(updatedAt)
	if completedAt.Valid {
		value := mustParseTime(completedAt.String)
		run.CompletedAt = &value
	}
	return run, nil
}

func scanEvent(row scanner) (history.Event, error) {
	var event history.Event
	var createdAt string
	if err := row.Scan(&event.RunID, &event.Sequence, &event.Type, &event.StepID, &event.TaskID, &event.Payload, &createdAt); err != nil {
		return history.Event{}, err
	}
	event.CreatedAt = mustParseTime(createdAt)
	return event, nil
}

func scanTask(row scanner) (engine.TaskRecord, error) {
	var task engine.TaskRecord
	var availableAt string
	var leaseExpiresAt sql.NullString
	var heartbeatAt sql.NullString
	var timeoutSeconds int
	var createdAt string
	var updatedAt string
	if err := row.Scan(&task.ID, &task.RunID, &task.WorkflowName, &task.StepID, &task.Kind, &task.Phase, &task.ActivityName, &task.QueueName, &task.Status, &task.Attempt, &task.MaxAttempts,
		&availableAt, &task.LeaseOwner, &leaseExpiresAt, &heartbeatAt, &timeoutSeconds, &task.Input, &task.LastError, &createdAt, &updatedAt); err != nil {
		return engine.TaskRecord{}, err
	}
	task.AvailableAt = mustParseTime(availableAt)
	task.Timeout = time.Duration(timeoutSeconds) * time.Second
	task.CreatedAt = mustParseTime(createdAt)
	task.UpdatedAt = mustParseTime(updatedAt)
	task.Input = normalizeJSON(task.Input)
	if leaseExpiresAt.Valid {
		value := mustParseTime(leaseExpiresAt.String)
		task.LeaseExpiresAt = &value
	}
	if heartbeatAt.Valid {
		value := mustParseTime(heartbeatAt.String)
		task.HeartbeatAt = &value
	}
	return task, nil
}

func nextSequence(ctx context.Context, tx *sql.Tx, runID string) (int64, error) {
	var next int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) + 1 FROM history_events WHERE run_id = ?`, runID).Scan(&next); err != nil {
		return 0, err
	}
	return next, nil
}

func bumpRunVersion(ctx context.Context, tx *sql.Tx, runID string, version int64, now time.Time) error {
	_, err := tx.ExecContext(ctx, `UPDATE runs SET version = ?, updated_at = ? WHERE id = ?`, version, now.UTC().Format(time.RFC3339Nano), runID)
	return err
}

func marshalMetadata(metadata map[string]string) []byte {
	if len(metadata) == 0 {
		return nil
	}
	body, _ := json.Marshal(metadata)
	return body
}

func unmarshalMetadata(raw []byte) map[string]string {
	if len(raw) == 0 {
		return nil
	}
	out := make(map[string]string)
	_ = json.Unmarshal(raw, &out)
	return out
}

func normalizeJSON(raw []byte) []byte {
	if len(raw) == 0 {
		return []byte("null")
	}
	return raw
}

func nullableBytes(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	return raw
}

func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func mustParseTime(raw string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		panic(err)
	}
	return parsed
}
