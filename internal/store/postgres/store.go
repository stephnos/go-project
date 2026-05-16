package postgres

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/spapa/orchid/internal/engine"
	"github.com/spapa/orchid/internal/history"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Store struct {
	db     *sql.DB
	logger *slog.Logger
}

func New(dsn string, logger *slog.Logger) (*Store, error) {
	if logger == nil {
		logger = slog.Default()
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(4)
	db.SetMaxOpenConns(16)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)
	return &Store{db: db, logger: logger}, nil
}

func (s *Store) Initialize(ctx context.Context) error {
	files, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	for _, name := range files {
		body, err := migrations.ReadFile(name)
		if err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx, string(body)); err != nil {
			return err
		}
	}
	return nil
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
FROM runs
WHERE id = $1`, runID)
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
LIMIT $1`, limit)
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
WHERE run_id = $1
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
WHERE run_id = $1
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
WHERE id = $1`, taskID)
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
SET status = $1, input_json = $2, output_json = $3, failure = $4, cancel_reason = $5, metadata_json = $6, updated_at = $7, completed_at = $8, version = $9
WHERE id = $10 AND version = $11`,
			run.Status, normalizeJSON(run.Input), nullableBytes(run.Output), run.Failure, run.CancelReason, marshalMetadata(run.Metadata),
			run.UpdatedAt.UTC(), nullableTime(run.CompletedAt), run.Version, run.ID, expectedVersion)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			return errors.New("run version conflict for " + run.ID)
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
				return errors.New("unsupported task mutation action")
			}
		}
		return nil
	})
}

func (s *Store) LeaseTasks(ctx context.Context, queueName string, workerID string, limit int, leaseTTL time.Duration, now time.Time) ([]engine.TaskLease, error) {
	if limit <= 0 {
		limit = 1
	}
	out := make([]engine.TaskLease, 0, limit)
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
SELECT id, run_id, workflow_name, step_id, kind, phase, activity_name, queue_name, status, attempt, max_attempts,
       available_at, leased_by, lease_expires_at, heartbeat_at, timeout_seconds, input_json, last_error, created_at, updated_at
FROM tasks
WHERE queue_name = $1 AND kind = $2 AND status = $3 AND available_at <= $4
ORDER BY available_at ASC, created_at ASC
LIMIT $5
FOR UPDATE SKIP LOCKED`,
			queueName, string(engine.TaskKindActivity), string(engine.TaskStatusAvailable), now.UTC(), limit)
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
			if _, err := tx.ExecContext(ctx, `
UPDATE tasks
SET status = $1, leased_by = $2, lease_expires_at = $3, heartbeat_at = $4, updated_at = $5
WHERE id = $6`,
				engine.TaskStatusLeased, workerID, leaseExpiresAt.UTC(), now.UTC(), now.UTC(), task.ID); err != nil {
				return err
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

func (s *Store) HeartbeatTask(ctx context.Context, taskID string, workerID string, extendBy time.Duration, now time.Time) (engine.HeartbeatReply, error) {
	var reply engine.HeartbeatReply
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		row := tx.QueryRowContext(ctx, `
SELECT t.run_id, t.step_id, t.phase, t.status, t.leased_by, r.status
FROM tasks t
JOIN runs r ON r.id = t.run_id
WHERE t.id = $1`, taskID)
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
UPDATE tasks
SET heartbeat_at = $1, lease_expires_at = $2, updated_at = $3
WHERE id = $4 AND leased_by = $5`,
			now.UTC(), expiresAt.UTC(), now.UTC(), taskID, workerID); err != nil {
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
	if limit <= 0 {
		limit = 32
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, run_id, workflow_name, step_id, kind, phase, activity_name, queue_name, status, attempt, max_attempts,
       available_at, leased_by, lease_expires_at, heartbeat_at, timeout_seconds, input_json, last_error, created_at, updated_at
FROM tasks
WHERE kind = $1 AND status = $2 AND available_at <= $3
ORDER BY updated_at ASC
LIMIT $4`,
		string(engine.TaskKindTimer), string(engine.TaskStatusAvailable), now.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectTasks(rows)
}

func (s *Store) ExpiredLeases(ctx context.Context, limit int, now time.Time) ([]engine.TaskRecord, error) {
	if limit <= 0 {
		limit = 32
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, run_id, workflow_name, step_id, kind, phase, activity_name, queue_name, status, attempt, max_attempts,
       available_at, leased_by, lease_expires_at, heartbeat_at, timeout_seconds, input_json, last_error, created_at, updated_at
FROM tasks
WHERE status = $1 AND lease_expires_at IS NOT NULL AND lease_expires_at <= $2
ORDER BY updated_at ASC
LIMIT $3`,
		string(engine.TaskStatusLeased), now.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectTasks(rows)
}

func collectTasks(rows *sql.Rows) ([]engine.TaskRecord, error) {
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
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func insertRun(tx *sql.Tx, run engine.RunRecord) error {
	_, err := tx.Exec(`
INSERT INTO runs (id, workflow_name, status, input_json, output_json, failure, cancel_reason, metadata_json, created_at, updated_at, completed_at, version)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		run.ID, run.WorkflowName, run.Status, normalizeJSON(run.Input), nullableBytes(run.Output), run.Failure, run.CancelReason, marshalMetadata(run.Metadata),
		run.CreatedAt.UTC(), run.UpdatedAt.UTC(), nullableTime(run.CompletedAt), run.Version)
	return err
}

func insertEvent(tx *sql.Tx, event history.Event) error {
	_, err := tx.Exec(`
INSERT INTO history_events (run_id, sequence, event_type, step_id, task_id, payload_json, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		event.RunID, event.Sequence, event.Type, event.StepID, event.TaskID, normalizeJSON(event.Payload), event.CreatedAt.UTC())
	return err
}

func insertTask(tx *sql.Tx, task engine.TaskRecord) error {
	_, err := tx.Exec(`
INSERT INTO tasks (id, run_id, workflow_name, step_id, kind, phase, activity_name, queue_name, status, attempt, max_attempts,
                   available_at, leased_by, lease_expires_at, heartbeat_at, timeout_seconds, input_json, last_error, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)`,
		task.ID, task.RunID, task.WorkflowName, task.StepID, task.Kind, task.Phase, task.ActivityName, task.QueueName, task.Status, task.Attempt, task.MaxAttempts,
		task.AvailableAt.UTC(), task.LeaseOwner, nullableTime(task.LeaseExpiresAt), nullableTime(task.HeartbeatAt), int(task.Timeout.Seconds()),
		normalizeJSON(task.Input), task.LastError, task.CreatedAt.UTC(), task.UpdatedAt.UTC())
	return err
}

func updateTask(tx *sql.Tx, task engine.TaskRecord) error {
	_, err := tx.Exec(`
UPDATE tasks
SET status = $1, attempt = $2, max_attempts = $3, available_at = $4, leased_by = $5, lease_expires_at = $6, heartbeat_at = $7, timeout_seconds = $8,
    input_json = $9, last_error = $10, updated_at = $11
WHERE id = $12`,
		task.Status, task.Attempt, task.MaxAttempts, task.AvailableAt.UTC(), task.LeaseOwner, nullableTime(task.LeaseExpiresAt), nullableTime(task.HeartbeatAt),
		int(task.Timeout.Seconds()), normalizeJSON(task.Input), task.LastError, task.UpdatedAt.UTC(), task.ID)
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
	var createdAt time.Time
	var updatedAt time.Time
	var completedAt sql.NullTime
	if err := row.Scan(&run.ID, &run.WorkflowName, &run.Status, &input, &output, &run.Failure, &run.CancelReason, &metadata, &createdAt, &updatedAt, &completedAt, &run.Version); err != nil {
		return engine.RunRecord{}, err
	}
	run.Input = normalizeJSON(input)
	run.Output = output
	run.Metadata = unmarshalMetadata(metadata)
	run.CreatedAt = createdAt.UTC()
	run.UpdatedAt = updatedAt.UTC()
	if completedAt.Valid {
		value := completedAt.Time.UTC()
		run.CompletedAt = &value
	}
	return run, nil
}

func scanEvent(row scanner) (history.Event, error) {
	var event history.Event
	var createdAt time.Time
	if err := row.Scan(&event.RunID, &event.Sequence, &event.Type, &event.StepID, &event.TaskID, &event.Payload, &createdAt); err != nil {
		return history.Event{}, err
	}
	event.CreatedAt = createdAt.UTC()
	return event, nil
}

func scanTask(row scanner) (engine.TaskRecord, error) {
	var task engine.TaskRecord
	var leaseExpiresAt sql.NullTime
	var heartbeatAt sql.NullTime
	var timeoutSeconds int
	var createdAt time.Time
	var updatedAt time.Time
	if err := row.Scan(&task.ID, &task.RunID, &task.WorkflowName, &task.StepID, &task.Kind, &task.Phase, &task.ActivityName, &task.QueueName, &task.Status, &task.Attempt, &task.MaxAttempts,
		&task.AvailableAt, &task.LeaseOwner, &leaseExpiresAt, &heartbeatAt, &timeoutSeconds, &task.Input, &task.LastError, &createdAt, &updatedAt); err != nil {
		return engine.TaskRecord{}, err
	}
	task.AvailableAt = task.AvailableAt.UTC()
	task.Timeout = time.Duration(timeoutSeconds) * time.Second
	task.CreatedAt = createdAt.UTC()
	task.UpdatedAt = updatedAt.UTC()
	task.Input = normalizeJSON(task.Input)
	if leaseExpiresAt.Valid {
		value := leaseExpiresAt.Time.UTC()
		task.LeaseExpiresAt = &value
	}
	if heartbeatAt.Valid {
		value := heartbeatAt.Time.UTC()
		task.HeartbeatAt = &value
	}
	return task, nil
}

func nextSequence(ctx context.Context, tx *sql.Tx, runID string) (int64, error) {
	var next int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) + 1 FROM history_events WHERE run_id = $1`, runID).Scan(&next); err != nil {
		return 0, err
	}
	return next, nil
}

func bumpRunVersion(ctx context.Context, tx *sql.Tx, runID string, version int64, now time.Time) error {
	_, err := tx.ExecContext(ctx, `UPDATE runs SET version = $1, updated_at = $2 WHERE id = $3`, version, now.UTC(), runID)
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
	return value.UTC()
}
