package engine

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/spapa/orchid/internal/history"
	"github.com/spapa/orchid/pkg/workflow"
)

type RunStatus string

const (
	RunStatusRunning      RunStatus = "running"
	RunStatusCompensating RunStatus = "compensating"
	RunStatusCancelling   RunStatus = "cancelling"
	RunStatusCompleted    RunStatus = "completed"
	RunStatusFailed       RunStatus = "failed"
	RunStatusCancelled    RunStatus = "cancelled"
)

func (s RunStatus) Terminal() bool {
	switch s {
	case RunStatusCompleted, RunStatusFailed, RunStatusCancelled:
		return true
	default:
		return false
	}
}

type TaskKind string

const (
	TaskKindActivity TaskKind = "activity"
	TaskKindTimer    TaskKind = "timer"
)

type TaskStatus string

const (
	TaskStatusAvailable TaskStatus = "available"
	TaskStatusLeased    TaskStatus = "leased"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

type TaskPhase string

const (
	TaskPhaseForward      TaskPhase = "forward"
	TaskPhaseCompensation TaskPhase = "compensation"
)

type RunRecord struct {
	ID           string            `json:"id"`
	WorkflowName string            `json:"workflow_name"`
	Status       RunStatus         `json:"status"`
	Input        json.RawMessage   `json:"input"`
	Output       json.RawMessage   `json:"output,omitempty"`
	Failure      string            `json:"failure,omitempty"`
	CancelReason string            `json:"cancel_reason,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
	CompletedAt  *time.Time        `json:"completed_at,omitempty"`
	Version      int64             `json:"version"`
}

type TaskRecord struct {
	ID             string          `json:"id"`
	RunID          string          `json:"run_id"`
	WorkflowName   string          `json:"workflow_name"`
	StepID         string          `json:"step_id"`
	Kind           TaskKind        `json:"kind"`
	Phase          TaskPhase       `json:"phase"`
	ActivityName   string          `json:"activity_name,omitempty"`
	QueueName      string          `json:"queue_name"`
	Status         TaskStatus      `json:"status"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	AvailableAt    time.Time       `json:"available_at"`
	LeaseOwner     string          `json:"lease_owner,omitempty"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	HeartbeatAt    *time.Time      `json:"heartbeat_at,omitempty"`
	Timeout        time.Duration   `json:"timeout"`
	Input          json.RawMessage `json:"input"`
	LastError      string          `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type TaskLease struct {
	TaskRecord
	Dependencies map[string]json.RawMessage `json:"dependencies,omitempty"`
}

type HeartbeatReply struct {
	LeaseExpiresAt  time.Time `json:"lease_expires_at"`
	CancelRequested bool      `json:"cancel_requested"`
}

type TaskMutationAction string

const (
	TaskMutationInsert TaskMutationAction = "insert"
	TaskMutationUpdate TaskMutationAction = "update"
)

type TaskMutation struct {
	Action TaskMutationAction
	Task   TaskRecord
}

type Repository interface {
	CreateRun(context.Context, RunRecord, []history.Event, []TaskRecord) error
	GetRun(context.Context, string) (RunRecord, error)
	ListRuns(context.Context, int) ([]RunRecord, error)
	GetHistory(context.Context, string) ([]history.Event, error)
	ListTasks(context.Context, string) ([]TaskRecord, error)
	GetTask(context.Context, string) (TaskRecord, error)
	AppendRunChanges(context.Context, RunRecord, int64, []history.Event, []TaskMutation) error
	LeaseTasks(context.Context, string, string, int, time.Duration, time.Time) ([]TaskLease, error)
	HeartbeatTask(context.Context, string, string, time.Duration, time.Time) (HeartbeatReply, error)
	AvailableTimers(context.Context, int, time.Time) ([]TaskRecord, error)
	ExpiredLeases(context.Context, int, time.Time) ([]TaskRecord, error)
}

type StepState struct {
	ID                    string            `json:"id"`
	ForwardStatus         TaskStatus        `json:"forward_status,omitempty"`
	ForwardTaskID         string            `json:"forward_task_id,omitempty"`
	ForwardAttempt        int               `json:"forward_attempt,omitempty"`
	ForwardOutput         json.RawMessage   `json:"forward_output,omitempty"`
	ForwardError          string            `json:"forward_error,omitempty"`
	DependenciesSatisfied bool              `json:"dependencies_satisfied"`
	CompensationScheduled bool              `json:"compensation_scheduled,omitempty"`
	CompensationTaskID    string            `json:"compensation_task_id,omitempty"`
	CompensationCompleted bool              `json:"compensation_completed,omitempty"`
	CompensationFailed    bool              `json:"compensation_failed,omitempty"`
	CompensationOutput    json.RawMessage   `json:"compensation_output,omitempty"`
	CompensationError     string            `json:"compensation_error,omitempty"`
	Metadata              map[string]string `json:"metadata,omitempty"`
	Dependencies          []string          `json:"dependencies,omitempty"`
	Definition            workflow.Step     `json:"definition"`
}

type TaskState struct {
	ID             string          `json:"id"`
	StepID         string          `json:"step_id"`
	Kind           TaskKind        `json:"kind"`
	Phase          TaskPhase       `json:"phase"`
	Status         TaskStatus      `json:"status"`
	Attempt        int             `json:"attempt"`
	AvailableAt    time.Time       `json:"available_at"`
	LeaseOwner     string          `json:"lease_owner,omitempty"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	HeartbeatAt    *time.Time      `json:"heartbeat_at,omitempty"`
	Output         json.RawMessage `json:"output,omitempty"`
	Error          string          `json:"error,omitempty"`
}

type RunState struct {
	Run              RunRecord                  `json:"run"`
	Metadata         map[string]string          `json:"metadata,omitempty"`
	Steps            map[string]*StepState      `json:"steps"`
	Tasks            map[string]*TaskState      `json:"tasks"`
	Outputs          map[string]json.RawMessage `json:"outputs"`
	CompletedOrder   []string                   `json:"completed_order,omitempty"`
	CancelRequested  bool                       `json:"cancel_requested"`
	Compensating     bool                       `json:"compensating"`
	WorkflowFinished bool                       `json:"workflow_finished"`
}

type RunView struct {
	Run   RunRecord    `json:"run"`
	Tasks []TaskRecord `json:"tasks"`
}

type ReplayReport struct {
	Run            RunRecord    `json:"run"`
	State          RunState     `json:"state"`
	SummaryMatches bool         `json:"summary_matches"`
	Tasks          []TaskRecord `json:"tasks"`
	HistoryLength  int          `json:"history_length"`
}

var (
	ErrRunNotFound        = errors.New("run not found")
	ErrTaskNotFound       = errors.New("task not found")
	ErrWorkflowNotFound   = errors.New("workflow not found")
	ErrHandlerNotFound    = errors.New("activity handler not found")
	ErrTaskLeaseLost      = errors.New("task lease lost")
	ErrRunCancelledSignal = errors.New("run cancelled")
)
