package history

import (
	"encoding/json"
	"fmt"
	"time"
)

type EventType string

const (
	EventWorkflowStarted    EventType = "workflow.started"
	EventTaskScheduled      EventType = "task.scheduled"
	EventTaskLeased         EventType = "task.leased"
	EventTaskHeartbeat      EventType = "task.heartbeat"
	EventTaskCompleted      EventType = "task.completed"
	EventTaskFailed         EventType = "task.failed"
	EventTaskRetryScheduled EventType = "task.retry_scheduled"
	EventTaskLeaseExpired   EventType = "task.lease_expired"
	EventTaskCancelled      EventType = "task.cancelled"
	EventTimerFired         EventType = "timer.fired"
	EventRunCancelRequested EventType = "run.cancel_requested"
	EventRunCompensating    EventType = "run.compensating"
	EventRunCompleted       EventType = "run.completed"
	EventRunFailed          EventType = "run.failed"
	EventRunCancelled       EventType = "run.cancelled"
)

type Event struct {
	RunID     string          `json:"run_id"`
	Sequence  int64           `json:"sequence"`
	Type      EventType       `json:"type"`
	StepID    string          `json:"step_id,omitempty"`
	TaskID    string          `json:"task_id,omitempty"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

func New(runID string, eventType EventType, stepID string, taskID string, payload any, now time.Time) (Event, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return Event{}, fmt.Errorf("marshal event payload: %w", err)
	}
	return Event{
		RunID:     runID,
		Type:      eventType,
		StepID:    stepID,
		TaskID:    taskID,
		Payload:   body,
		CreatedAt: now.UTC(),
	}, nil
}

type WorkflowStartedPayload struct {
	Workflow string            `json:"workflow"`
	Input    json.RawMessage   `json:"input"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type TaskScheduledPayload struct {
	TaskID         string                     `json:"task_id"`
	StepID         string                     `json:"step_id"`
	Kind           string                     `json:"kind"`
	Queue          string                     `json:"queue"`
	Activity       string                     `json:"activity,omitempty"`
	Attempt        int                        `json:"attempt"`
	MaxAttempts    int                        `json:"max_attempts"`
	TimeoutSeconds int                        `json:"timeout_seconds"`
	AvailableAt    time.Time                  `json:"available_at"`
	Input          json.RawMessage            `json:"input"`
	Dependencies   map[string]json.RawMessage `json:"dependencies,omitempty"`
	Compensation   bool                       `json:"compensation"`
}

type TaskLeasedPayload struct {
	TaskID         string    `json:"task_id"`
	StepID         string    `json:"step_id"`
	WorkerID       string    `json:"worker_id"`
	Attempt        int       `json:"attempt"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
}

type TaskHeartbeatPayload struct {
	TaskID         string    `json:"task_id"`
	WorkerID       string    `json:"worker_id"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
}

type TaskCompletedPayload struct {
	TaskID       string          `json:"task_id"`
	StepID       string          `json:"step_id"`
	WorkerID     string          `json:"worker_id,omitempty"`
	Output       json.RawMessage `json:"output"`
	Compensation bool            `json:"compensation"`
}

type TaskFailedPayload struct {
	TaskID       string `json:"task_id"`
	StepID       string `json:"step_id"`
	WorkerID     string `json:"worker_id,omitempty"`
	Error        string `json:"error"`
	Compensation bool   `json:"compensation"`
}

type TaskRetryScheduledPayload struct {
	TaskID      string    `json:"task_id"`
	StepID      string    `json:"step_id"`
	Attempt     int       `json:"attempt"`
	AvailableAt time.Time `json:"available_at"`
	Error       string    `json:"error"`
}

type TaskLeaseExpiredPayload struct {
	TaskID string `json:"task_id"`
	StepID string `json:"step_id"`
	Error  string `json:"error"`
}

type TaskCancelledPayload struct {
	TaskID       string `json:"task_id"`
	StepID       string `json:"step_id"`
	Reason       string `json:"reason"`
	Compensation bool   `json:"compensation"`
}

type TimerFiredPayload struct {
	TaskID string `json:"task_id"`
	StepID string `json:"step_id"`
}

type RunCancelRequestedPayload struct {
	Reason string `json:"reason"`
}

type RunCompensatingPayload struct {
	Reason string `json:"reason"`
}

type RunCompletedPayload struct {
	Output json.RawMessage `json:"output"`
}

type RunFailedPayload struct {
	Error string `json:"error"`
}

type RunCancelledPayload struct {
	Reason string `json:"reason"`
}
