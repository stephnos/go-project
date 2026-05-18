package engine

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/spapa/orchid/internal/history"
	"github.com/spapa/orchid/pkg/workflow"
)

func TestReplayDeterministic(t *testing.T) {
	builder := workflow.New("deterministic")
	builder.Activity("validate", "orders.validate", workflow.WithRetry(workflow.RetryPolicy{MaxAttempts: 1}))
	builder.Activity("charge", "payments.charge", workflow.DependsOn("validate"))
	definition, err := builder.Build()
	if err != nil {
		t.Fatalf("build workflow: %v", err)
	}

	now := time.Now().UTC()
	run := RunRecord{
		ID:           "run-1",
		WorkflowName: definition.Name,
		Status:       RunStatusRunning,
		Input:        json.RawMessage(`{"order_id":"ord-1"}`),
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	events := mustEvents(t,
		buildEvent(t, run.ID, 1, history.EventWorkflowStarted, "", "", history.WorkflowStartedPayload{
			Workflow: definition.Name,
			Input:    run.Input,
		}, now),
		buildEvent(t, run.ID, 2, history.EventTaskScheduled, "validate", "task-1", history.TaskScheduledPayload{
			TaskID:       "task-1",
			StepID:       "validate",
			Kind:         string(TaskKindActivity),
			Queue:        "default",
			Activity:     "orders.validate",
			Attempt:      1,
			MaxAttempts:  1,
			AvailableAt:  now,
			Input:        json.RawMessage(`{"run_id":"run-1"}`),
			Compensation: false,
		}, now),
		buildEvent(t, run.ID, 3, history.EventTaskLeased, "validate", "task-1", history.TaskLeasedPayload{
			TaskID:         "task-1",
			StepID:         "validate",
			WorkerID:       "worker-a",
			Attempt:        1,
			LeaseExpiresAt: now.Add(time.Second),
		}, now),
		buildEvent(t, run.ID, 4, history.EventTaskCompleted, "validate", "task-1", history.TaskCompletedPayload{
			TaskID:   "task-1",
			StepID:   "validate",
			WorkerID: "worker-a",
			Output:   json.RawMessage(`{"validated":true}`),
		}, now),
		buildEvent(t, run.ID, 5, history.EventTaskScheduled, "charge", "task-2", history.TaskScheduledPayload{
			TaskID:       "task-2",
			StepID:       "charge",
			Kind:         string(TaskKindActivity),
			Queue:        "default",
			Activity:     "payments.charge",
			Attempt:      1,
			MaxAttempts:  3,
			AvailableAt:  now,
			Input:        json.RawMessage(`{"run_id":"run-1"}`),
			Compensation: false,
		}, now),
		buildEvent(t, run.ID, 6, history.EventTaskCompleted, "charge", "task-2", history.TaskCompletedPayload{
			TaskID:   "task-2",
			StepID:   "charge",
			WorkerID: "worker-b",
			Output:   json.RawMessage(`{"captured":true}`),
		}, now),
		buildEvent(t, run.ID, 7, history.EventRunCompleted, "", "", history.RunCompletedPayload{
			Output: json.RawMessage(`{"validate":{"validated":true},"charge":{"captured":true}}`),
		}, now),
	)

	first, err := Replay(definition, run, events)
	if err != nil {
		t.Fatalf("first replay: %v", err)
	}
	second, err := Replay(definition, run, events)
	if err != nil {
		t.Fatalf("second replay: %v", err)
	}
	if !reflect.DeepEqual(first.CompletedOrder, second.CompletedOrder) {
		t.Fatalf("completed order mismatch: %#v != %#v", first.CompletedOrder, second.CompletedOrder)
	}
	if !jsonEqual(first.Run.Output, second.Run.Output) {
		t.Fatalf("run output mismatch: %s != %s", first.Run.Output, second.Run.Output)
	}
	if first.Run.Status != RunStatusCompleted || second.Run.Status != RunStatusCompleted {
		t.Fatalf("expected completed status, got %s and %s", first.Run.Status, second.Run.Status)
	}
}

func buildEvent(t *testing.T, runID string, seq int64, eventType history.EventType, stepID string, taskID string, payload any, now time.Time) history.Event {
	t.Helper()
	event, err := history.New(runID, eventType, stepID, taskID, payload, now)
	if err != nil {
		t.Fatalf("new event: %v", err)
	}
	event.Sequence = seq
	return event
}

func mustEvents(t *testing.T, events ...history.Event) []history.Event {
	t.Helper()
	return events
}
