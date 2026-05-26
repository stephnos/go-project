package engine

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/spapa/orchid/internal/history"
	"github.com/spapa/orchid/pkg/workflow"
)

func BenchmarkReplayHistory(b *testing.B) {
	builder := workflow.New("benchmark")
	for i := 0; i < 64; i++ {
		stepID := fmt.Sprintf("step_%02d", i)
		if i == 0 {
			builder.Activity(stepID, "noop")
			continue
		}
		builder.Activity(stepID, "noop", workflow.DependsOn(fmt.Sprintf("step_%02d", i-1)))
	}
	definition, err := builder.Build()
	if err != nil {
		b.Fatalf("build workflow: %v", err)
	}
	now := time.Now().UTC()
	run := RunRecord{
		ID:           "bench-run",
		WorkflowName: definition.Name,
		Status:       RunStatusRunning,
		Input:        json.RawMessage(`{"bench":true}`),
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	events := make([]history.Event, 0, 1+len(definition.StepIDs())*2+1)
	started, _ := history.New(run.ID, history.EventWorkflowStarted, "", "", history.WorkflowStartedPayload{
		Workflow: definition.Name,
		Input:    run.Input,
	}, now)
	started.Sequence = 1
	events = append(events, started)

	seq := int64(2)
	for _, stepID := range definition.StepIDs() {
		scheduled, _ := history.New(run.ID, history.EventTaskScheduled, stepID, "task-"+stepID, history.TaskScheduledPayload{
			TaskID:       "task-" + stepID,
			StepID:       stepID,
			Kind:         string(TaskKindActivity),
			Queue:        "default",
			Activity:     "noop",
			Attempt:      1,
			MaxAttempts:  1,
			AvailableAt:  now,
			Input:        json.RawMessage(`{"run_id":"bench-run"}`),
			Compensation: false,
		}, now)
		scheduled.Sequence = seq
		seq++
		completed, _ := history.New(run.ID, history.EventTaskCompleted, stepID, "task-"+stepID, history.TaskCompletedPayload{
			TaskID:   "task-" + stepID,
			StepID:   stepID,
			WorkerID: "bench-worker",
			Output:   json.RawMessage(`{"ok":true}`),
		}, now)
		completed.Sequence = seq
		seq++
		events = append(events, scheduled, completed)
	}
	done, _ := history.New(run.ID, history.EventRunCompleted, "", "", history.RunCompletedPayload{
		Output: json.RawMessage(`{"done":true}`),
	}, now)
	done.Sequence = seq
	events = append(events, done)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Replay(definition, run, events); err != nil {
			b.Fatalf("replay: %v", err)
		}
	}
}
