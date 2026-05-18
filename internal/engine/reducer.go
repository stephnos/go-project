package engine

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/spapa/orchid/internal/history"
	"github.com/spapa/orchid/pkg/workflow"
)

var nullJSON = json.RawMessage("null")

func Replay(def workflow.Definition, run RunRecord, events []history.Event) (RunState, error) {
	state := RunState{
		Run:      run,
		Metadata: cloneMetadata(run.Metadata),
		Steps:    make(map[string]*StepState, len(def.Steps)),
		Tasks:    make(map[string]*TaskState),
		Outputs:  make(map[string]json.RawMessage),
	}
	for _, id := range def.StepIDs() {
		step := def.Steps[id]
		state.Steps[id] = &StepState{
			ID:           step.ID,
			Definition:   step,
			Metadata:     cloneMetadata(step.Metadata),
			Dependencies: slices.Clone(step.Dependencies),
		}
	}
	if state.Run.WorkflowName == "" {
		state.Run.WorkflowName = def.Name
	}
	if state.Run.Status == "" {
		state.Run.Status = RunStatusRunning
	}
	for _, event := range events {
		if err := applyEvent(&state, event); err != nil {
			return RunState{}, err
		}
	}
	for _, step := range state.Steps {
		step.DependenciesSatisfied = dependenciesCompleted(step.Definition, state)
	}
	return state, nil
}

func applyEvent(state *RunState, event history.Event) error {
	state.Run.UpdatedAt = event.CreatedAt
	state.Run.Version = event.Sequence

	switch event.Type {
	case history.EventWorkflowStarted:
		var payload history.WorkflowStartedPayload
		if err := decodePayload(event.Payload, &payload); err != nil {
			return err
		}
		state.Run.WorkflowName = payload.Workflow
		state.Run.Input = payload.Input
		state.Metadata = cloneMetadata(payload.Metadata)
		state.Run.Metadata = cloneMetadata(payload.Metadata)
		state.Run.Status = RunStatusRunning
	case history.EventTaskScheduled:
		var payload history.TaskScheduledPayload
		if err := decodePayload(event.Payload, &payload); err != nil {
			return err
		}
		task := &TaskState{
			ID:          payload.TaskID,
			StepID:      payload.StepID,
			Kind:        TaskKind(payload.Kind),
			Status:      TaskStatusAvailable,
			Attempt:     payload.Attempt,
			AvailableAt: payload.AvailableAt,
		}
		if payload.Compensation {
			task.Phase = TaskPhaseCompensation
		} else {
			task.Phase = TaskPhaseForward
		}
		state.Tasks[payload.TaskID] = task
		step := state.Steps[payload.StepID]
		if step == nil {
			return fmt.Errorf("task scheduled for unknown step %q", payload.StepID)
		}
		if payload.Compensation {
			step.CompensationScheduled = true
			step.CompensationTaskID = payload.TaskID
		} else {
			step.ForwardStatus = TaskStatusAvailable
			step.ForwardTaskID = payload.TaskID
			step.ForwardAttempt = payload.Attempt
		}
	case history.EventTaskLeased:
		var payload history.TaskLeasedPayload
		if err := decodePayload(event.Payload, &payload); err != nil {
			return err
		}
		task := state.Tasks[payload.TaskID]
		if task == nil {
			return fmt.Errorf("task leased before schedule: %q", payload.TaskID)
		}
		task.Status = TaskStatusLeased
		task.LeaseOwner = payload.WorkerID
		expires := payload.LeaseExpiresAt
		task.LeaseExpiresAt = &expires
		step := state.Steps[payload.StepID]
		if step != nil && task.Phase == TaskPhaseForward {
			step.ForwardStatus = TaskStatusLeased
		}
	case history.EventTaskHeartbeat:
		var payload history.TaskHeartbeatPayload
		if err := decodePayload(event.Payload, &payload); err != nil {
			return err
		}
		task := state.Tasks[event.TaskID]
		if task == nil {
			return fmt.Errorf("heartbeat for unknown task %q", event.TaskID)
		}
		task.LeaseOwner = payload.WorkerID
		expires := payload.LeaseExpiresAt
		task.LeaseExpiresAt = &expires
		heartbeatAt := event.CreatedAt
		task.HeartbeatAt = &heartbeatAt
	case history.EventTaskCompleted:
		var payload history.TaskCompletedPayload
		if err := decodePayload(event.Payload, &payload); err != nil {
			return err
		}
		task := state.Tasks[payload.TaskID]
		if task == nil {
			return fmt.Errorf("completion for unknown task %q", payload.TaskID)
		}
		task.Status = TaskStatusCompleted
		task.Output = payload.Output
		task.LeaseOwner = payload.WorkerID
		task.LeaseExpiresAt = nil
		step := state.Steps[payload.StepID]
		if step == nil {
			return fmt.Errorf("task completed for unknown step %q", payload.StepID)
		}
		if payload.Compensation {
			step.CompensationCompleted = true
			step.CompensationOutput = payload.Output
		} else {
			step.ForwardStatus = TaskStatusCompleted
			step.ForwardOutput = payload.Output
			state.Outputs[payload.StepID] = payload.Output
			if !slices.Contains(state.CompletedOrder, payload.StepID) {
				state.CompletedOrder = append(state.CompletedOrder, payload.StepID)
			}
		}
	case history.EventTaskFailed:
		var payload history.TaskFailedPayload
		if err := decodePayload(event.Payload, &payload); err != nil {
			return err
		}
		task := state.Tasks[payload.TaskID]
		if task == nil {
			return fmt.Errorf("failure for unknown task %q", payload.TaskID)
		}
		task.Status = TaskStatusFailed
		task.Error = payload.Error
		task.LeaseOwner = payload.WorkerID
		task.LeaseExpiresAt = nil
		step := state.Steps[payload.StepID]
		if step == nil {
			return fmt.Errorf("task failed for unknown step %q", payload.StepID)
		}
		if payload.Compensation {
			step.CompensationFailed = true
			step.CompensationError = payload.Error
		} else {
			step.ForwardStatus = TaskStatusFailed
			step.ForwardError = payload.Error
		}
	case history.EventTaskRetryScheduled:
		var payload history.TaskRetryScheduledPayload
		if err := decodePayload(event.Payload, &payload); err != nil {
			return err
		}
		task := state.Tasks[payload.TaskID]
		if task == nil {
			return fmt.Errorf("retry scheduled for unknown task %q", payload.TaskID)
		}
		task.Status = TaskStatusAvailable
		task.Attempt = payload.Attempt
		task.AvailableAt = payload.AvailableAt
		task.Error = payload.Error
		task.LeaseOwner = ""
		task.LeaseExpiresAt = nil
		step := state.Steps[payload.StepID]
		if step != nil && task.Phase == TaskPhaseForward {
			step.ForwardStatus = TaskStatusAvailable
			step.ForwardAttempt = payload.Attempt
			step.ForwardError = payload.Error
		}
	case history.EventTaskLeaseExpired:
		var payload history.TaskLeaseExpiredPayload
		if err := decodePayload(event.Payload, &payload); err != nil {
			return err
		}
		task := state.Tasks[payload.TaskID]
		if task == nil {
			return fmt.Errorf("lease expired for unknown task %q", payload.TaskID)
		}
		task.Status = TaskStatusFailed
		task.Error = payload.Error
		task.LeaseOwner = ""
		task.LeaseExpiresAt = nil
	case history.EventTaskCancelled:
		var payload history.TaskCancelledPayload
		if err := decodePayload(event.Payload, &payload); err != nil {
			return err
		}
		task := state.Tasks[payload.TaskID]
		if task == nil {
			return fmt.Errorf("task cancelled for unknown task %q", payload.TaskID)
		}
		task.Status = TaskStatusCancelled
		task.Error = payload.Reason
		task.LeaseOwner = ""
		task.LeaseExpiresAt = nil
		step := state.Steps[payload.StepID]
		if step != nil && !payload.Compensation {
			step.ForwardStatus = TaskStatusCancelled
			step.ForwardError = payload.Reason
		}
	case history.EventTimerFired:
		var payload history.TimerFiredPayload
		if err := decodePayload(event.Payload, &payload); err != nil {
			return err
		}
		task := state.Tasks[payload.TaskID]
		if task == nil {
			return fmt.Errorf("timer fired for unknown task %q", payload.TaskID)
		}
		task.Status = TaskStatusCompleted
		task.Output = nullJSON
		step := state.Steps[payload.StepID]
		if step == nil {
			return fmt.Errorf("timer fired for unknown step %q", payload.StepID)
		}
		step.ForwardStatus = TaskStatusCompleted
		step.ForwardOutput = nullJSON
		state.Outputs[payload.StepID] = nullJSON
		if !slices.Contains(state.CompletedOrder, payload.StepID) {
			state.CompletedOrder = append(state.CompletedOrder, payload.StepID)
		}
	case history.EventRunCancelRequested:
		var payload history.RunCancelRequestedPayload
		if err := decodePayload(event.Payload, &payload); err != nil {
			return err
		}
		state.CancelRequested = true
		state.Run.CancelReason = payload.Reason
		state.Run.Status = RunStatusCancelling
	case history.EventRunCompensating:
		var payload history.RunCompensatingPayload
		if err := decodePayload(event.Payload, &payload); err != nil {
			return err
		}
		state.Compensating = true
		state.Run.Status = RunStatusCompensating
		if state.CancelRequested {
			state.Run.CancelReason = payload.Reason
		} else {
			state.Run.Failure = payload.Reason
		}
	case history.EventRunCompleted:
		var payload history.RunCompletedPayload
		if err := decodePayload(event.Payload, &payload); err != nil {
			return err
		}
		state.Run.Status = RunStatusCompleted
		state.Run.Output = payload.Output
		state.WorkflowFinished = true
		completedAt := event.CreatedAt
		state.Run.CompletedAt = &completedAt
	case history.EventRunFailed:
		var payload history.RunFailedPayload
		if err := decodePayload(event.Payload, &payload); err != nil {
			return err
		}
		state.Run.Status = RunStatusFailed
		state.Run.Failure = payload.Error
		state.WorkflowFinished = true
		completedAt := event.CreatedAt
		state.Run.CompletedAt = &completedAt
	case history.EventRunCancelled:
		var payload history.RunCancelledPayload
		if err := decodePayload(event.Payload, &payload); err != nil {
			return err
		}
		state.Run.Status = RunStatusCancelled
		state.Run.CancelReason = payload.Reason
		state.WorkflowFinished = true
		completedAt := event.CreatedAt
		state.Run.CompletedAt = &completedAt
	default:
		return fmt.Errorf("unsupported event type %q", event.Type)
	}
	return nil
}

func dependenciesCompleted(step workflow.Step, state RunState) bool {
	for _, dep := range step.Dependencies {
		if output, ok := state.Outputs[dep]; !ok || output == nil {
			return false
		}
	}
	return true
}

func decodePayload(raw json.RawMessage, target any) error {
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("decode history payload: %w", err)
	}
	return nil
}

func cloneMetadata(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func BackoffForAttempt(policy workflow.RetryPolicy, attempt int) time.Duration {
	normalized := policy.Normalize()
	backoff := normalized.InitialBackoff
	for i := 1; i < attempt; i++ {
		backoff *= 2
		if backoff >= normalized.MaxBackoff {
			return normalized.MaxBackoff
		}
	}
	return backoff
}
