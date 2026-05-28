package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/spapa/orchid/internal/history"
	"github.com/spapa/orchid/pkg/workflow"
)

const timerQueueName = "__orchid_timers__"

type Service struct {
	repo         Repository
	registry     *workflow.Registry
	logger       *slog.Logger
	tracer       trace.Tracer
	runsStarted  metric.Int64Counter
	runsFinished metric.Int64Counter
	taskResults  metric.Int64Counter
}

func NewService(repo Repository, registry *workflow.Registry, logger *slog.Logger, tracer trace.Tracer, meter metric.Meter) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	if tracer == nil {
		tracer = otel.Tracer("orchid/engine")
	}
	if meter == nil {
		meter = otel.GetMeterProvider().Meter("orchid/engine")
	}
	runsStarted, _ := meter.Int64Counter("orchid.runs.started")
	runsFinished, _ := meter.Int64Counter("orchid.runs.finished")
	taskResults, _ := meter.Int64Counter("orchid.tasks.results")
	return &Service{
		repo:         repo,
		registry:     registry,
		logger:       logger,
		tracer:       tracer,
		runsStarted:  runsStarted,
		runsFinished: runsFinished,
		taskResults:  taskResults,
	}
}

func (s *Service) StartRun(ctx context.Context, workflowName string, input json.RawMessage, metadata map[string]string) (RunRecord, error) {
	ctx, span := s.tracer.Start(ctx, "engine.StartRun")
	defer span.End()

	def, ok := s.registry.Definition(workflowName)
	if !ok {
		return RunRecord{}, ErrWorkflowNotFound
	}
	now := time.Now().UTC()
	if len(input) == 0 {
		input = nullJSON
	}
	run := RunRecord{
		ID:           uuid.NewString(),
		WorkflowName: def.Name,
		Status:       RunStatusRunning,
		Input:        input,
		Metadata:     cloneMetadata(metadata),
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	seq := int64(1)
	started, err := s.makeEvent(run.ID, &seq, history.EventWorkflowStarted, "", "", history.WorkflowStartedPayload{
		Workflow: def.Name,
		Input:    input,
		Metadata: metadata,
	}, now)
	if err != nil {
		return RunRecord{}, err
	}
	events := []history.Event{started}
	state, err := Replay(def, run, events)
	if err != nil {
		return RunRecord{}, err
	}
	scheduledEvents, tasks, err := s.scheduleForwardReady(run, def, &state, &seq, now)
	if err != nil {
		return RunRecord{}, err
	}
	events = append(events, scheduledEvents...)
	run.Version = int64(len(events))
	run.UpdatedAt = now
	if err := s.repo.CreateRun(ctx, run, events, tasks); err != nil {
		return RunRecord{}, err
	}
	s.runsStarted.Add(ctx, 1, metric.WithAttributes(attribute.String("workflow", workflowName)))
	return run, nil
}

func (s *Service) GetRun(ctx context.Context, runID string) (RunView, error) {
	run, err := s.repo.GetRun(ctx, runID)
	if err != nil {
		return RunView{}, err
	}
	tasks, err := s.repo.ListTasks(ctx, runID)
	if err != nil {
		return RunView{}, err
	}
	return RunView{Run: run, Tasks: tasks}, nil
}

func (s *Service) ListRuns(ctx context.Context, limit int) ([]RunRecord, error) {
	return s.repo.ListRuns(ctx, limit)
}

func (s *Service) GetHistory(ctx context.Context, runID string) ([]history.Event, error) {
	return s.repo.GetHistory(ctx, runID)
}

func (s *Service) ReplayRun(ctx context.Context, runID string) (ReplayReport, error) {
	loaded, err := s.loadRun(ctx, runID)
	if err != nil {
		return ReplayReport{}, err
	}
	summaryMatches := loaded.run.Status == loaded.state.Run.Status &&
		loaded.run.Failure == loaded.state.Run.Failure &&
		loaded.run.CancelReason == loaded.state.Run.CancelReason &&
		jsonEqual(loaded.run.Output, loaded.state.Run.Output)
	return ReplayReport{
		Run:            loaded.run,
		State:          loaded.state,
		SummaryMatches: summaryMatches,
		Tasks:          loaded.taskList,
		HistoryLength:  len(loaded.events),
	}, nil
}

func (s *Service) LeaseTasks(ctx context.Context, queueName string, workerID string, limit int, leaseTTL time.Duration) ([]TaskLease, error) {
	return s.repo.LeaseTasks(ctx, queueName, workerID, limit, leaseTTL, time.Now().UTC())
}

func (s *Service) HeartbeatTask(ctx context.Context, taskID string, workerID string, extendBy time.Duration) (HeartbeatReply, error) {
	return s.repo.HeartbeatTask(ctx, taskID, workerID, extendBy, time.Now().UTC())
}

func (s *Service) CompleteTask(ctx context.Context, workerID string, taskID string, output json.RawMessage) error {
	return s.withOptimisticRetry(ctx, func() error {
		return s.completeTaskOnce(ctx, workerID, taskID, output)
	})
}

func (s *Service) completeTaskOnce(ctx context.Context, workerID string, taskID string, output json.RawMessage) error {
	ctx, span := s.tracer.Start(ctx, "engine.CompleteTask")
	defer span.End()

	loaded, err := s.loadTaskRun(ctx, taskID)
	if err != nil {
		return err
	}
	current, ok := loaded.state.Tasks[taskID]
	if !ok {
		return ErrTaskNotFound
	}
	if current.Kind == TaskKindActivity && current.Status != TaskStatusLeased {
		return ErrTaskLeaseLost
	}
	if current.Kind == TaskKindActivity && current.LeaseOwner != workerID {
		return ErrTaskLeaseLost
	}
	now := time.Now().UTC()
	seq := loaded.run.Version + 1
	compensation := current.Phase == TaskPhaseCompensation
	event, err := s.makeEvent(loaded.run.ID, &seq, history.EventTaskCompleted, current.StepID, current.ID, history.TaskCompletedPayload{
		TaskID:       current.ID,
		StepID:       current.StepID,
		WorkerID:     workerID,
		Output:       normalizeJSON(output),
		Compensation: compensation,
	}, now)
	if err != nil {
		return err
	}
	events := []history.Event{event}
	if err := applyEvent(&loaded.state, event); err != nil {
		return err
	}
	taskRecord := loaded.tasks[current.ID]
	taskRecord = updateTaskRecord(taskRecord, loaded.state.Tasks[current.ID], now)
	mutations := []TaskMutation{{Action: TaskMutationUpdate, Task: taskRecord}}
	if err := s.postProcess(&loaded, now, &seq, &events, &mutations); err != nil {
		return err
	}
	loaded.run = loaded.state.Run
	loaded.run.Version = int64(len(loaded.events) + len(events))
	loaded.run.UpdatedAt = now
	if loaded.run.Status.Terminal() && loaded.run.CompletedAt == nil {
		completed := now
		loaded.run.CompletedAt = &completed
	}
	if err := s.repo.AppendRunChanges(ctx, loaded.run, loaded.eventsVersion, events, mutations); err != nil {
		return err
	}
	s.taskResults.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "completed")))
	if loaded.run.Status.Terminal() {
		s.runsFinished.Add(ctx, 1, metric.WithAttributes(attribute.String("status", string(loaded.run.Status))))
	}
	return nil
}

func (s *Service) FailTask(ctx context.Context, workerID string, taskID string, taskErr error, retryable bool) error {
	return s.withOptimisticRetry(ctx, func() error {
		return s.failTaskOnce(ctx, workerID, taskID, taskErr, retryable)
	})
}

func (s *Service) failTaskOnce(ctx context.Context, workerID string, taskID string, taskErr error, retryable bool) error {
	ctx, span := s.tracer.Start(ctx, "engine.FailTask")
	defer span.End()

	loaded, err := s.loadTaskRun(ctx, taskID)
	if err != nil {
		return err
	}
	current, ok := loaded.state.Tasks[taskID]
	if !ok {
		return ErrTaskNotFound
	}
	if current.Kind == TaskKindActivity && current.Status != TaskStatusLeased {
		return ErrTaskLeaseLost
	}
	if current.Kind == TaskKindActivity && current.LeaseOwner != workerID {
		return ErrTaskLeaseLost
	}
	now := time.Now().UTC()
	seq := loaded.run.Version + 1
	compensation := current.Phase == TaskPhaseCompensation
	failureMessage := taskErr.Error()
	failedEvent, err := s.makeEvent(loaded.run.ID, &seq, history.EventTaskFailed, current.StepID, current.ID, history.TaskFailedPayload{
		TaskID:       current.ID,
		StepID:       current.StepID,
		WorkerID:     workerID,
		Error:        failureMessage,
		Compensation: compensation,
	}, now)
	if err != nil {
		return err
	}
	events := []history.Event{failedEvent}
	if err := applyEvent(&loaded.state, failedEvent); err != nil {
		return err
	}
	taskRecord := updateTaskRecord(loaded.tasks[current.ID], loaded.state.Tasks[current.ID], now)
	mutations := []TaskMutation{{Action: TaskMutationUpdate, Task: taskRecord}}

	if !compensation && retryable && !loaded.state.CancelRequested {
		step := loaded.def.Steps[current.StepID]
		policy := step.Retry.Normalize()
		if current.Attempt < policy.MaxAttempts {
			nextAttempt := current.Attempt + 1
			nextAt := now.Add(BackoffForAttempt(policy, current.Attempt))
			retryEvent, err := s.makeEvent(loaded.run.ID, &seq, history.EventTaskRetryScheduled, current.StepID, current.ID, history.TaskRetryScheduledPayload{
				TaskID:      current.ID,
				StepID:      current.StepID,
				Attempt:     nextAttempt,
				AvailableAt: nextAt,
				Error:       failureMessage,
			}, now)
			if err != nil {
				return err
			}
			events = append(events, retryEvent)
			if err := applyEvent(&loaded.state, retryEvent); err != nil {
				return err
			}
			taskRecord = loaded.tasks[current.ID]
			taskRecord = updateTaskRecord(taskRecord, loaded.state.Tasks[current.ID], now)
			taskRecord.Attempt = nextAttempt
			taskRecord.AvailableAt = nextAt
			taskRecord.LastError = failureMessage
			mutations[0] = TaskMutation{Action: TaskMutationUpdate, Task: taskRecord}
			loaded.run = loaded.state.Run
			loaded.run.Version = int64(len(loaded.events) + len(events))
			loaded.run.UpdatedAt = now
			return s.repo.AppendRunChanges(ctx, loaded.run, loaded.eventsVersion, events, mutations)
		}
	}

	if compensation {
		loaded.state.Run.Failure = fmt.Sprintf("compensation for step %s failed: %s", current.StepID, failureMessage)
	} else {
		loaded.state.Run.Failure = failureMessage
	}
	if err := s.beginCompensationOrFinish(&loaded, now, &seq, &events, &mutations); err != nil {
		return err
	}
	loaded.run = loaded.state.Run
	loaded.run.Version = int64(len(loaded.events) + len(events))
	loaded.run.UpdatedAt = now
	if loaded.run.Status.Terminal() && loaded.run.CompletedAt == nil {
		completed := now
		loaded.run.CompletedAt = &completed
	}
	if err := s.repo.AppendRunChanges(ctx, loaded.run, loaded.eventsVersion, events, mutations); err != nil {
		return err
	}
	s.taskResults.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "failed")))
	if loaded.run.Status.Terminal() {
		s.runsFinished.Add(ctx, 1, metric.WithAttributes(attribute.String("status", string(loaded.run.Status))))
	}
	return nil
}

func (s *Service) CancelRun(ctx context.Context, runID string, reason string) error {
	return s.withOptimisticRetry(ctx, func() error {
		return s.cancelRunOnce(ctx, runID, reason)
	})
}

func (s *Service) cancelRunOnce(ctx context.Context, runID string, reason string) error {
	loaded, err := s.loadRun(ctx, runID)
	if err != nil {
		return err
	}
	if loaded.run.Status.Terminal() {
		return nil
	}
	now := time.Now().UTC()
	seq := loaded.run.Version + 1
	cancelled, err := s.makeEvent(runID, &seq, history.EventRunCancelRequested, "", "", history.RunCancelRequestedPayload{Reason: reason}, now)
	if err != nil {
		return err
	}
	events := []history.Event{cancelled}
	if err := applyEvent(&loaded.state, cancelled); err != nil {
		return err
	}
	mutations := make([]TaskMutation, 0)
	for _, task := range loaded.taskList {
		stateTask := loaded.state.Tasks[task.ID]
		if stateTask == nil {
			continue
		}
		if stateTask.Status == TaskStatusCompleted || stateTask.Status == TaskStatusCancelled || stateTask.Status == TaskStatusFailed {
			continue
		}
		cancelEvent, err := s.makeEvent(runID, &seq, history.EventTaskCancelled, task.StepID, task.ID, history.TaskCancelledPayload{
			TaskID:       task.ID,
			StepID:       task.StepID,
			Reason:       reason,
			Compensation: stateTask.Phase == TaskPhaseCompensation,
		}, now)
		if err != nil {
			return err
		}
		events = append(events, cancelEvent)
		if err := applyEvent(&loaded.state, cancelEvent); err != nil {
			return err
		}
		updated := updateTaskRecord(task, loaded.state.Tasks[task.ID], now)
		mutations = append(mutations, TaskMutation{Action: TaskMutationUpdate, Task: updated})
	}
	if err := s.beginCompensationOrFinish(&loaded, now, &seq, &events, &mutations); err != nil {
		return err
	}
	loaded.run = loaded.state.Run
	loaded.run.Version = int64(len(loaded.events) + len(events))
	loaded.run.UpdatedAt = now
	if loaded.run.Status.Terminal() && loaded.run.CompletedAt == nil {
		completed := now
		loaded.run.CompletedAt = &completed
	}
	return s.repo.AppendRunChanges(ctx, loaded.run, loaded.eventsVersion, events, mutations)
}

func (s *Service) FireTimer(ctx context.Context, taskID string) error {
	return s.withOptimisticRetry(ctx, func() error {
		return s.fireTimerOnce(ctx, taskID)
	})
}

func (s *Service) fireTimerOnce(ctx context.Context, taskID string) error {
	loaded, err := s.loadTaskRun(ctx, taskID)
	if err != nil {
		return err
	}
	current := loaded.state.Tasks[taskID]
	if current == nil || current.Kind != TaskKindTimer || current.Status != TaskStatusAvailable {
		return nil
	}
	now := time.Now().UTC()
	if current.AvailableAt.After(now) {
		return nil
	}
	seq := loaded.run.Version + 1
	event, err := s.makeEvent(loaded.run.ID, &seq, history.EventTimerFired, current.StepID, current.ID, history.TimerFiredPayload{
		TaskID: current.ID,
		StepID: current.StepID,
	}, now)
	if err != nil {
		return err
	}
	events := []history.Event{event}
	if err := applyEvent(&loaded.state, event); err != nil {
		return err
	}
	taskRecord := updateTaskRecord(loaded.tasks[current.ID], loaded.state.Tasks[current.ID], now)
	mutations := []TaskMutation{{Action: TaskMutationUpdate, Task: taskRecord}}
	if err := s.postProcess(&loaded, now, &seq, &events, &mutations); err != nil {
		return err
	}
	loaded.run = loaded.state.Run
	loaded.run.Version = int64(len(loaded.events) + len(events))
	loaded.run.UpdatedAt = now
	if loaded.run.Status.Terminal() && loaded.run.CompletedAt == nil {
		completed := now
		loaded.run.CompletedAt = &completed
	}
	return s.repo.AppendRunChanges(ctx, loaded.run, loaded.eventsVersion, events, mutations)
}

func (s *Service) RecoverExpiredLease(ctx context.Context, taskID string) error {
	loaded, err := s.loadTaskRun(ctx, taskID)
	if err != nil {
		return err
	}
	current := loaded.state.Tasks[taskID]
	if current == nil || current.Status != TaskStatusLeased || current.LeaseExpiresAt == nil || current.LeaseExpiresAt.After(time.Now().UTC()) {
		return nil
	}
	return s.FailTask(ctx, current.LeaseOwner, taskID, fmt.Errorf("lease expired"), true)
}

func (s *Service) Sweep(ctx context.Context, timerLimit int, leaseLimit int) error {
	timers, err := s.repo.AvailableTimers(ctx, timerLimit, time.Now().UTC())
	if err != nil {
		return err
	}
	for _, timerTask := range timers {
		if err := s.FireTimer(ctx, timerTask.ID); err != nil {
			s.logger.Error("fire timer", "task_id", timerTask.ID, "error", err)
		}
	}
	expired, err := s.repo.ExpiredLeases(ctx, leaseLimit, time.Now().UTC())
	if err != nil {
		return err
	}
	for _, task := range expired {
		if err := s.RecoverExpiredLease(ctx, task.ID); err != nil {
			s.logger.Error("recover expired lease", "task_id", task.ID, "error", err)
		}
	}
	return nil
}

type loadedRun struct {
	run           RunRecord
	def           workflow.Definition
	events        []history.Event
	state         RunState
	taskList      []TaskRecord
	tasks         map[string]TaskRecord
	eventsVersion int64
}

func (s *Service) loadRun(ctx context.Context, runID string) (loadedRun, error) {
	run, err := s.repo.GetRun(ctx, runID)
	if err != nil {
		return loadedRun{}, err
	}
	def, ok := s.registry.Definition(run.WorkflowName)
	if !ok {
		return loadedRun{}, ErrWorkflowNotFound
	}
	events, err := s.repo.GetHistory(ctx, runID)
	if err != nil {
		return loadedRun{}, err
	}
	state, err := Replay(def, run, events)
	if err != nil {
		return loadedRun{}, err
	}
	taskList, err := s.repo.ListTasks(ctx, runID)
	if err != nil {
		return loadedRun{}, err
	}
	taskMap := make(map[string]TaskRecord, len(taskList))
	for _, task := range taskList {
		taskMap[task.ID] = task
	}
	return loadedRun{
		run:           run,
		def:           def,
		events:        events,
		state:         state,
		taskList:      taskList,
		tasks:         taskMap,
		eventsVersion: run.Version,
	}, nil
}

func (s *Service) loadTaskRun(ctx context.Context, taskID string) (loadedRun, error) {
	task, err := s.repo.GetTask(ctx, taskID)
	if err != nil {
		return loadedRun{}, err
	}
	loaded, err := s.loadRun(ctx, task.RunID)
	if err != nil {
		return loadedRun{}, err
	}
	if _, ok := loaded.tasks[taskID]; !ok {
		loaded.tasks[taskID] = task
		loaded.taskList = append(loaded.taskList, task)
	}
	return loaded, nil
}

func (s *Service) scheduleForwardReady(run RunRecord, def workflow.Definition, state *RunState, seq *int64, now time.Time) ([]history.Event, []TaskRecord, error) {
	events := make([]history.Event, 0)
	tasks := make([]TaskRecord, 0)
	for _, stepID := range def.StepIDs() {
		step := state.Steps[stepID]
		if step.ForwardTaskID != "" || step.ForwardStatus == TaskStatusCompleted || step.ForwardStatus == TaskStatusCancelled || step.ForwardStatus == TaskStatusFailed {
			continue
		}
		if !dependenciesCompleted(step.Definition, *state) {
			continue
		}
		task, event, err := s.newTask(run, *state, step.Definition, TaskPhaseForward, 1, now, seq)
		if err != nil {
			return nil, nil, err
		}
		events = append(events, event)
		tasks = append(tasks, task)
		if err := applyEvent(state, event); err != nil {
			return nil, nil, err
		}
	}
	return events, tasks, nil
}

func (s *Service) postProcess(loaded *loadedRun, now time.Time, seq *int64, events *[]history.Event, mutations *[]TaskMutation) error {
	if loaded.state.Compensating {
		return s.advanceCompensation(loaded, now, seq, events, mutations)
	}
	readyEvents, readyTasks, err := s.scheduleForwardReady(loaded.run, loaded.def, &loaded.state, seq, now)
	if err != nil {
		return err
	}
	*events = append(*events, readyEvents...)
	for _, task := range readyTasks {
		*mutations = append(*mutations, TaskMutation{Action: TaskMutationInsert, Task: task})
		loaded.tasks[task.ID] = task
	}
	if allForwardCompleted(loaded.def, loaded.state) && !hasOpenForwardTasks(loaded.state) {
		output, err := buildRunOutput(loaded.state)
		if err != nil {
			return err
		}
		completed, err := s.makeEvent(loaded.run.ID, seq, history.EventRunCompleted, "", "", history.RunCompletedPayload{Output: output}, now)
		if err != nil {
			return err
		}
		*events = append(*events, completed)
		return applyEvent(&loaded.state, completed)
	}
	return nil
}

func (s *Service) beginCompensationOrFinish(loaded *loadedRun, now time.Time, seq *int64, events *[]history.Event, mutations *[]TaskMutation) error {
	if next, ok := nextCompensationStep(loaded.state); ok {
		reason := loaded.state.Run.Failure
		if loaded.state.CancelRequested {
			reason = loaded.state.Run.CancelReason
		}
		compensating, err := s.makeEvent(loaded.run.ID, seq, history.EventRunCompensating, "", "", history.RunCompensatingPayload{Reason: reason}, now)
		if err != nil {
			return err
		}
		*events = append(*events, compensating)
		if err := applyEvent(&loaded.state, compensating); err != nil {
			return err
		}
		task, event, err := s.newCompensationTask(loaded.run, loaded.state, next, now, seq)
		if err != nil {
			return err
		}
		*events = append(*events, event)
		if err := applyEvent(&loaded.state, event); err != nil {
			return err
		}
		*mutations = append(*mutations, TaskMutation{Action: TaskMutationInsert, Task: task})
		loaded.tasks[task.ID] = task
		return nil
	}
	return s.finishRunWithoutCompensation(loaded, now, seq, events)
}

func (s *Service) advanceCompensation(loaded *loadedRun, now time.Time, seq *int64, events *[]history.Event, mutations *[]TaskMutation) error {
	if hasOpenCompensationTasks(loaded.state) {
		return nil
	}
	if next, ok := nextCompensationStep(loaded.state); ok {
		task, event, err := s.newCompensationTask(loaded.run, loaded.state, next, now, seq)
		if err != nil {
			return err
		}
		*events = append(*events, event)
		if err := applyEvent(&loaded.state, event); err != nil {
			return err
		}
		*mutations = append(*mutations, TaskMutation{Action: TaskMutationInsert, Task: task})
		loaded.tasks[task.ID] = task
		return nil
	}
	return s.finishRunWithoutCompensation(loaded, now, seq, events)
}

func (s *Service) finishRunWithoutCompensation(loaded *loadedRun, now time.Time, seq *int64, events *[]history.Event) error {
	if loaded.state.CancelRequested {
		cancelled, err := s.makeEvent(loaded.run.ID, seq, history.EventRunCancelled, "", "", history.RunCancelledPayload{Reason: loaded.state.Run.CancelReason}, now)
		if err != nil {
			return err
		}
		*events = append(*events, cancelled)
		return applyEvent(&loaded.state, cancelled)
	}
	failed, err := s.makeEvent(loaded.run.ID, seq, history.EventRunFailed, "", "", history.RunFailedPayload{Error: loaded.state.Run.Failure}, now)
	if err != nil {
		return err
	}
	*events = append(*events, failed)
	return applyEvent(&loaded.state, failed)
}

func (s *Service) newTask(run RunRecord, state RunState, step workflow.Step, phase TaskPhase, attempt int, now time.Time, seq *int64) (TaskRecord, history.Event, error) {
	input, dependencies, err := buildTaskInput(run, state, step, phase == TaskPhaseCompensation, attempt)
	if err != nil {
		return TaskRecord{}, history.Event{}, err
	}
	kind := TaskKind(step.Kind)
	timeout := step.Timeout
	if timeout <= 0 && kind == TaskKindActivity {
		timeout = 15 * time.Second
	}
	queueName := step.Queue
	activity := step.Activity
	availableAt := now
	maxAttempts := step.Retry.Normalize().MaxAttempts
	if kind == TaskKindTimer {
		queueName = timerQueueName
		activity = ""
		availableAt = now.Add(step.Delay)
		maxAttempts = 1
	}
	task := TaskRecord{
		ID:           uuid.NewString(),
		RunID:        run.ID,
		WorkflowName: run.WorkflowName,
		StepID:       step.ID,
		Kind:         kind,
		Phase:        phase,
		ActivityName: activity,
		QueueName:    queueName,
		Status:       TaskStatusAvailable,
		Attempt:      attempt,
		MaxAttempts:  maxAttempts,
		AvailableAt:  availableAt,
		Timeout:      timeout,
		Input:        input,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	event, err := s.makeEvent(run.ID, seq, history.EventTaskScheduled, step.ID, task.ID, history.TaskScheduledPayload{
		TaskID:         task.ID,
		StepID:         step.ID,
		Kind:           string(task.Kind),
		Queue:          task.QueueName,
		Activity:       task.ActivityName,
		Attempt:        task.Attempt,
		MaxAttempts:    task.MaxAttempts,
		TimeoutSeconds: int(task.Timeout.Seconds()),
		AvailableAt:    task.AvailableAt,
		Input:          task.Input,
		Dependencies:   dependencies,
		Compensation:   phase == TaskPhaseCompensation,
	}, now)
	return task, event, err
}

func (s *Service) newCompensationTask(run RunRecord, state RunState, step workflow.Step, now time.Time, seq *int64) (TaskRecord, history.Event, error) {
	compStep := step
	compStep.Kind = workflow.StepKindActivity
	compStep.Activity = step.Compensation
	if compStep.CompensationQueue != "" {
		compStep.Queue = compStep.CompensationQueue
	}
	return s.newTask(run, state, compStep, TaskPhaseCompensation, 1, now, seq)
}

func (s *Service) makeEvent(runID string, seq *int64, eventType history.EventType, stepID string, taskID string, payload any, now time.Time) (history.Event, error) {
	event, err := history.New(runID, eventType, stepID, taskID, payload, now)
	if err != nil {
		return history.Event{}, err
	}
	event.Sequence = *seq
	*seq++
	return event, nil
}

func buildTaskInput(run RunRecord, state RunState, step workflow.Step, compensation bool, attempt int) (json.RawMessage, map[string]json.RawMessage, error) {
	dependencies := make(map[string]json.RawMessage)
	for _, dep := range step.Dependencies {
		if out, ok := state.Outputs[dep]; ok {
			dependencies[dep] = out
		}
	}
	if compensation {
		if out, ok := state.Outputs[step.ID]; ok {
			dependencies[step.ID] = out
		}
	}
	body, err := json.Marshal(workflow.ActivityInput{
		RunID:        run.ID,
		Workflow:     run.WorkflowName,
		StepID:       step.ID,
		Attempt:      attempt,
		Compensation: compensation,
		RunInput:     normalizeJSON(run.Input),
		Dependencies: dependencies,
		Metadata:     cloneMetadata(step.Metadata),
	})
	if err != nil {
		return nil, nil, err
	}
	return body, dependencies, nil
}

func updateTaskRecord(task TaskRecord, state *TaskState, now time.Time) TaskRecord {
	task.Status = state.Status
	task.Attempt = state.Attempt
	task.AvailableAt = state.AvailableAt
	task.LeaseOwner = state.LeaseOwner
	task.LeaseExpiresAt = state.LeaseExpiresAt
	task.HeartbeatAt = state.HeartbeatAt
	task.LastError = state.Error
	task.UpdatedAt = now
	if state.Status != TaskStatusLeased {
		task.LeaseOwner = ""
		task.LeaseExpiresAt = nil
	}
	return task
}

func buildRunOutput(state RunState) (json.RawMessage, error) {
	if len(state.Outputs) == 0 {
		return nullJSON, nil
	}
	return json.Marshal(state.Outputs)
}

func hasOpenForwardTasks(state RunState) bool {
	for _, task := range state.Tasks {
		if task.Phase != TaskPhaseForward {
			continue
		}
		if task.Status == TaskStatusAvailable || task.Status == TaskStatusLeased {
			return true
		}
	}
	return false
}

func hasOpenCompensationTasks(state RunState) bool {
	for _, task := range state.Tasks {
		if task.Phase != TaskPhaseCompensation {
			continue
		}
		if task.Status == TaskStatusAvailable || task.Status == TaskStatusLeased {
			return true
		}
	}
	return false
}

func nextCompensationStep(state RunState) (workflow.Step, bool) {
	for i := len(state.CompletedOrder) - 1; i >= 0; i-- {
		stepID := state.CompletedOrder[i]
		stepState := state.Steps[stepID]
		if stepState == nil {
			continue
		}
		if stepState.Definition.Compensation == "" || stepState.CompensationCompleted || stepState.CompensationScheduled {
			continue
		}
		return stepState.Definition, true
	}
	return workflow.Step{}, false
}

func allForwardCompleted(def workflow.Definition, state RunState) bool {
	for _, stepID := range def.StepIDs() {
		step := state.Steps[stepID]
		if step == nil || step.ForwardStatus != TaskStatusCompleted {
			return false
		}
	}
	return true
}

func normalizeJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nullJSON
	}
	return raw
}

func jsonEqual(left, right json.RawMessage) bool {
	return string(normalizeJSON(left)) == string(normalizeJSON(right))
}

func (s *Service) Close() error {
	if closer, ok := s.repo.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

func (s *Service) withOptimisticRetry(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		err = fn()
		if !isVersionConflict(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 10 * time.Millisecond):
		}
	}
	return err
}

func isVersionConflict(err error) bool {
	return err != nil && strings.Contains(err.Error(), "run version conflict")
}
