package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/spapa/orchid/internal/engine"
	"github.com/spapa/orchid/internal/queue"
	"github.com/spapa/orchid/pkg/workflow"
)

type Config struct {
	WorkerID     string
	Queue        string
	Concurrency  int
	PollInterval time.Duration
	Lease        queue.LeaseConfig
}

type Pool struct {
	service  *engine.Service
	registry *workflow.Registry
	logger   *slog.Logger
	cfg      Config
}

func New(service *engine.Service, registry *workflow.Registry, logger *slog.Logger, cfg Config) *Pool {
	if logger == nil {
		logger = slog.Default()
	}
	cfg.Lease = cfg.Lease.Normalize()
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 150 * time.Millisecond
	}
	if cfg.Queue == "" {
		cfg.Queue = "default"
	}
	return &Pool{
		service:  service,
		registry: registry,
		logger:   logger.With("worker_id", cfg.WorkerID, "queue", cfg.Queue),
		cfg:      cfg,
	}
}

func (p *Pool) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for i := 0; i < p.cfg.Concurrency; i++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			p.loop(ctx, slot)
		}(i)
	}
	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}

func (p *Pool) loop(ctx context.Context, slot int) {
	for {
		if ctx.Err() != nil {
			return
		}
		tasks, err := p.service.LeaseTasks(ctx, p.cfg.Queue, p.cfg.WorkerID, 1, p.cfg.Lease.TTL)
		if err != nil {
			p.logger.Error("lease tasks", "slot", slot, "error", err)
			time.Sleep(p.cfg.PollInterval)
			continue
		}
		if len(tasks) == 0 {
			time.Sleep(p.cfg.PollInterval)
			continue
		}
		for _, task := range tasks {
			p.execute(ctx, task)
		}
	}
}

func (p *Pool) execute(parent context.Context, task engine.TaskLease) {
	handler, ok := p.registry.Handler(task.ActivityName)
	if !ok {
		if err := p.service.FailTask(parent, p.cfg.WorkerID, task.ID, engine.ErrHandlerNotFound, false); err != nil {
			p.logger.Error("report missing handler failure", "task_id", task.ID, "error", err)
		}
		return
	}
	taskCtx, cancel := context.WithCancel(parent)
	defer cancel()

	input := workflow.ActivityInput{}
	if err := json.Unmarshal(task.Input, &input); err != nil {
		if reportErr := p.service.FailTask(parent, p.cfg.WorkerID, task.ID, err, false); reportErr != nil {
			p.logger.Error("report input decode failure", "task_id", task.ID, "error", reportErr)
		}
		return
	}

	heartbeatDone := make(chan struct{})
	leaseLost := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(p.cfg.Lease.HeartbeatEvery)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatDone:
				return
			case <-taskCtx.Done():
				return
			case <-ticker.C:
				reply, err := p.service.HeartbeatTask(parent, task.ID, p.cfg.WorkerID, p.cfg.Lease.TTL)
				if err != nil {
					leaseLost <- err
					cancel()
					return
				}
				if reply.CancelRequested {
					leaseLost <- engine.ErrRunCancelledSignal
					cancel()
					return
				}
			}
		}
	}()

	result, err := handler(taskCtx, input)
	close(heartbeatDone)

	select {
	case hbErr := <-leaseLost:
		if hbErr != nil && err == nil {
			err = hbErr
		}
	default:
	}

	if err != nil {
		retryable := !workflow.IsNonRetryable(err) && !errors.Is(err, context.Canceled) && !errors.Is(err, engine.ErrRunCancelledSignal)
		if reportErr := p.service.FailTask(parent, p.cfg.WorkerID, task.ID, err, retryable); reportErr != nil {
			p.logger.Error("report task failure", "task_id", task.ID, "error", reportErr)
		}
		return
	}
	output, marshalErr := marshalOutput(result)
	if marshalErr != nil {
		if reportErr := p.service.FailTask(parent, p.cfg.WorkerID, task.ID, marshalErr, false); reportErr != nil {
			p.logger.Error("report marshal failure", "task_id", task.ID, "error", reportErr)
		}
		return
	}
	if err := p.service.CompleteTask(parent, p.cfg.WorkerID, task.ID, output); err != nil {
		p.logger.Error("report task completion", "task_id", task.ID, "error", err)
	}
}

func marshalOutput(value any) (json.RawMessage, error) {
	switch typed := value.(type) {
	case nil:
		return json.RawMessage("null"), nil
	case json.RawMessage:
		if len(typed) == 0 {
			return json.RawMessage("null"), nil
		}
		return typed, nil
	case []byte:
		if len(typed) == 0 {
			return json.RawMessage("null"), nil
		}
		return typed, nil
	case string:
		body, err := json.Marshal(map[string]string{"value": typed})
		return body, err
	default:
		body, err := json.Marshal(typed)
		return body, err
	}
}
