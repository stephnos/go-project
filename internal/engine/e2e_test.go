package engine_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	orderfulfillment "github.com/spapa/orchid/examples/order_fulfillment"
	"github.com/spapa/orchid/internal/engine"
	"github.com/spapa/orchid/internal/queue"
	sqlitestore "github.com/spapa/orchid/internal/store/sqlite"
	"github.com/spapa/orchid/internal/worker"
	"github.com/spapa/orchid/pkg/workflow"
)

func TestWorkflowCompletesEndToEnd(t *testing.T) {
	t.Parallel()

	service, registry, cleanup := newTestService(t, filepath.Join(t.TempDir(), "orchid.db"))
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startSweepLoop(ctx, t, service)
	startWorkers(ctx, service, registry, "default", "operations", "payments", "shipping")

	run, err := service.StartRun(ctx, "order_fulfillment", marshal(orderfulfillment.ExampleInput()), map[string]string{"test": t.Name()})
	if err != nil {
		t.Fatalf("start run: %v", err)
	}

	view := waitForTerminal(t, ctx, service, run.ID, 6*time.Second)
	if view.Run.Status != engine.RunStatusCompleted {
		t.Fatalf("expected completed run, got %s", view.Run.Status)
	}

	report, err := service.ReplayRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("replay report: %v", err)
	}
	if !report.SummaryMatches {
		t.Fatalf("expected replay summary match")
	}
	if report.HistoryLength < 8 {
		t.Fatalf("expected non-trivial history length, got %d", report.HistoryLength)
	}
}

func TestRunRecoversAfterRestart(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "orchid.db")
	service, _, cleanup := newTestService(t, dbPath)
	run, err := service.StartRun(context.Background(), "order_fulfillment", marshal(orderfulfillment.ExampleInput()), nil)
	if err != nil {
		t.Fatalf("start run: %v", err)
	}

	leased, err := service.LeaseTasks(context.Background(), "default", "bootstrap-worker", 1, 3*time.Second)
	if err != nil {
		t.Fatalf("lease first task: %v", err)
	}
	if len(leased) != 1 {
		t.Fatalf("expected one leased task, got %d", len(leased))
	}
	if err := service.CompleteTask(context.Background(), "bootstrap-worker", leased[0].ID, marshal(map[string]any{"validated": true})); err != nil {
		t.Fatalf("complete first task: %v", err)
	}
	cleanup()

	restarted, restartedRegistry, restartedCleanup := newTestService(t, dbPath)
	defer restartedCleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startSweepLoop(ctx, t, restarted)
	startWorkers(ctx, restarted, restartedRegistry, "default", "operations", "payments", "shipping")

	view := waitForTerminal(t, ctx, restarted, run.ID, 6*time.Second)
	if view.Run.Status != engine.RunStatusCompleted {
		t.Fatalf("expected completed run after restart, got %s", view.Run.Status)
	}
}

func newTestService(t *testing.T, dbPath string) (*engine.Service, *workflow.Registry, func()) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	registry := workflow.NewRegistry()
	if err := orderfulfillment.Register(registry); err != nil {
		t.Fatalf("register example workflow: %v", err)
	}
	store, err := sqlitestore.New(dbPath, logger)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	if err := store.Initialize(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}
	service := engine.NewService(store, registry, logger, nil, nil)
	cleanup := func() {
		_ = service.Close()
	}
	return service, registry, cleanup
}

func startWorkers(ctx context.Context, service *engine.Service, registry *workflow.Registry, queues ...string) {
	for _, queueName := range queues {
		pool := worker.New(service, registry, slog.New(slog.NewTextHandler(io.Discard, nil)), worker.Config{
			WorkerID:     "worker-" + queueName,
			Queue:        queueName,
			Concurrency:  1,
			PollInterval: 10 * time.Millisecond,
			Lease: queue.LeaseConfig{
				TTL:              250 * time.Millisecond,
				HeartbeatEvery:   50 * time.Millisecond,
				RecoveryInterval: 25 * time.Millisecond,
			},
		})
		go func(pool *worker.Pool) {
			_ = pool.Run(ctx)
		}(pool)
	}
}

func startSweepLoop(ctx context.Context, t *testing.T, service *engine.Service) {
	t.Helper()
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = service.Sweep(ctx, 16, 16)
			}
		}
	}()
}

func waitForTerminal(t *testing.T, ctx context.Context, service *engine.Service, runID string, timeout time.Duration) engine.RunView {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		view, err := service.GetRun(ctx, runID)
		if err != nil {
			t.Fatalf("get run: %v", err)
		}
		if view.Run.Status.Terminal() {
			return view
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("run %s did not reach terminal state in time", runID)
	return engine.RunView{}
}

func marshal(value any) []byte {
	body, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return body
}
