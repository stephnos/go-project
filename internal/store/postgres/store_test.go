package postgres

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	orderfulfillment "github.com/spapa/orchid/examples/order_fulfillment"
	"github.com/spapa/orchid/internal/engine"
	"github.com/spapa/orchid/internal/queue"
	"github.com/spapa/orchid/internal/worker"
	"github.com/spapa/orchid/pkg/workflow"
)

func TestStoreRoundTrip(t *testing.T) {
	dsn := os.Getenv("ORCHID_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set ORCHID_TEST_POSTGRES_DSN to run Postgres integration tests")
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	registry := workflow.NewRegistry()
	if err := orderfulfillment.Register(registry); err != nil {
		t.Fatalf("register workflow: %v", err)
	}

	store, err := New(dsn, logger)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.Initialize(context.Background()); err != nil {
		t.Fatalf("initialize store: %v", err)
	}
	if _, err := store.db.Exec(`TRUNCATE history_events, tasks, runs`); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}

	service := engine.NewService(store, registry, logger, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startWorkers(ctx, service, registry, "default", "operations", "payments", "shipping")
	startSweepLoop(ctx, service)

	run, err := service.StartRun(ctx, "order_fulfillment", marshal(orderfulfillment.ExampleInput()), map[string]string{"backend": "postgres"})
	if err != nil {
		t.Fatalf("start run: %v", err)
	}

	view := waitForTerminal(t, ctx, service, run.ID, 8*time.Second)
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
}

func startWorkers(ctx context.Context, service *engine.Service, registry *workflow.Registry, queues ...string) {
	for _, queueName := range queues {
		pool := worker.New(service, registry, slog.New(slog.NewTextHandler(io.Discard, nil)), worker.Config{
			WorkerID:     "pg-worker-" + queueName,
			Queue:        queueName,
			Concurrency:  1,
			PollInterval: 10 * time.Millisecond,
			Lease: queue.LeaseConfig{
				TTL:              400 * time.Millisecond,
				HeartbeatEvery:   80 * time.Millisecond,
				RecoveryInterval: 25 * time.Millisecond,
			},
		})
		go func(pool *worker.Pool) {
			_ = pool.Run(ctx)
		}(pool)
	}
}

func startSweepLoop(ctx context.Context, service *engine.Service) {
	go func() {
		ticker := time.NewTicker(25 * time.Millisecond)
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
