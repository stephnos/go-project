package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/spapa/orchid/internal/api"
	"github.com/spapa/orchid/internal/engine"
	"github.com/spapa/orchid/internal/observability"
	"github.com/spapa/orchid/internal/queue"
	pgstore "github.com/spapa/orchid/internal/store/postgres"
	sqlitestore "github.com/spapa/orchid/internal/store/sqlite"
	"github.com/spapa/orchid/internal/worker"
	"github.com/spapa/orchid/pkg/workflow"
)

type Config struct {
	ServiceName       string
	ListenAddr        string
	StorageDriver     string
	DBPath            string
	PostgresDSN       string
	Registry          *workflow.Registry
	EnableWorkers     bool
	WorkerQueues      []string
	WorkerConcurrency int
	Lease             queue.LeaseConfig
	Logger            *slog.Logger
}

type Runtime struct {
	Service     *engine.Service
	HTTPServer  *http.Server
	API         *api.Server
	Logger      *slog.Logger
	workers     []*worker.Pool
	shutdownObs func(context.Context) error
}

type backend interface {
	engine.Repository
	Initialize(context.Context) error
	Close() error
}

func New(ctx context.Context, cfg Config) (*Runtime, error) {
	if cfg.Registry == nil {
		cfg.Registry = workflow.NewRegistry()
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "orchid"
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8080"
	}
	if cfg.StorageDriver == "" {
		cfg.StorageDriver = "sqlite"
	}
	stack, shutdownObs, err := observability.New(ctx, cfg.ServiceName)
	if err != nil {
		return nil, err
	}
	logger := cfg.Logger
	if logger == nil {
		logger = stack.Logger
	}

	store, err := openStore(cfg, logger)
	if err != nil {
		return nil, err
	}
	if err := store.Initialize(ctx); err != nil {
		return nil, err
	}
	service := engine.NewService(store, cfg.Registry, logger, stack.Tracer, stack.MeterProvider.Meter(cfg.ServiceName))
	apiServer := api.New(service, cfg.Registry, logger)
	httpServer := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: apiServer.Handler(),
	}

	runtime := &Runtime{
		Service:     service,
		HTTPServer:  httpServer,
		API:         apiServer,
		Logger:      logger,
		shutdownObs: shutdownObs,
	}
	if cfg.EnableWorkers {
		queues := cfg.WorkerQueues
		if len(queues) == 0 {
			queues = []string{"default"}
		}
		for _, queueName := range queues {
			pool := worker.New(service, cfg.Registry, logger, worker.Config{
				WorkerID:     "embedded-" + queueName,
				Queue:        queueName,
				Concurrency:  cfg.WorkerConcurrency,
				PollInterval: 150 * time.Millisecond,
				Lease:        cfg.Lease,
			})
			runtime.workers = append(runtime.workers, pool)
		}
	}
	return runtime, nil
}

func openStore(cfg Config, logger *slog.Logger) (backend, error) {
	switch cfg.StorageDriver {
	case "sqlite":
		path := cfg.DBPath
		if path == "" {
			path = "orchid.db"
		}
		return sqlitestore.New(path, logger)
	case "postgres":
		if cfg.PostgresDSN == "" {
			return nil, fmt.Errorf("postgres storage selected but postgres dsn is empty")
		}
		return pgstore.New(cfg.PostgresDSN, logger)
	default:
		return nil, fmt.Errorf("unsupported storage driver %q", cfg.StorageDriver)
	}
}

func (r *Runtime) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		if err := r.HTTPServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go r.runMaintenance(ctx)
	for _, pool := range r.workers {
		go func(pool *worker.Pool) {
			_ = pool.Run(ctx)
		}(pool)
	}

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return r.Close(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func (r *Runtime) Close(ctx context.Context) error {
	if err := r.HTTPServer.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	if err := r.Service.Close(); err != nil {
		return err
	}
	if r.shutdownObs != nil {
		return r.shutdownObs(ctx)
	}
	return nil
}

func (r *Runtime) runMaintenance(ctx context.Context) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.Service.Sweep(ctx, 32, 32); err != nil {
				r.Logger.Error("maintenance sweep", "error", err)
			}
		}
	}
}
