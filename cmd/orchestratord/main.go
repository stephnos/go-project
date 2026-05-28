package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"strings"
	"syscall"
	"time"

	orderfulfillment "github.com/spapa/orchid/examples/order_fulfillment"
	"github.com/spapa/orchid/internal/app"
	"github.com/spapa/orchid/internal/queue"
	"github.com/spapa/orchid/pkg/workflow"
)

func main() {
	var (
		storage      = flag.String("storage", "sqlite", "storage backend: sqlite or postgres")
		listen       = flag.String("listen", ":8080", "HTTP listen address")
		dbPath       = flag.String("db", "orchid.db", "SQLite database path")
		postgresDSN  = flag.String("postgres-dsn", "", "Postgres connection string")
		embeddedWork = flag.Bool("with-workers", true, "run embedded workers")
		queues       = flag.String("queues", "default,operations,payments,shipping", "comma-separated worker queues")
		concurrency  = flag.Int("concurrency", 2, "worker concurrency per queue")
	)
	flag.Parse()

	registry := workflow.NewRegistry()
	if err := orderfulfillment.Register(registry); err != nil {
		log.Fatalf("register example workflows: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runtime, err := app.New(ctx, app.Config{
		ServiceName:       "orchestratord",
		ListenAddr:        *listen,
		StorageDriver:     *storage,
		DBPath:            *dbPath,
		PostgresDSN:       *postgresDSN,
		Registry:          registry,
		EnableWorkers:     *embeddedWork,
		WorkerQueues:      splitQueues(*queues),
		WorkerConcurrency: *concurrency,
		Lease: queue.LeaseConfig{
			TTL:              8 * time.Second,
			HeartbeatEvery:   2 * time.Second,
			RecoveryInterval: 500 * time.Millisecond,
		},
	})
	if err != nil {
		log.Fatalf("boot runtime: %v", err)
	}

	if err := runtime.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("runtime stopped: %v", err)
	}
}

func splitQueues(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
