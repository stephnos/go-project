# Orchid

Orchid is a durable workflow engine written in Go. It accepts declarative workflow definitions, persists every state transition to an append-only history log, leases work to queue-specific workers, survives process restarts, and can replay workflow state deterministically from history.

The design is intentionally small enough to read in a sitting, but deep enough to show the engineering signals interviewers actually care about:

- deterministic replay over event history
- SQLite and Postgres backends with transactional state transitions
- worker leasing, heartbeats, retries, and lease recovery
- timers, fan-out/fan-in dependencies, and compensation steps
- operator UX through HTTP APIs, a browser-based run inspector, and a text-first CLI
- structured telemetry and `pprof` hooks

## Quick Start

Start the server with embedded workers:

```bash
go run ./cmd/orchestratord -listen :8080 -db orchid.db -with-workers
```

Start the same engine on Postgres:

```bash
go run ./cmd/orchestratord -storage postgres -postgres-dsn "postgres://localhost:5432/orchid?sslmode=disable" -with-workers
```

Kick off the built-in order fulfillment workflow:

```bash
go run ./cmd/orchctl start
```

Inspect the latest runs:

```bash
go run ./cmd/orchctl list
go run ./cmd/orchctl get <run-id>
go run ./cmd/orchctl history <run-id>
go run ./cmd/orchctl replay <run-id>
```

Open the browser inspector at [http://127.0.0.1:8080/](http://127.0.0.1:8080/) to inspect runs, replay state, task leases, and the workflow graph.

## What Makes It Interesting

### Event-sourced orchestration

Every meaningful transition is captured in `history_events`, including scheduling, leasing, heartbeats, retries, completion, cancellation, and terminal workflow status. Replay is not a side feature; it is the main mechanism for reconstructing workflow state.

### Durable worker execution

Tasks live in durable storage, not in memory. Workers lease tasks for a bounded period, renew those leases with heartbeats, and the engine can recover expired leases after a crash or worker death.

### Storage evolution

SQLite keeps the single-node story simple. Postgres adds stronger concurrency primitives, `FOR UPDATE SKIP LOCKED` leasing, and a more realistic path toward multi-instance operation.

### Compensation support

Forward steps can attach compensating activities. When a workflow fails after partial success, Orchid enters a compensation phase and unwinds completed steps in reverse completion order.

### Practical operator workflow

The API exposes run creation, inspection, history browsing, cancellation, and replay. `orchctl` mirrors those operations, while the embedded web inspector gives you a live view of run state, graph structure, task status, and event history.

## Example Workflow

The repository ships with `order_fulfillment`, a workflow that exercises:

- dependency-driven fan-out/fan-in
- retryable payment work
- a timer-based fraud window
- shipping failure compensation
- operator replay and history inspection

## Layout

- `cmd/orchestratord`: HTTP server and optional embedded workers
- `cmd/orchctl`: CLI for start, inspect, history, replay, and cancel
- `internal/engine`: replay reducer and orchestration service
- `internal/store/postgres`: Postgres persistence adapter and migrations
- `internal/store/sqlite`: SQLite persistence adapter
- `internal/worker`: leasing and heartbeat-driven activity execution
- `internal/api/static`: embedded browser UI for run inspection
- `pkg/workflow`: public workflow DSL and activity registry
- `pkg/client`: API client
- `examples/order_fulfillment`: realistic demo workflow and activities
- `docs/architecture.md`: deeper design notes
- `docs/google-interview-walkthrough.md`: file-by-file tradeoff walkthrough

## Development

```bash
make test
make bench
make run
make run-postgres
```