# Orchid: Google Interview Walkthrough

## The One-Sentence Pitch

Orchid is a durable workflow engine in Go that persists every state transition to an append-only history log, reconstructs workflow state deterministically through replay, and executes activities through leased workers with retries, timers, and compensation.

This is not meant to be a Temporal clone. The point of the project is to show strong systems engineering judgment in a repository that is still small enough to explain end to end.

## What An Interviewer Should Notice

The project is designed to show:

- concurrency and cancellation discipline in worker execution
- durable state transitions rather than in-memory orchestration
- event-sourced replay as a correctness mechanism, not a logging side effect
- pragmatic storage abstraction boundaries
- realistic failure modes: lease expiry, retry, restart, and compensation
- operator ergonomics through CLI, HTTP APIs, and a browser inspector

## File-By-File Walkthrough

### `pkg/workflow/workflow.go`

This is the authoring surface for workflows and activities.

Why it matters:

- it keeps the public API intentionally small
- workflow definitions are declarative DAGs, which makes replay deterministic
- retry policy, dependencies, timers, and compensation are all explicit in the model

Tradeoff:

I chose declarative workflow structure over executing arbitrary user workflow code during replay. That gives up some flexibility, but it avoids a whole class of nondeterminism problems and keeps the system explainable.

### `internal/history/events.go`

This file defines the event vocabulary for the engine.

Why it matters:

- it makes state transitions explicit and inspectable
- it keeps the reducer honest: replay only works if the event model is precise
- it creates a durable audit trail for scheduling, leasing, retries, cancellations, and completion

Tradeoff:

The event set is intentionally verbose. That costs storage, but the clarity is worth it for debugging and replay correctness.

### `internal/engine/reducer.go`

This is the deterministic replay core.

Why it matters:

- it rebuilds step state, task state, outputs, cancellation, and compensation from history
- it proves the system is not trusting mutable summaries blindly
- it gives the browser inspector and replay endpoint a derived source of truth

Tradeoff:

The reducer is more work than mutating a row in place, but it is the strongest technical signal in the repo. It turns the system into something you can reason about after crashes.

### `internal/engine/service.go`

This is the orchestration command processor.

Why it matters:

- start, completion, failure, retry, timer firing, cancellation, and compensation all flow through one service
- it loads durable state, replays history, computes the next commands, and appends the new transition atomically
- it retries on optimistic version conflicts instead of assuming single-threaded progress

Tradeoff:

This file is intentionally central. I could have split it into many smaller services, but that would have made the execution path harder to follow. The current shape is easier to explain in an interview.

### `internal/store/sqlite/store.go`

This is the first durable adapter.

Why it matters:

- it proves the engine is durable with no external infrastructure
- it keeps the demo story simple: one binary, one file, crash recovery still works
- it uses transactional writes to keep `runs`, `tasks`, and `history_events` consistent

Tradeoff:

SQLite is the wrong choice for high-contention multi-worker deployments, but the right choice for a clean first version that still demonstrates durability and replay.

### `internal/store/postgres/store.go`

This is the stronger concurrency backend.

Why it matters:

- it preserves the engine contract instead of forking the orchestration logic
- it uses Postgres-native locking behavior for task leasing
- it raises the ceiling of the project from a local durable demo to a more realistic deployment shape

Tradeoff:

I kept the optimistic version model from SQLite rather than redesigning the engine for full distributed coordination. That was a deliberate scope choice. Postgres improves concurrency and operability, but it still is not enough by itself to claim a distributed control plane.

### `internal/worker/worker.go`

This is the activity execution loop.

Why it matters:

- it polls by queue, acquires leases, sends heartbeats, and reports completion or failure
- it treats lease loss and cancellation as first-class runtime conditions
- it cleanly separates activity code from orchestration code

Tradeoff:

Polling is simpler than push dispatch. It is less efficient at scale, but much easier to reason about and perfectly adequate for the scope of the project.

### `internal/api/server.go`

This is the operator-facing control plane.

Why it matters:

- it exposes start, inspect, history, replay, and cancel
- it keeps the transport layer thin and pushes orchestration logic down into the engine
- it makes both the CLI and browser inspector consume the same backend surface

Tradeoff:

The API is intentionally narrow. I avoided premature auth, tenancy, and heavy mutation surfaces to keep the core systems story stronger.

### `internal/api/static/*`

These files implement the embedded run inspector UI.

Why it matters:

- they turn replay and history into something visual and immediately inspectable
- they make the project feel operable, not just executable
- the UI is served by the same binary, which keeps the deployment story simple

Tradeoff:

I chose embedded static assets over a larger frontend toolchain. That keeps the repo more Go-centric and avoids turning the project into two separate demos.

### `internal/app/app.go`

This is the runtime assembly layer.

Why it matters:

- it wires observability, storage, API, workers, and maintenance together
- it is where storage selection happens
- it makes the process model obvious: server plus optional embedded workers

Tradeoff:

The runtime still assumes a single maintenance loop. That is fine for local and single-node operation, but a real multi-instance version would need coordination here.

### `examples/order_fulfillment/main.go`

This is the example workflow.

Why it matters:

- it demonstrates fan-out/fan-in
- it includes retry behavior
- it uses a timer
- it exercises compensation paths

Tradeoff:

The example is realistic enough to show orchestration concerns, but still small enough to understand quickly in a review.

## Why Replay Instead Of Mutable State Only

The main reason is correctness under failure.

If I only mutated workflow rows in place, I could answer operational questions like "what is the latest status?" but I would be weaker on:

- proving how the workflow got there
- reconstructing execution after bugs or crashes
- validating whether the durable summary still matches derived state

Replay is more work, but it is the strongest signal in the project.

## Why Both `runs`/`tasks` And `history_events`

Because pure event sourcing is too expensive operationally for every API read.

`history_events` is the authority.
`runs` and `tasks` are denormalized views optimized for inspection and leasing.

That is the practical compromise:

- correctness from history
- operability from query-friendly tables

## Why SQLite First, Then Postgres

SQLite first:

- no infrastructure required
- one-file durability
- better first demo

Postgres next:

- stronger concurrency semantics
- `FOR UPDATE SKIP LOCKED` for task leasing
- more realistic operational ceiling

That sequence shows judgment: start with the smallest thing that proves the architecture, then extend the backend without rewriting the engine.

## Failure Modes To Talk Through

These are the best interview scenarios to walk through:

1. Worker dies after leasing a task.
   The lease expires, the maintenance loop detects it, the task is retried or failed based on policy.

2. Server crashes mid-run.
   Durable task and event state remain in storage, replay rebuilds state, and workers continue after restart.

3. Two operations race on the same run.
   The run version check detects optimistic conflicts, and the service retries the orchestration step.

4. A downstream step fails after prior success.
   The engine enters compensation mode and unwinds completed steps in reverse completion order.

5. The persisted run summary drifts from reality.
   Replay exposes the mismatch and provides a correctness check against the derived state.

## What I Would Say Is Intentionally Missing

- multi-node leadership for timer and lease recovery
- auth and multi-tenancy
- dynamic workflow registration across processes
- push-based scheduling
- external task queues

Leaving these out is not a weakness by itself. The important thing is explaining why they are out of scope and what the next architectural step would be.

## The Next Serious Step

If I were continuing this as a platform project, the next step would be coordinated multi-instance operation on Postgres:

- advisory locking or leader election for maintenance
- clearer API pagination and filtering
- richer metrics and traces
- stronger integration tests around leasing contention

That would move Orchid from a strong single-node orchestration engine to the edge of a real control plane.
