GO ?= go
DB ?= orchid.db
ADDR ?= :8080
PG_DSN ?= postgres://localhost:5432/orchid?sslmode=disable

.PHONY: test bench build run run-postgres clean

test:
	$(GO) test ./...

bench:
	$(GO) test -bench=. -benchmem ./...

build:
	$(GO) build ./...

run:
	$(GO) run ./cmd/orchestratord -listen $(ADDR) -db $(DB) -with-workers

run-postgres:
	$(GO) run ./cmd/orchestratord -listen $(ADDR) -storage postgres -postgres-dsn "$(PG_DSN)" -with-workers

clean:
	rm -f $(DB)
