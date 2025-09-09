# ssepg — Ephemeral SSE over Postgres LISTEN/NOTIFY

Zero-persistence, topic-based **Server-Sent Events** (SSE) fanout using **Postgres LISTEN/NOTIFY**.  
Works across multiple app instances behind a load balancer. No tables or storage required.

**Created by AI and perfected under Marco's supervision**

- Per-topic dispatcher with a **ring buffer** (drop-oldest under burst)
- Single **LISTEN** connection, **sharded NOTIFY** channels (`topic_broadcast_{0..N-1}`)
- **Compact JSON**, prebuilt SQL, and buffer pools for low allocations
- Optional **gzip** for SSE (`Accept-Encoding: gzip`)
- **Health** endpoint with per-topic and total counters (in-memory)

> Ephemeral by design: if a client is disconnected, it **misses** events.

## Install

```bash
go get github.com/subnetmarco/ssepg
```

## Quickstart

```
package main

import (
  "context"
  "encoding/json"
  "log"
  "net/http"
  "os"
  "time"

  _ "go.uber.org/automaxprocs"
  "github.com/subnetmarco/ssepg" // replace with your module path
)

func main() {
  cfg := ssepg.DefaultConfig()
  cfg.DSN = os.Getenv("DATABASE_URL") // postgres://user:pass@host:5432/db?sslmode=disable

  svc, err := ssepg.New(context.Background(), cfg)
  if err != nil { log.Fatal(err) }
  defer svc.Close(context.Background())

  mux := http.NewServeMux()
  svc.Attach(mux) // POST/GET /topics/:id/events, GET /healthz

  // Example programmatic publish (optional)
  _ = svc.Publish(context.Background(), "alpha", json.RawMessage(`{"msg":"hello"}`))

  srv := &http.Server{
    Addr:              ":8080",
    Handler:           mux,
    ReadHeaderTimeout: 5 * time.Second,
    IdleTimeout:       70 * time.Second, // SSE-friendly
  }
  log.Fatal(srv.ListenAndServe())
}
```

## Publish / Subscribe

```
# Subscribe (SSE)
curl -N -H 'Accept-Encoding: gzip' http://localhost:8080/topics/alpha/events

# Publish
curl -X POST http://localhost:8080/topics/alpha/events \
  -H 'Content-Type: application/json' \
  -d '{"data":{"msg":"hello"}}'

# Health
curl -s http://localhost:8080/healthz | jq
```

## HTTP API

### POST /topics/{id}/events

Body: {"data": <json>} (≤ ~7.9KB after compaction)
Returns: {"topic":"{id}","data":<json>}

### GET /topics/{id}/events

SSE stream. Heartbeat comment every 15s. Frames use event: message with data: lines.

### GET /healthz

JSON totals + per-topic metrics (subscribers, ring depth, delivered, dropped). Per-instance.

## Configuration

```
cfg := ssepg.DefaultConfig()
cfg.DSN = "<postgres DSN>"

cfg.BasePath = "/topics"       // POST/GET /topics/:id/events
cfg.Healthz  = "/healthz"      // health endpoint
cfg.KeepAlive = 15*time.Second // SSE heartbeat
cfg.SSEBufSize = 32<<10        // bufio writer size

cfg.NotifyShards = 8           // LISTEN/NOTIFY channels (topic_broadcast_{0..N-1})
cfg.FanoutShards = 4           // per-topic fanout workers
cfg.RingCapacity = 1024        // power-of-two; bitmask ring
cfg.ClientChanBuf = 64         // per-subscriber channel size
cfg.MaxNotifyBytes = 7900      // NOTIFY payload limit
cfg.GracefulDrain = 10*time.Second

cfg.QueueWarnThreshold = 0.5   // warn when pg_notification_queue_usage() > 50%
cfg.QueuePollInterval = 30*time.Second

// Optional (requires superuser; 0 disables)
cfg.AlterSystemMaxNotificationMB = 64
```

## Semantics & Limits

* Ephemeral: no persistence or replay; disconnected clients miss events.
* Cross-instance: yes—shared Postgres delivers NOTIFYs to all instances.
* Ordering: best-effort per topic per instance.
* Payload size: keep data ≤ ~8KB due to NOTIFY limits.
* Backpressure: slow clients are dropped (channel full); clients should reconnect.

## Operational Notes

* Load balancer / proxy: disable response buffering; allow long-lived HTTP/1.1; generous idle timeouts.
* Postgres queue: monitor SELECT pg_notification_queue_usage(); Consider increasing max_notification_queue_size (DBA/superuser).
* Throughput tips: compact JSON; many smaller topics; raise shards if hot.

## Customizing Routes

```
cfg.BasePath = "/bus"   // POST/GET /bus/:id/events
cfg.Healthz  = "/ready" // GET /ready
```

## Development

### Running Tests

```bash
# Run all tests (requires PostgreSQL)
make test

# Run tests in short mode (uses in-memory mocks)
make test-short

# Run with coverage
make test-coverage

# Run integration tests with testcontainers
make test-integration
```

### Local Development

```bash
# Set up development environment
make dev-setup

# Run linting
make lint

# Format code
make fmt

# Run example locally (requires PostgreSQL)
DATABASE_URL=postgres://postgres@localhost:5432/postgres?sslmode=disable make example

# Or use Docker Compose for full setup
docker-compose up
```

### CI/CD

The project uses GitHub Actions for:
- **Linting**: golangci-lint with comprehensive rules
- **Testing**: Unit and integration tests with PostgreSQL
- **Building**: Multi-platform builds (Linux, macOS, Windows)  
- **Security**: gosec security scanning
- **Dependencies**: go mod tidy verification

All PRs must pass linting, testing, and building before merge.

## License

Apache License, Version 2.0

```
Copyright (c) 2025 Kong Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```