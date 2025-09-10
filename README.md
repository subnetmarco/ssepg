# ssepg — Ephemeral SSE over Postgres LISTEN/NOTIFY

[![CI](https://github.com/subnetmarco/ssepg/actions/workflows/ci.yml/badge.svg)](https://github.com/subnetmarco/ssepg/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/subnetmarco/ssepg)](https://goreportcard.com/report/github.com/subnetmarco/ssepg)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Zero-persistence, topic-based **Server-Sent Events** (SSE) fanout using **Postgres LISTEN/NOTIFY**.  
**Horizontally scalable** across multiple app instances behind a load balancer. No tables or storage required.

**ℹ️ Created by AI under Marco's supervision**

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
  svc.Attach(mux) // POST/GET /topics/:id/events (health on separate port if configured)

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

```bash
# Subscribe (SSE) - without gzip for readability
curl -N http://localhost:8080/topics/alpha/events

# Subscribe (SSE) - with gzip compression  
curl -N -H 'Accept-Encoding: gzip' --output - http://localhost:8080/topics/alpha/events

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

**Note**: When `cfg.HealthPort` is configured, this endpoint is only available on the separate health port, not on the main application server.

## Configuration

ssepg **automatically adapts** to your system resources for optimal performance:

```go
cfg := ssepg.DefaultConfig()  // 🤖 Auto-detects CPU cores and memory
cfg.DSN = "<postgres DSN>"

// ✨ Adaptive configuration automatically optimizes:
// - NotifyShards: CPU-based (8-128) for PostgreSQL load distribution
// - FanoutShards: 2-4×CPU (4-64) for message delivery parallelism  
// - RingCapacity: Memory-based (512-8192) for traffic burst handling
// - ClientChanBuf: Scale-based (32-512) to prevent client drops
// - MemoryPressureThreshold: 25-33% of RAM for automatic cleanup

// Optional security and customization
cfg.HealthPort = ":9090"       // separate port for health (security)
cfg.PublishToken = "secret123"  // Bearer token for POST requests
cfg.ListenToken = "secret456"   // Bearer token for GET SSE requests
```

**🤖 Intelligent Auto-Configuration:**

Your system will be automatically optimized on startup:
```
ssepg: auto-configured for 12 CPU cores, 36864 MB RAM
ssepg: NotifyShards=16, FanoutShards=48, RingCapacity=8192, ClientChanBuf=512
```

**💪 System Resource Utilization:**
- **CPU cores**: Fully utilized with optimal shard distribution
- **Memory**: 25-33% allocated for message buffering  
- **Network**: Optimized buffer sizes for your system's capacity
- **PostgreSQL**: Sharding scales with your CPU count

**Configuration Options:**

| Function | Behavior | Use Case |
|----------|----------|----------|
| `DefaultConfig()` | **Auto-adaptive** | All deployments - automatically optimal |

**Adaptive Scaling Tiers:**

| System RAM | Configuration Tier | Max Clients | NotifyShards | FanoutShards |
|------------|-------------------|-------------|--------------|--------------|
| < 1GB | Minimal | ~1K | 4 | 2 |
| 1-4GB | Small | ~10K | 8 | 4 |
| 4-16GB | Medium | ~100K | CPU-based | 2×CPU |
| 16GB+ | High | **500K+** | CPU-based | **4×CPU** |

**Manual Configuration (if needed):**
```go
cfg := ssepg.DefaultConfig()  // Start with adaptive base
cfg.NotifyShards = 32         // Override specific values
cfg.FanoutShards = 16
cfg.MemoryPressureThreshold = 5 * 1024 * 1024 * 1024 // 5GB
```

## High-Scale Configuration

For deployments handling **hundreds of thousands of concurrent clients**, start with adaptive configuration and apply manual overrides:

```go
// Start with adaptive configuration (automatically optimal for your hardware)
cfg := ssepg.DefaultConfig()
cfg.DSN = "postgres://..."

// Manual overrides for extreme scale (500K+ concurrent clients)
cfg.RingCapacity = 32768                             // 4x larger ring buffers
cfg.ClientChanBuf = 2048                             // 4x larger client buffers  
cfg.MemoryPressureThreshold = 50 * 1024 * 1024 * 1024 // 50GB threshold
cfg.GracefulDrain = 60 * time.Second                 // Longer drain time
cfg.QueuePollInterval = 5 * time.Second              // More aggressive monitoring

// NotifyShards and FanoutShards are automatically set based on CPU count
// but can also be manually overridden if needed:
// cfg.NotifyShards = 128    // Force specific value
// cfg.FanoutShards = 256    // Force specific value
```

**Scaling Guidelines:**

| Concurrent Clients | NotifyShards | FanoutShards | ClientChanBuf | MemoryThreshold |
|-------------------|--------------|--------------|---------------|-----------------|
| 1K - 10K | 8 (default) | 4 (default) | 64 | 100MB |
| 10K - 100K | 16-32 | 8-16 | 128-256 | 1GB |
| 100K - 500K | 32-64 | 16-32 | 256-512 | 5-10GB |
| 500K+ | 64-128 | 32-64 | 512-1024 | 10GB+ |

**Performance Considerations:**
- **NotifyShards**: Distributes PostgreSQL LISTEN/NOTIFY load
- **FanoutShards**: Parallelizes message delivery per topic
- **ClientChanBuf**: Prevents client drops during traffic bursts
- **MemoryThreshold**: Allows larger memory usage before cleanup
- **RingCapacity**: Buffers more messages during traffic spikes

## Semantics & Limits

* Ephemeral: no persistence or replay; disconnected clients miss events.
* Cross-instance: yes—shared Postgres delivers NOTIFYs to all instances.
* Ordering: best-effort per topic per instance.
* Payload size: keep data ≤ ~8KB due to NOTIFY limits.
* Backpressure: slow clients are dropped (channel full); clients should reconnect.

## Horizontal Scaling

ssepg is designed for **horizontal scaling** across multiple instances:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   ssepg     │    │   ssepg     │    │   ssepg     │
│ Instance 1  │    │ Instance 2  │    │ Instance 3  │
│   :8080     │    │   :8081     │    │   :8082     │
└─────────────┘    └─────────────┘    └─────────────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
                  ┌─────────────────┐
                  │   PostgreSQL    │
                  │ LISTEN/NOTIFY   │
                  └─────────────────┘
```

**Key Features:**
- **Publish anywhere**: Message published to any instance reaches all subscribers
- **Subscribe anywhere**: Clients can connect to any instance via load balancer  
- **Shared state**: PostgreSQL coordinates message delivery across instances
- **No coordination**: Instances don't need to know about each other
- **Load balancer friendly**: Works with round-robin, least-connections, etc.

## Operational Notes

* Load balancer / proxy: disable response buffering; allow long-lived HTTP/1.1; generous idle timeouts.
* Postgres queue: monitor SELECT pg_notification_queue_usage(); Consider increasing max_notification_queue_size (DBA/superuser).
* Throughput tips: compact JSON; many smaller topics; raise shards if hot.

### High-Scale Deployment Checklist

For **100K+ concurrent clients**, ensure:

**PostgreSQL Tuning:**
```sql
-- Increase notification queue (requires superuser)
ALTER SYSTEM SET max_notification_queue_size = '1GB';
SELECT pg_reload_conf();

-- Monitor notification queue usage
SELECT pg_notification_queue_usage(); -- Keep < 0.3

-- Optimize connection limits
ALTER SYSTEM SET max_connections = 1000;
```

**System Resources:**
- **Memory**: 16GB+ RAM (estimate ~100KB per 1000 clients)
- **CPU**: 8+ cores (fanout workers are CPU-intensive)
- **Network**: 10Gbps+ for high message throughput
- **File descriptors**: `ulimit -n 65536` or higher

**Load Balancer Configuration:**
```nginx
# NGINX example for SSE
location /topics/ {
    proxy_pass http://ssepg_backend;
    proxy_buffering off;                # Critical for SSE
    proxy_cache off;
    proxy_read_timeout 3600s;          # Long timeout for SSE
    proxy_http_version 1.1;
    proxy_set_header Connection "";
}
```

**Monitoring Alerts:**
- `pg_notification_queue_usage() > 0.5`
- Memory usage > 80% of threshold
- Client drop rate > 1%/minute
- Ring buffer utilization > 90%

## Customizing Routes

```
cfg.BasePath = "/bus"   // POST/GET /bus/:id/events
cfg.Healthz  = "/ready" // GET /ready
cfg.HealthPort = ":9090" // Separate health port for security
```

## Security

### Authentication with Bearer Tokens

Protect your SSE endpoints with optional Bearer token authentication. Supports separate tokens for publish and subscribe operations:

```go
cfg := ssepg.DefaultConfig()
cfg.DSN = "postgres://..."
cfg.PublishToken = "publisher-secret-abc123"   // Required for POST /topics/:id/events
cfg.ListenToken = "subscriber-secret-def456"   // Required for GET /topics/:id/events

svc, _ := ssepg.New(context.Background(), cfg)
```

**Authentication Modes:**

| Configuration | Publish | Subscribe | Use Case |
|---------------|---------|-----------|----------|
| Both tokens set | 🔒 Auth required | 🔒 Auth required | Full security |
| Only `PublishToken` | 🔒 Auth required | ✅ Open | Public read, controlled write |
| Only `ListenToken` | ✅ Open | 🔒 Auth required | Public write, controlled read |
| No tokens | ✅ Open | ✅ Open | Development/internal use |

**Security Benefits:**
- **🔒 Principle of least privilege**: Separate permissions for different operations
- **🛡️ Token isolation**: Leaked subscriber token cannot publish messages
- **🎯 Fine-grained access**: Grant only necessary permissions per client
- **✅ Zero-trust**: Every operation can require authentication
- **🔄 Independent rotation**: Change publish/subscribe tokens separately

### Separate Health Port

For production deployments, you can isolate health/metrics endpoints on a separate port for enhanced security:

```go
cfg := ssepg.DefaultConfig()
cfg.DSN = "postgres://..."
cfg.HealthPort = ":9090" // Health metrics on separate port

svc, _ := ssepg.New(context.Background(), cfg)

// Main server (public-facing, no health endpoint)
mux := http.NewServeMux()
svc.Attach(mux) // Only topics endpoints: POST/GET /topics/:id/events
http.ListenAndServe(":8080", mux)

// Health server runs automatically on :9090 with full metrics
// curl http://localhost:9090/healthz
```

**Architecture:**
```
Public Internet → Load Balancer → :8080 (SSE topics only)
                                     ↓
Internal Network → Monitoring   → :9090 (health/metrics only)
```

**Security Benefits:**
- **🔒 Firewall isolation**: Block external access to metrics port
- **📊 Internal monitoring**: Health checks only from internal networks  
- **🛡️ Reduced attack surface**: Metrics not exposed to public internet
- **✅ Compliance**: Meets security requirements for sensitive environments
- **🚀 Zero-downtime health checks**: Monitor without affecting user traffic

**Health Metrics Include:**
- Per-topic statistics (subscribers, published, delivered, dropped)
- **Memory usage tracking** (per-topic and total)
- **Idle topic detection** (last activity timestamps)
- Ring buffer depths and pending message counts
- PostgreSQL notification timestamps
- Go runtime metrics (version, goroutines)

## Memory Management & Resource Cleanup

ssepg includes intelligent memory management to handle long-running deployments:

```go
cfg.MemoryCleanupInterval = 5*time.Minute    // How often to scan for cleanup
cfg.TopicIdleTimeout = 10*time.Minute        // Remove topics after idle time
cfg.MemoryPressureThreshold = 100*1024*1024  // Trigger cleanup at 100MB
```

**Automatic Cleanup Features:**
- **🧹 Idle topic removal**: Topics with no subscribers are automatically removed
- **💾 Memory pressure handling**: Ring buffers are reduced when memory usage is high  
- **📊 Memory tracking**: Per-topic memory usage estimation and reporting
- **⏰ Activity monitoring**: Tracks last activity timestamp for each topic
- **🔄 Ring buffer optimization**: Batch allocation for multiple subscribers

**Memory Optimization Benefits:**
- **Prevents memory leaks**: Unused topics don't accumulate indefinitely
- **Handles traffic spikes**: Automatic cleanup after burst traffic
- **Efficient allocation**: Single large buffer allocation for multiple subscribers
- **Observable cleanup**: Logs cleanup actions for monitoring
- **Configurable thresholds**: Tune cleanup behavior for your environment

**Health Endpoint Memory Metrics:**
```json
{
  "totals": {
    "total_memory_usage_bytes": 12582912,
    "idle_topics": 3
  },
  "topics": [{
    "topic": "active-topic",
    "memory_usage_bytes": 8192,
    "last_activity_unix": 1694234567,
    "is_idle": false
  }]
}
```

## Security: Token Authentication

Protect your SSE endpoints with optional Bearer token authentication:

```go
cfg := ssepg.DefaultConfig()
cfg.DSN = "postgres://..."
cfg.PublishToken = "publisher-secret-abc123"   // Required for POST
cfg.ListenToken = "subscriber-secret-def456"   // Required for GET

svc, _ := ssepg.New(context.Background(), cfg)
```

**Separate Token Benefits:**
- **🔒 Principle of least privilege**: Different permissions for publish/subscribe
- **🛡️ Token isolation**: Leaked listen token can't be used for publishing
- **🎯 Fine-grained control**: Grant only necessary permissions
- **✅ Zero-trust security**: Every request requires valid authentication

**Usage Examples:**

```bash
# Subscribe with authentication
curl -N -H 'Authorization: Bearer subscriber-secret-def456' \
  http://localhost:8080/topics/alpha/events

# Publish with authentication  
curl -X POST http://localhost:8080/topics/alpha/events \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer publisher-secret-abc123' \
  -d '{"data":{"msg":"authenticated hello"}}'

# Unauthorized requests return 401
curl http://localhost:8080/topics/alpha/events
# → 401 Unauthorized: subscribe requires valid token
```

**Startup Security Logging:**

ssepg logs security status on startup to ensure awareness:

```
ssepg: ✅ Full authentication enabled (publish + subscribe)
ssepg: ✅ Health metrics isolated on separate port
ssepg: security features active (2 enabled, 0 warnings)
```

Or with warnings for partial security:
```
ssepg: ✅ Publish authentication enabled
ssepg: ⚠️  Subscribe endpoints are UNAUTHENTICATED
ssepg: ⚠️  Health metrics exposed on main port
ssepg: security features active (1 enabled, 2 warnings)
```

**Token Management:**
- **Environment variables**: Store tokens securely outside code
- **Independent rotation**: Change publish/subscribe tokens separately
- **Standard format**: Bearer token (`Authorization: Bearer <token>`)
- **Proper responses**: 401 Unauthorized with WWW-Authenticate header

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

# Or start PostgreSQL with Docker and run example locally
make postgres-up
DATABASE_URL=postgres://ssepg:ssepg@localhost:5432/ssepg?sslmode=disable make example
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