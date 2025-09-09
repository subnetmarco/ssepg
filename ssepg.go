// Package ssepg provides ephemeral topic-based Server-Sent Events (SSE)
// fanout backed by Postgres LISTEN/NOTIFY — no tables, no persistence.
// It is safe to run behind a load balancer across multiple instances.
//
// Features
// - Single dedicated LISTEN connection (sharded NOTIFY channels)
// - Per-topic dispatchers with a lock-free-ish ring buffer (drop-oldest)
// - Sharded fan-out goroutines to scale per-topic subscriber counts
// - Compact JSON + prebuilt NOTIFY SQL for low allocs
// - Optional gzip for SSE (automatic, via Accept-Encoding)
// - Health snapshot handler (in-memory metrics, per instance)
//
// Ephemeral semantics: if a client is disconnected, it misses messages.
package ssepg

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// Message is the wire format carried in LISTEN/NOTIFY.
type Message struct {
	Topic string          `json:"topic"`
	Data  json.RawMessage `json:"data"`
}

// ---------- Configuration ----------

type Config struct {
	// Required
	DSN string

	// HTTP routes
	// Base path for topics; POST/GET {BasePath}/{id}/events
	BasePath   string        // default "/topics"
	Healthz    string        // default "/healthz"
	HealthPort string        // optional separate port for health endpoint (e.g. ":9090")
	KeepAlive  time.Duration // default 15s (SSE heartbeat)
	SSEBufSize int           // default 32KB buffered writer

	// Security (optional token-based authentication)
	PublishToken string // optional: required for POST requests (Bearer token)
	ListenToken  string // optional: required for GET SSE requests (Bearer token)

	// Broker internals
	NotifyShards   int           // default 8 (LISTEN/NOTIFY channels)
	FanoutShards   int           // default 4 (per-topic fanout workers)
	RingCapacity   int           // default 1024 (power-of-two)
	ClientChanBuf  int           // default 64 (per-subscriber buffer)
	MaxNotifyBytes int           // default 7900 (Postgres payload limit ~8KB)
	GracefulDrain  time.Duration // default 10s

	// Diagnostics
	QueueWarnThreshold float64       // default 0.5 (warn at 50% usage)
	QueuePollInterval  time.Duration // default 30s
	
	// Memory management
	MemoryCleanupInterval time.Duration // default 5m (cleanup unused topics)
	TopicIdleTimeout      time.Duration // default 10m (remove idle topics)
	MemoryPressureThreshold int64       // default 100MB (trigger cleanup)
	
	// Advanced (optional, requires superuser; 0 disables)
	AlterSystemMaxNotificationMB int
}

// getSystemMemoryMB returns total system memory in megabytes
func getSystemMemoryMB() int64 {
	// Try Linux /proc/meminfo first
	if runtime.GOOS == "linux" {
		if data, err := os.ReadFile("/proc/meminfo"); err == nil {
			lines := strings.Split(string(data), "\n")
			for _, line := range lines {
				if strings.HasPrefix(line, "MemTotal:") {
					fields := strings.Fields(line)
					if len(fields) >= 2 {
						if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
							return kb / 1024 // Convert KB to MB
						}
					}
				}
			}
		}
	}
	
	// Try macOS sysctl
	if runtime.GOOS == "darwin" {
		if cmd := exec.Command("sysctl", "-n", "hw.memsize"); cmd != nil {
			if output, err := cmd.Output(); err == nil {
				if bytes, err := strconv.ParseInt(strings.TrimSpace(string(output)), 10, 64); err == nil {
					return bytes / (1024 * 1024) // Convert bytes to MB
				}
			}
		}
	}
	
	// Try Windows wmic
	if runtime.GOOS == "windows" {
		if cmd := exec.Command("wmic", "computersystem", "get", "TotalPhysicalMemory", "/value"); cmd != nil {
			if output, err := cmd.Output(); err == nil {
				lines := strings.Split(string(output), "\n")
				for _, line := range lines {
					if strings.HasPrefix(line, "TotalPhysicalMemory=") {
						valueStr := strings.TrimPrefix(line, "TotalPhysicalMemory=")
						valueStr = strings.TrimSpace(valueStr)
						if bytes, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
							return bytes / (1024 * 1024) // Convert bytes to MB
						}
					}
				}
			}
		}
	}
	
	// Final fallback: estimate based on CPU cores (modern systems)
	cpus := runtime.NumCPU()
	var estimatedGB int64
	
	// More realistic estimates based on typical modern systems
	if cpus >= 16 {
		estimatedGB = int64(cpus) * 4 // 4GB per core for high-end systems
	} else if cpus >= 8 {
		estimatedGB = int64(cpus) * 3 // 3GB per core for mid-range
	} else if cpus >= 4 {
		estimatedGB = int64(cpus) * 2 // 2GB per core for entry-level
	} else {
		estimatedGB = 8 // Minimum reasonable assumption
	}
	
	// Cap the estimate at reasonable bounds
	if estimatedGB < 4 {
		estimatedGB = 4 // Minimum 4GB
	}
	if estimatedGB > 256 {
		estimatedGB = 256 // Maximum 256GB for safety
	}
	
	return estimatedGB * 1024 // Convert GB to MB
}

// getOptimalShardCount calculates optimal shard count based on CPU cores
func getOptimalShardCount(baseFactor int) int {
	cpus := runtime.NumCPU()
	// Use CPU count as base, with minimum and maximum bounds
	shards := cpus * baseFactor
	
	// Ensure power of 2 for efficient hashing (for NotifyShards)
	if baseFactor == 1 { // NotifyShards case
		if shards < 8 {
			return 8
		}
		if shards > 128 {
			return 128
		}
		// Round to next power of 2
		for i := 8; i <= 128; i *= 2 {
			if i >= shards {
				return i
			}
		}
		return 64
	}
	
	// For FanoutShards, use direct CPU multiple with bounds
	if shards < 4 {
		return 4
	}
	if shards > 64 {
		return 64
	}
	return shards
}

// calculateOptimalConfig creates configuration based on system resources
func calculateOptimalConfig() Config {
	memoryMB := getSystemMemoryMB()
	cpus := runtime.NumCPU()
	
	// Base configuration
	cfg := Config{
		BasePath:  "/topics",
		Healthz:   "/healthz",
		KeepAlive: 15 * time.Second,
		SSEBufSize: 32 << 10,
		MaxNotifyBytes: 7900,
		GracefulDrain: 10 * time.Second,
		QueueWarnThreshold: 0.5,
		QueuePollInterval: 30 * time.Second,
		MemoryCleanupInterval: 5 * time.Minute,
		TopicIdleTimeout: 10 * time.Minute,
	}
	
	// Scale based on available memory
	if memoryMB < 1024 { // < 1GB - minimal config
		cfg.NotifyShards = 4
		cfg.FanoutShards = 2
		cfg.RingCapacity = 512
		cfg.ClientChanBuf = 32
		cfg.MemoryPressureThreshold = 50 * 1024 * 1024 // 50MB
		cfg.AlterSystemMaxNotificationMB = 16
	} else if memoryMB < 4096 { // 1-4GB - small scale
		cfg.NotifyShards = 8
		cfg.FanoutShards = 4
		cfg.RingCapacity = 1024
		cfg.ClientChanBuf = 64
		cfg.MemoryPressureThreshold = 200 * 1024 * 1024 // 200MB
		cfg.AlterSystemMaxNotificationMB = 64
	} else if memoryMB < 16384 { // 4-16GB - medium scale
		cfg.NotifyShards = getOptimalShardCount(1)  // CPU-based
		cfg.FanoutShards = getOptimalShardCount(2) // 2x CPU
		cfg.RingCapacity = 2048
		cfg.ClientChanBuf = 128
		cfg.MemoryPressureThreshold = int64(memoryMB) * 1024 * 1024 / 4 // 25% of RAM
		cfg.AlterSystemMaxNotificationMB = 256
	} else { // 16GB+ - high scale
		cfg.NotifyShards = getOptimalShardCount(1)  // CPU-based
		cfg.FanoutShards = getOptimalShardCount(4) // 4x CPU for high fanout
		cfg.RingCapacity = 8192
		cfg.ClientChanBuf = 512
		cfg.MemoryPressureThreshold = int64(memoryMB) * 1024 * 1024 / 3 // 33% of RAM
		cfg.AlterSystemMaxNotificationMB = 1024
		
		// High-scale optimizations
		cfg.KeepAlive = 30 * time.Second // Reduce heartbeat overhead
		cfg.SSEBufSize = 64 << 10        // Larger SSE buffers
		cfg.QueuePollInterval = 10 * time.Second // More frequent monitoring
		cfg.MemoryCleanupInterval = 2 * time.Minute // More frequent cleanup
	}
	
	log.Printf("ssepg: auto-configured for %d CPU cores, %d MB RAM", cpus, memoryMB)
	log.Printf("ssepg: NotifyShards=%d, FanoutShards=%d, RingCapacity=%d, ClientChanBuf=%d", 
		cfg.NotifyShards, cfg.FanoutShards, cfg.RingCapacity, cfg.ClientChanBuf)
	
	return cfg
}

// DefaultConfig returns an adaptive configuration that automatically scales based on system resources
func DefaultConfig() Config {
	return calculateOptimalConfig()
}

// StaticConfig returns the original static configuration for backward compatibility
func StaticConfig() Config {
	return Config{
		BasePath:                     "/topics",
		Healthz:                      "/healthz",
		KeepAlive:                    15 * time.Second,
		SSEBufSize:                   32 << 10,
		NotifyShards:                 8,
		FanoutShards:                 4,
		RingCapacity:                 1024,
		ClientChanBuf:                64,
		MaxNotifyBytes:               7900,
		GracefulDrain:                10 * time.Second,
		QueueWarnThreshold:           0.5,
		QueuePollInterval:            30 * time.Second,
		MemoryCleanupInterval:        5 * time.Minute,
		TopicIdleTimeout:             10 * time.Minute,
		MemoryPressureThreshold:      100 * 1024 * 1024, // 100MB
		AlterSystemMaxNotificationMB: 0,
	}
}

// HighScaleConfig returns a configuration optimized for hundreds of thousands of concurrent clients
func HighScaleConfig() Config {
	return Config{
		BasePath:                     "/topics",
		Healthz:                      "/healthz",
		KeepAlive:                    30 * time.Second, // Longer to reduce overhead
		SSEBufSize:                   64 << 10,         // 64KB buffer
		NotifyShards:                 64,               // More shards for distribution
		FanoutShards:                 32,               // More workers per topic
		RingCapacity:                 8192,             // Larger ring buffer
		ClientChanBuf:                512,              // Larger client buffers
		MaxNotifyBytes:               7900,
		GracefulDrain:                30 * time.Second, // Longer drain for many clients
		QueueWarnThreshold:           0.3,              // Earlier warning
		QueuePollInterval:            10 * time.Second, // More frequent monitoring
		MemoryCleanupInterval:        2 * time.Minute, // More frequent cleanup
		TopicIdleTimeout:             5 * time.Minute, // Faster cleanup
		MemoryPressureThreshold:      10 * 1024 * 1024 * 1024, // 10GB
		AlterSystemMaxNotificationMB: 1024,                     // 1GB notification queue
	}
}

// ---------- Public Service API ----------

type Service struct {
	cfg         Config
	br          *broker
	mux         *http.ServeMux // used in Attach
	healthMux   *http.ServeMux // separate health endpoint mux
	healthServer *http.Server   // separate health server (if HealthPort configured)
}

// New creates the service; it opens Postgres connections and starts the LISTEN loop.
func New(ctx context.Context, cfg Config) (*Service, error) {
	if cfg.DSN == "" {
		return nil, errors.New("ssepg: DSN required")
	}
	if cfg.RingCapacity&(cfg.RingCapacity-1) != 0 {
		return nil, fmt.Errorf("ssepg: RingCapacity must be power-of-two, got %d", cfg.RingCapacity)
	}
	if cfg.BasePath == "" {
		cfg.BasePath = "/topics"
	}
	if cfg.Healthz == "" {
		cfg.Healthz = "/healthz"
	}
	if cfg.KeepAlive <= 0 {
		cfg.KeepAlive = 15 * time.Second
	}
	if cfg.SSEBufSize <= 0 {
		cfg.SSEBufSize = 32 << 10
	}
	if cfg.NotifyShards <= 0 {
		cfg.NotifyShards = 8
	}
	if cfg.FanoutShards <= 0 {
		cfg.FanoutShards = 4
	}
	if cfg.ClientChanBuf <= 0 {
		cfg.ClientChanBuf = 64
	}
	if cfg.MaxNotifyBytes <= 0 {
		cfg.MaxNotifyBytes = 7900
	}
	if cfg.GracefulDrain <= 0 {
		cfg.GracefulDrain = 10 * time.Second
	}
	if cfg.QueueWarnThreshold <= 0 {
		cfg.QueueWarnThreshold = 0.5
	}
	if cfg.QueuePollInterval <= 0 {
		cfg.QueuePollInterval = 30 * time.Second
	}
	if cfg.MemoryCleanupInterval <= 0 {
		cfg.MemoryCleanupInterval = 5 * time.Minute
	}
	if cfg.TopicIdleTimeout <= 0 {
		cfg.TopicIdleTimeout = 10 * time.Minute
	}
	if cfg.MemoryPressureThreshold <= 0 {
		cfg.MemoryPressureThreshold = 100 * 1024 * 1024 // 100MB
	}

	br, err := newBroker(ctx, cfg)
	if err != nil {
		return nil, err
	}
	
	svc := &Service{cfg: cfg, br: br}
	
	// Log security configuration status
	svc.logSecurityStatus()
	
	// Set up separate health server if HealthPort is configured
	if cfg.HealthPort != "" {
		svc.healthMux = http.NewServeMux()
		svc.healthMux.HandleFunc(cfg.Healthz, svc.handleHealthz())
		
		svc.healthServer = &http.Server{
			Addr:              cfg.HealthPort,
			Handler:           svc.healthMux,
			ReadHeaderTimeout: 5 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       30 * time.Second,
		}
		
		// Start health server in background
		go func() {
			log.Printf("ssepg: health server starting on %s", cfg.HealthPort)
			if err := svc.healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("ssepg: health server error: %v", err)
			}
		}()
	}
	
	return svc, nil
}

// Attach registers handlers on the provided mux:
//
//	POST {BasePath}/{id}/events publish a message: {"data":<json>}
//	GET  {BasePath}/{id}/events SSE stream
//	GET  {Healthz}              JSON with per-topic and totals (only if HealthPort not configured)
func (s *Service) Attach(mux *http.ServeMux) {
	s.mux = mux
	mux.HandleFunc(strings.TrimRight(s.cfg.BasePath, "/")+"/", s.handleTopic())
	
	// Only attach health endpoint if not running on separate port
	if s.cfg.HealthPort == "" {
		mux.HandleFunc(s.cfg.Healthz, s.handleHealthz())
	}
}

// Publish allows programmatic publish (bypassing HTTP).
func (s *Service) Publish(ctx context.Context, topic string, data json.RawMessage) error {
	topic, ok := normalizeTopic(topic)
	if !ok {
		return fmt.Errorf("ssepg: invalid topic")
	}
	return s.br.Publish(ctx, topic, data)
}

// logSecurityStatus logs the current security configuration
func (s *Service) logSecurityStatus() {
	var securityFeatures []string
	var warnings []string
	
	// Check authentication status
	publishAuth := s.cfg.PublishToken != ""
	listenAuth := s.cfg.ListenToken != ""
	
	if publishAuth && listenAuth {
		securityFeatures = append(securityFeatures, "✅ Full authentication enabled (publish + subscribe)")
	} else if publishAuth {
		securityFeatures = append(securityFeatures, "✅ Publish authentication enabled")
		warnings = append(warnings, "⚠️  Subscribe endpoints are UNAUTHENTICATED")
	} else if listenAuth {
		securityFeatures = append(securityFeatures, "✅ Subscribe authentication enabled")
		warnings = append(warnings, "⚠️  Publish endpoints are UNAUTHENTICATED")
	} else {
		warnings = append(warnings, "⚠️  NO AUTHENTICATION: All endpoints are PUBLIC")
	}
	
	// Check health port isolation
	if s.cfg.HealthPort != "" {
		securityFeatures = append(securityFeatures, "✅ Health metrics isolated on separate port")
	} else {
		warnings = append(warnings, "⚠️  Health metrics exposed on main port")
	}
	
	// Log security status
	if len(securityFeatures) > 0 {
		for _, feature := range securityFeatures {
			log.Printf("ssepg: %s", feature)
		}
	}
	
	if len(warnings) > 0 {
		for _, warning := range warnings {
			log.Printf("ssepg: %s", warning)
		}
	}
	
	// Summary log
	if publishAuth || listenAuth || s.cfg.HealthPort != "" {
		log.Printf("ssepg: security features active (%d enabled, %d warnings)", len(securityFeatures), len(warnings))
	} else {
		log.Printf("ssepg: ⚠️  SECURITY WARNING: No authentication or isolation configured")
	}
}

// Close gracefully drains and shuts down the service.
func (s *Service) Close(ctx context.Context) error {
	// Shutdown health server if running separately
	if s.healthServer != nil {
		if err := s.healthServer.Shutdown(ctx); err != nil {
			log.Printf("ssepg: error shutting down health server: %v", err)
		}
	}
	
	s.br.Shutdown(ctx)
	return nil
}

// ---------- HTTP Handlers ----------

var topicRE = regexp.MustCompile(`^[a-z0-9_-]{1,128}$`)

func normalizeTopic(s string) (string, bool) {
	x := strings.ToLower(s)
	return x, topicRE.MatchString(x)
}

// validateToken checks if the request has the required Bearer token
func validateToken(r *http.Request, requiredToken string) bool {
	if requiredToken == "" {
		return true // No token required
	}
	
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return false
	}
	
	// Check for Bearer token format
	const bearerPrefix = "Bearer "
	if !strings.HasPrefix(authHeader, bearerPrefix) {
		return false
	}
	
	token := strings.TrimPrefix(authHeader, bearerPrefix)
	return token == requiredToken
}

// sendUnauthorized sends a 401 Unauthorized response
func sendUnauthorized(w http.ResponseWriter, message string) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	http.Error(w, message, http.StatusUnauthorized)
}

type postBody struct {
	Data json.RawMessage `json:"data"`
}

func (s *Service) handleTopic() http.HandlerFunc {
	base := strings.TrimRight(s.cfg.BasePath, "/") + "/"
	const eventsPath = "events"

	return func(w http.ResponseWriter, r *http.Request) {
		topic, parts, ok := s.parseTopicRequest(w, r, base)
		if !ok {
			return // Error already sent
		}

		// Route to appropriate handler
		if len(parts) == 2 && parts[1] == eventsPath {
			switch r.Method {
			case http.MethodPost:
				s.handlePublish(w, r, topic)
			case http.MethodGet:
				s.handleSubscribe(w, r, topic)
			default:
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
			return
		}

		http.NotFound(w, r)
	}
}

// parseTopicRequest extracts and validates topic from request path
func (s *Service) parseTopicRequest(w http.ResponseWriter, r *http.Request, base string) (string, []string, bool) {
	if !strings.HasPrefix(r.URL.Path, base) {
		http.NotFound(w, r)
		return "", nil, false
	}
	rest := strings.TrimPrefix(r.URL.Path, base)
	parts := strings.Split(rest, "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return "", nil, false
	}
	topic, ok := normalizeTopic(parts[0])
	if !ok {
		http.Error(w, "invalid topic (allowed [a-z0-9_-]{1,128})", http.StatusBadRequest)
		return "", nil, false
	}
	return topic, parts, true
}

// handlePublish processes POST requests to publish messages
func (s *Service) handlePublish(w http.ResponseWriter, r *http.Request, topic string) {
	// Check publish token if configured
	if !validateToken(r, s.cfg.PublishToken) {
		sendUnauthorized(w, "publish requires valid token")
		return
	}
	
	r.Body = http.MaxBytesReader(w, r.Body, 256<<10) // 256KB
	var body postBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Data) == 0 || string(body.Data) == "null" {
		http.Error(w, "invalid JSON; expected {\"data\": ...}", http.StatusBadRequest)
		return
	}
	if err := s.br.Publish(r.Context(), topic, body.Data); err != nil {
		http.Error(w, "publish error: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"topic": topic, "data": body.Data})
}

// handleSubscribe processes GET requests for SSE subscriptions
func (s *Service) handleSubscribe(w http.ResponseWriter, r *http.Request, topic string) {
	// Check listen token if configured
	if !validateToken(r, s.cfg.ListenToken) {
		sendUnauthorized(w, "subscribe requires valid token")
		return
	}
	
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	bw := bufio.NewWriterSize(w, s.cfg.SSEBufSize)
	var out io.Writer = bw
	var zw *gzip.Writer
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		zw = gzip.NewWriter(bw)
		out = zw
	}

	stream, cancel := s.br.Subscribe(topic, s.cfg.ClientChanBuf)
	defer cancel()

	s.sendSSEMessage(out, ": listening "+topic+"\n\n", flusher, zw, bw)

	t := time.NewTicker(s.cfg.KeepAlive)
	defer t.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-t.C:
			s.sendSSEMessage(out, ": keep-alive\n\n", flusher, zw, bw)
		case payload, ok := <-stream:
			if !ok {
				s.sendSSEMessage(out, ": server shutting down\n\n", flusher, zw, bw)
				if zw != nil {
					_ = zw.Close()
				}
				return
			}
			// writeSSE and then return payload to pool (subscriber owns this copy)
			err := writeSSE(out, "message", "", payload)
			// Always return to pool, even on error
			byteSliceBuf.Put(payload)
			if err != nil {
				return
			}
			s.flushSSE(flusher, zw, bw)
		}
	}
}

// sendSSEMessage sends a message and flushes the SSE stream
func (s *Service) sendSSEMessage(out io.Writer, message string, flusher http.Flusher, zw *gzip.Writer, bw *bufio.Writer) {
	_, _ = io.WriteString(out, message)
	s.flushSSE(flusher, zw, bw)
}

// flushSSE flushes all layers of the SSE output stream
func (s *Service) flushSSE(flusher http.Flusher, zw *gzip.Writer, bw *bufio.Writer) {
	if zw != nil {
		_ = zw.Flush()
	}
	_ = bw.Flush()
	flusher.Flush()
}

func (s *Service) handleHealthz() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		type topicSnapshot struct {
			Topic            string `json:"topic"`
			Subscribers      int    `json:"subscribers"`
			DispatchDepth    int    `json:"dispatch_depth"`
			PendingToClients int    `json:"pending_to_clients"`
			Published        int64  `json:"published"`
			Broadcast        int64  `json:"broadcast"`
			DeliveredFrames  int64  `json:"delivered_frames"`
			DroppedClients   int64  `json:"dropped_clients"`
			MemoryUsage      int64  `json:"memory_usage_bytes"`
			LastActivity     int64  `json:"last_activity_unix"`
			IsIdle           bool   `json:"is_idle"`
		}
		type totalsResp struct {
			Topics           int   `json:"topics"`
			Subscribers      int   `json:"subscribers"`
			DispatchDepth    int   `json:"dispatch_depth"`
			PendingToClients int   `json:"pending_to_clients"`
			Published        int64 `json:"published"`
			Broadcast        int64 `json:"broadcast"`
			DeliveredFrames  int64 `json:"delivered_frames"`
			DroppedClients   int64 `json:"dropped_clients"`
			TotalMemoryUsage int64 `json:"total_memory_usage_bytes"`
			IdleTopics       int   `json:"idle_topics"`
		}

		// Collect from all topic shards
		topicList := make([]topicSnapshot, 0)
		var tot totalsResp

		for i := range s.br.topicShards {
			shard := &s.br.topicShards[i]
			shard.mu.RLock()
			for name, h := range shard.topics {
				subs, pending := 0, 0
				for i := 0; i < len(h.subs); i++ {
					h.subsMu[i].RLock()
					subs += len(h.subs[i])
					for ch := range h.subs[i] {
						pending += len(ch)
					}
					h.subsMu[i].RUnlock()
				}
				h.ringMu.Lock()
				depth := h.size
				h.ringMu.Unlock()

				memUsage := h.memoryUsage.Load()
				lastActivity := h.lastActivity.Load()
				isIdle := h.isMarkedIdle.Load()
				
				topicList = append(topicList, topicSnapshot{
					Topic:            name,
					Subscribers:      subs,
					DispatchDepth:    depth,
					PendingToClients: pending,
					Published:        h.published.Load(),
					Broadcast:        h.broadcast.Load(),
					DeliveredFrames:  h.deliveredFrames.Load(),
					DroppedClients:   h.droppedClients.Load(),
					MemoryUsage:      memUsage,
					LastActivity:     lastActivity,
					IsIdle:           isIdle,
				})
				
				tot.TotalMemoryUsage += memUsage
				if isIdle {
					tot.IdleTopics++
				}
				tot.Topics++
				tot.Subscribers += subs
				tot.DispatchDepth += depth
				tot.PendingToClients += pending
			}
			shard.mu.RUnlock()
		}

		pub, bro, del, drp := s.br.totals.snapshot()
		tot.Published, tot.Broadcast, tot.DeliveredFrames, tot.DroppedClients = pub, bro, del, drp

		resp := map[string]any{
			"status": "ok",
			"now":    time.Now().Format(time.RFC3339Nano),
			"last_notification_at": func() string {
				t := s.br.lastNotificationAt()
				if t.IsZero() {
					return ""
				}
				return t.Format(time.RFC3339Nano)
			}(),
			"totals":        tot,
			"topics":        topicList,
			"go_version":    runtime.Version(),
			"num_goroutine": runtime.NumGoroutine(),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// ---------- Internals: broker, hubs, pools, SSE writer ----------

type counterShard struct {
	published atomic.Int64
	broadcast atomic.Int64
	delivered atomic.Int64
	dropped   atomic.Int64
}
type totals struct{ shards [32]counterShard }

func (t *totals) addPublished(h uint32, n int64) { t.shards[h%32].published.Add(n) }
func (t *totals) addBroadcast(h uint32, n int64) { t.shards[h%32].broadcast.Add(n) }
func (t *totals) addDelivered(h uint32, n int64) { t.shards[h%32].delivered.Add(n) }
func (t *totals) addDropped(h uint32, n int64)   { t.shards[h%32].dropped.Add(n) }
func (t *totals) snapshot() (pub, bro, del, drp int64) {
	for i := 0; i < 32; i++ {
		pub += t.shards[i].published.Load()
		bro += t.shards[i].broadcast.Load()
		del += t.shards[i].delivered.Load()
		drp += t.shards[i].dropped.Load()
	}
	return
}

type topicHub struct {
	// subscribers (sharded)
	subsMu []sync.RWMutex
	subs   []map[chan []byte]struct{}
	nextSh uint32

	// per-shard fanout channels
	fanout []chan []byte

	// ring buffer
	ringMu sync.Mutex
	ring   [][]byte
	head   int
	tail   int
	size   int
	cap    int
	mask   int
	notify chan struct{}
	quit   chan struct{}

	// counters
	published       atomic.Int64
	broadcast       atomic.Int64
	deliveredFrames atomic.Int64
	droppedClients  atomic.Int64
	
	// memory management
	lastActivity  atomic.Int64 // Unix timestamp of last activity
	memoryUsage   atomic.Int64 // Estimated memory usage in bytes
	isMarkedIdle  atomic.Bool  // Whether this hub is marked for cleanup
}

// enqueue payload; drop oldest if full; aggressive buffer reuse
func (h *topicHub) enqueue(p []byte) {
	// Update activity timestamp for cleanup detection
	h.lastActivity.Store(time.Now().Unix())
	h.isMarkedIdle.Store(false) // Reset idle marker
	
	h.ringMu.Lock()
	var memoryDelta int64
	
	if h.size == h.cap { // drop-oldest
		// Return dropped buffer to pool before overwriting
		if dropped := h.ring[h.tail]; dropped != nil {
			memoryDelta -= int64(cap(dropped))
			byteSliceBuf.Put(dropped)
		}
		h.tail = (h.tail + 1) & h.mask
	}

	// Try to reuse existing slot buffer
	slot := h.ring[h.head]
	if slot != nil && cap(slot) >= len(p) {
		// Reuse existing buffer with sufficient capacity
		slot = slot[:len(p)]
		copy(slot, p)
		h.ring[h.head] = slot
	} else {
		// Get buffer from pool or allocate new one
		if slot != nil {
			memoryDelta -= int64(cap(slot))
			byteSliceBuf.Put(slot) // Return old buffer to pool
		}
		newSlot := byteSliceBuf.Get(len(p))
		newSlot = newSlot[:len(p)]
		copy(newSlot, p)
		h.ring[h.head] = newSlot
		memoryDelta += int64(cap(newSlot))
	}

	h.head = (h.head + 1) & h.mask
	if h.size < h.cap {
		h.size++
	}
	h.ringMu.Unlock()
	
	// Update memory usage estimate
	h.memoryUsage.Add(memoryDelta)

	// Non-blocking notification (coalesce wake-ups)
	select {
	case h.notify <- struct{}{}:
	default:
	}
}

func (h *topicHub) dequeue() (p []byte, ok bool) {
	h.ringMu.Lock()
	if h.size == 0 {
		h.ringMu.Unlock()
		return nil, false
	}
	p = h.ring[h.tail]
	// Clear slot but don't return to pool yet - it may be reused in enqueue
	h.ring[h.tail] = nil
	h.tail = (h.tail + 1) & h.mask
	h.size--
	h.ringMu.Unlock()
	return p, true
}

// Sharded topic management to reduce lock contention
const topicShards = 32 // Power of 2 for efficient modulo

type topicShard struct {
	mu     sync.RWMutex
	topics map[string]*topicHub
}

type broker struct {
	cfg Config

	notifyConn *pgx.Conn
	listenConn *pgx.Conn
	shards     []string
	notifyPref [][]byte // cached NOTIFY "ch", '

	// Sharded topic storage for reduced contention
	topicShards [topicShards]topicShard

	draining       atomic.Bool
	lastNote       atomic.Int64
	totals         totals
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
	shutdownDone   chan struct{}
}

func shardName(i int) string { return fmt.Sprintf("topic_broadcast_%d", i) }

func hashTopic(topic string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(topic))
	return h.Sum32()
}

func newBroker(ctx context.Context, cfg Config) (*broker, error) {
	lc, err := pgx.Connect(ctx, cfg.DSN)
	if err != nil {
		return nil, err
	}
	nc, err := pgx.Connect(ctx, cfg.DSN)
	if err != nil {
		_ = lc.Close(ctx)
		return nil, err
	}
	shards := make([]string, cfg.NotifyShards)
	prefix := make([][]byte, cfg.NotifyShards)
	for i := 0; i < cfg.NotifyShards; i++ {
		ch := shardName(i)
		if _, err := lc.Exec(ctx, fmt.Sprintf(`LISTEN "%s"`, ch)); err != nil {
			_ = lc.Close(ctx)
			_ = nc.Close(ctx)
			return nil, fmt.Errorf("LISTEN %s failed: %w", ch, err)
		}
		shards[i] = ch
		prefix[i] = []byte(`NOTIFY "` + ch + `", '`)
	}
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	b := &broker{
		cfg:            cfg,
		notifyConn:     nc,
		listenConn:     lc,
		shards:         shards,
		notifyPref:     prefix,
		shutdownCtx:    shutdownCtx,
		shutdownCancel: shutdownCancel,
		shutdownDone:   make(chan struct{}),
	}
	// Initialize topic shards
	for i := range b.topicShards {
		b.topicShards[i].topics = make(map[string]*topicHub)
	}
	go b.notificationLoop(ctx)
	go b.queueUsageMonitor(ctx)
	go b.memoryCleanupMonitor(ctx)

	if mb := cfg.AlterSystemMaxNotificationMB; mb > 0 {
		_, err := nc.Exec(ctx, `ALTER SYSTEM SET max_notification_queue_size = '`+fmt.Sprint(mb)+`MB'`)
		if err != nil {
			log.Printf("ssepg: alter system failed (requires superuser): %v", err)
		} else {
			_, _ = nc.Exec(ctx, `SELECT pg_reload_conf()`)
			log.Printf("ssepg: requested max_notification_queue_size=%dMB", mb)
		}
	}
	return b, nil
}

func (b *broker) queueUsageMonitor(ctx context.Context) {
	t := time.NewTicker(b.cfg.QueuePollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			var u float64
			if err := b.notifyConn.QueryRow(ctx, `SELECT pg_notification_queue_usage()`).Scan(&u); err == nil {
				if u > b.cfg.QueueWarnThreshold {
					log.Printf("ssepg: WARN pg_notification_queue_usage=%.2f", u)
				}
			}
		}
	}
}

func (b *broker) getTopicShard(topic string) *topicShard {
	h := hashTopic(topic)
	return &b.topicShards[h%topicShards]
}

// memoryCleanupMonitor periodically cleans up idle topics and manages memory pressure
func (b *broker) memoryCleanupMonitor(ctx context.Context) {
	ticker := time.NewTicker(b.cfg.MemoryCleanupInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.performMemoryCleanup()
		}
	}
}

// performMemoryCleanup identifies and cleans up idle topics and manages memory pressure
func (b *broker) performMemoryCleanup() {
	now := time.Now().Unix()
	idleThreshold := now - int64(b.cfg.TopicIdleTimeout.Seconds())
	
	var totalMemoryUsage int64
	var idleTopics []string
	var memoryPressureTopics []*topicHub
	
	// Scan all topic shards for cleanup candidates
	for i := range b.topicShards {
		shard := &b.topicShards[i]
		shard.mu.RLock()
		
		for topicName, hub := range shard.topics {
			lastActivity := hub.lastActivity.Load()
			memUsage := hub.memoryUsage.Load()
			totalMemoryUsage += memUsage
			
			// Check if topic is idle
			if lastActivity < idleThreshold && !hub.isMarkedIdle.Load() {
				// Check if there are no active subscribers
				hasSubscribers := false
				for j := 0; j < len(hub.subs); j++ {
					hub.subsMu[j].RLock()
					if len(hub.subs[j]) > 0 {
						hasSubscribers = true
					}
					hub.subsMu[j].RUnlock()
					if hasSubscribers {
						break
					}
				}
				
				if !hasSubscribers {
					idleTopics = append(idleTopics, topicName)
					hub.isMarkedIdle.Store(true)
				}
			}
			
			// Check for memory pressure
			if memUsage > b.cfg.MemoryPressureThreshold/10 { // 10MB per topic threshold
				memoryPressureTopics = append(memoryPressureTopics, hub)
			}
		}
		
		shard.mu.RUnlock()
	}
	
	// Log memory status if significant usage
	if totalMemoryUsage > b.cfg.MemoryPressureThreshold/2 {
		log.Printf("ssepg: memory usage %d MB, %d idle topics, %d high-memory topics", 
			totalMemoryUsage/(1024*1024), len(idleTopics), len(memoryPressureTopics))
	}
	
	// Clean up idle topics
	if len(idleTopics) > 0 {
		b.cleanupIdleTopics(idleTopics)
	}
	
	// Handle memory pressure by cleaning ring buffers
	if totalMemoryUsage > b.cfg.MemoryPressureThreshold {
		b.handleMemoryPressure(memoryPressureTopics)
	}
}

// cleanupIdleTopics removes topics that have been idle for too long
func (b *broker) cleanupIdleTopics(idleTopics []string) {
	cleaned := 0
	for _, topicName := range idleTopics {
		shard := b.getTopicShard(topicName)
		shard.mu.Lock()
		
		if hub, exists := shard.topics[topicName]; exists {
			// Double-check it's still idle and has no subscribers
			hasSubscribers := false
			for j := 0; j < len(hub.subs); j++ {
				hub.subsMu[j].RLock()
				if len(hub.subs[j]) > 0 {
					hasSubscribers = true
				}
				hub.subsMu[j].RUnlock()
				if hasSubscribers {
					break
				}
			}
			
			if !hasSubscribers && hub.isMarkedIdle.Load() {
				// Clean up the hub
				close(hub.quit)
				
				// Clean up ring buffer and return memory to pool
				hub.ringMu.Lock()
				for i := 0; i < hub.cap; i++ {
					if hub.ring[i] != nil {
						byteSliceBuf.Put(hub.ring[i])
					}
				}
				hub.ringMu.Unlock()
				
				delete(shard.topics, topicName)
				cleaned++
			}
		}
		
		shard.mu.Unlock()
	}
	
	if cleaned > 0 {
		log.Printf("ssepg: cleaned up %d idle topics", cleaned)
	}
}

// handleMemoryPressure reduces memory usage when under pressure
func (b *broker) handleMemoryPressure(pressureTopics []*topicHub) {
	cleaned := 0
	for _, hub := range pressureTopics {
		hub.ringMu.Lock()
		// Reduce ring buffer size by half to free memory
		if hub.size > hub.cap/4 {
			newTail := (hub.tail + hub.size/2) & hub.mask
			for i := hub.tail; i != newTail; i = (i + 1) & hub.mask {
				if hub.ring[i] != nil {
					byteSliceBuf.Put(hub.ring[i])
					hub.ring[i] = nil
				}
			}
			hub.tail = newTail
			hub.size = hub.size / 2
			cleaned++
		}
		hub.ringMu.Unlock()
	}
	
	if cleaned > 0 {
		log.Printf("ssepg: reduced ring buffers for %d topics under memory pressure", cleaned)
	}
}

func (b *broker) hub(topic string) *topicHub {
	shard := b.getTopicShard(topic)

	// Try read lock first for existing topics (common case)
	shard.mu.RLock()
	if h, ok := shard.topics[topic]; ok {
		shard.mu.RUnlock()
		return h
	}
	shard.mu.RUnlock()

	// Need to create new topic, acquire write lock
	shard.mu.Lock()
	defer shard.mu.Unlock()
	// Double-check after acquiring write lock
	if h, ok := shard.topics[topic]; ok {
		return h
	}
	h := &topicHub{
		ring:   make([][]byte, b.cfg.RingCapacity),
		cap:    b.cfg.RingCapacity,
		mask:   b.cfg.RingCapacity - 1,
		notify: make(chan struct{}, 1),
		quit:   make(chan struct{}),
	}
	// Initialize memory tracking
	h.lastActivity.Store(time.Now().Unix())
	h.memoryUsage.Store(int64(b.cfg.RingCapacity * 8)) // Estimate for slice overhead
	// init shards
	h.subsMu = make([]sync.RWMutex, b.cfg.FanoutShards)
	h.subs = make([]map[chan []byte]struct{}, b.cfg.FanoutShards)
	h.fanout = make([]chan []byte, b.cfg.FanoutShards)
	for i := 0; i < b.cfg.FanoutShards; i++ {
		h.subs[i] = make(map[chan []byte]struct{})
		h.fanout[i] = make(chan []byte, 64)
	}
	shard.topics[topic] = h

	// Start fanout workers
	b.startFanoutWorkers(topic, h)

	// Start dispatcher
	b.startDispatcher(topic, h)

	return h
}

// startFanoutWorkers starts the fanout worker goroutines for a topic hub
func (b *broker) startFanoutWorkers(topic string, hub *topicHub) {
	for i := 0; i < b.cfg.FanoutShards; i++ {
		idx := i
		go func(tp string, h *topicHub, shard int) {
			// Reusable snapshot buffer to reduce allocations
			var snapshotBuf []chan []byte
			for {
				select {
				case <-h.quit:
					b.cleanupFanoutShard(h, shard)
					return
				case payload := <-h.fanout[shard]:
					b.deliverToShard(tp, h, shard, payload, &snapshotBuf)
				}
			}
		}(topic, hub, idx)
	}
}

// cleanupFanoutShard closes all subscribers in a fanout shard
func (b *broker) cleanupFanoutShard(hub *topicHub, shard int) {
	hub.subsMu[shard].Lock()
	for ch := range hub.subs[shard] {
		close(ch)
		delete(hub.subs[shard], ch)
	}
	hub.subsMu[shard].Unlock()
}

// deliverToShard delivers a payload to all subscribers in a fanout shard
func (b *broker) deliverToShard(topic string, hub *topicHub, shard int, payload []byte, snapshotBuf *[]chan []byte) {
	hub.subsMu[shard].RLock()
	subCount := len(hub.subs[shard])
	if subCount == 0 {
		hub.subsMu[shard].RUnlock()
		byteSliceBuf.Put(payload)
		return
	}

	// Reuse snapshot slice
	var snapshot []chan []byte
	if cap(*snapshotBuf) >= subCount {
		snapshot = (*snapshotBuf)[:0]
	} else {
		snapshot = make([]chan []byte, 0, subCount)
	}
	for ch := range hub.subs[shard] {
		snapshot = append(snapshot, ch)
	}
	hub.subsMu[shard].RUnlock()

	// Memory optimization: batch allocate copies to reduce allocation overhead
	if len(snapshot) == 1 {
		// Single subscriber - can share the payload directly
		select {
		case snapshot[0] <- payload:
			hub.deliveredFrames.Add(1)
			b.totals.addDelivered(hashTopic(topic), 1)
		default:
			b.dropSlowSubscriber(hub, shard, snapshot[0], topic)
		}
	} else {
		// Multiple subscribers - batch allocate copies
		copies := b.batchAllocatePayloads(payload, len(snapshot))
		for i, ch := range snapshot {
			select {
			case ch <- copies[i]:
				hub.deliveredFrames.Add(1)
				b.totals.addDelivered(hashTopic(topic), 1)
			default:
				// Return unused copy to pool
				byteSliceBuf.Put(copies[i])
				b.dropSlowSubscriber(hub, shard, ch, topic)
			}
		}
	}

	*snapshotBuf = snapshot
	// Return the original shard payload to pool
	byteSliceBuf.Put(payload)
}

// dropSlowSubscriber removes a slow/full subscriber
func (b *broker) dropSlowSubscriber(hub *topicHub, shard int, ch chan []byte, topic string) {
	hub.subsMu[shard].Lock()
	if _, ok := hub.subs[shard][ch]; ok {
		close(ch)
		delete(hub.subs[shard], ch)
		hub.droppedClients.Add(1)
		b.totals.addDropped(hashTopic(topic), 1)
	}
	hub.subsMu[shard].Unlock()
}

// batchAllocatePayloads efficiently creates copies for multiple subscribers
func (b *broker) batchAllocatePayloads(payload []byte, count int) [][]byte {
	copies := make([][]byte, count)
	payloadLen := len(payload)
	
	// Allocate one large buffer and slice it up (more cache-friendly)
	if count > 1 && payloadLen < 4096 { // Only for reasonable payload sizes
		totalSize := payloadLen * count
		bigBuffer := byteSliceBuf.Get(totalSize)
		bigBuffer = bigBuffer[:totalSize]
		
		for i := 0; i < count; i++ {
			start := i * payloadLen
			end := start + payloadLen
			copies[i] = bigBuffer[start:end]
			copy(copies[i], payload)
		}
	} else {
		// Fall back to individual allocations for large payloads
		for i := 0; i < count; i++ {
			copies[i] = byteSliceBuf.Get(payloadLen)
			copies[i] = copies[i][:payloadLen]
			copy(copies[i], payload)
		}
	}
	
	return copies
}

// startDispatcher starts the ring buffer dispatcher for a topic hub
func (b *broker) startDispatcher(topic string, hub *topicHub) {
	go func(tp string, h *topicHub) {
		for {
			select {
			case <-h.quit:
				return
			case <-h.notify:
				for {
					payload, ok := h.dequeue()
					if !ok {
						break
					}
					h.broadcast.Add(1)
					b.totals.addBroadcast(hashTopic(tp), 1)

					// Send a copy to each fanout shard to avoid races
					for i := 0; i < b.cfg.FanoutShards; i++ {
						shardPayload := byteSliceBuf.Get(len(payload))
						shardPayload = shardPayload[:len(payload)]
						copy(shardPayload, payload)
						h.fanout[i] <- shardPayload
					}

					// Return original payload to pool
					byteSliceBuf.Put(payload)
				}
			}
		}
	}(topic, hub)
}

func (b *broker) Subscribe(topic string, clientBuf int) (<-chan []byte, func()) {
	h := b.hub(topic)
	ch := make(chan []byte, clientBuf)
	if b.cfg.FanoutShards <= 0 {
		b.cfg.FanoutShards = 1 // Safety check
	}
	
	// Update activity tracking
	h.lastActivity.Store(time.Now().Unix())
	h.isMarkedIdle.Store(false)
	
	// Safe conversion: modulo ensures result is always < FanoutShards
	sh := int(atomic.AddUint32(&h.nextSh, 1)) % b.cfg.FanoutShards
	h.subsMu[sh].Lock()
	h.subs[sh][ch] = struct{}{}
	h.subsMu[sh].Unlock()
	
	cancel := func() {
		h.subsMu[sh].Lock()
		if _, ok := h.subs[sh][ch]; ok {
			close(ch)
			delete(h.subs[sh], ch)
		}
		h.subsMu[sh].Unlock()
		// Update activity on unsubscribe as well
		h.lastActivity.Store(time.Now().Unix())
	}
	return ch, cancel
}

func (b *broker) Publish(ctx context.Context, topic string, data json.RawMessage) error {
	if b.draining.Load() {
		return errors.New("server draining")
	}
	// Compact inline data
	cData, err := compactJSON(data)
	if err != nil {
		return fmt.Errorf("invalid JSON data: %w", err)
	}
	// Build {"topic":"..","data":<cData>}
	jbuf := jsonBuf.Get()
	defer jsonBuf.Put(jbuf)
	jbuf.Reset()
	jbuf.WriteString(`{"topic":"`)
	jbuf.WriteString(topic)
	jbuf.WriteString(`","data":`)
	jbuf.Write(cData)
	jbuf.WriteByte('}')
	// Use pooled byte slice instead of make([]byte, ...)
	payload := byteSliceBuf.Get(jbuf.Len())
	payload = payload[:jbuf.Len()]
	copy(payload, jbuf.Bytes())
	defer byteSliceBuf.Put(payload)

	if len(payload) > b.cfg.MaxNotifyBytes {
		return fmt.Errorf("payload too large for NOTIFY (%d > %d)", len(payload), b.cfg.MaxNotifyBytes)
	}

	h := hashTopic(topic)
	if b.cfg.NotifyShards <= 0 {
		b.cfg.NotifyShards = 1 // Safety check
	}
	// Safe conversion: modulo ensures result is always < NotifyShards
	si := int(h) % b.cfg.NotifyShards
	sqlBuf := jsonBuf.Get()
	defer jsonBuf.Put(sqlBuf)
	sqlBuf.Reset()
	sqlBuf.Write(b.notifyPref[si]) // NOTIFY "channel", '
	writeSQLEscaped(sqlBuf, payload)
	sqlBuf.WriteByte('\'')

	if _, err := b.notifyConn.Exec(ctx, sqlBuf.String()); err != nil {
		return err
	}
	// counters
	b.hub(topic).published.Add(1)
	b.totals.addPublished(h, 1)
	return nil
}

func (b *broker) notificationLoop(ctx context.Context) {
	defer close(b.shutdownDone)

	for {
		// Check if we should shutdown
		select {
		case <-b.shutdownCtx.Done():
			return
		default:
		}

		n, err := b.listenConn.WaitForNotification(b.shutdownCtx)
		if err != nil {
			if b.draining.Load() || errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("ssepg: listen error: %v (retrying in 1s)", err)
			time.Sleep(time.Second)
			continue
		}
		b.lastNote.Store(time.Now().UnixNano())

		// Use pooled message struct
		m := messageBuf.Get()
		if err := json.Unmarshal([]byte(n.Payload), m); err != nil {
			log.Printf("ssepg: bad payload: %v", err)
			messageBuf.Put(m)
			continue
		}
		// enqueue raw bytes directly (reused/copy-on-write inside the ring)
		b.hub(m.Topic).enqueue(m.Data)
		messageBuf.Put(m)
	}
}

func (b *broker) Shutdown(ctx context.Context) {
	b.draining.Store(true)

	// Signal notification loop to stop
	b.shutdownCancel()

	// Wait for notification loop to finish with timeout
	shutdownTimer := time.NewTimer(500 * time.Millisecond) // Shorter timeout for tests
	defer shutdownTimer.Stop()

	gracefulShutdown := false
	select {
	case <-b.shutdownDone:
		// Notification loop finished gracefully
		gracefulShutdown = true
	case <-shutdownTimer.C:
		// Timeout - force shutdown without closing connections to avoid race
		log.Printf("ssepg: timeout waiting for notification loop to stop")
	case <-ctx.Done():
		// Context cancelled
		log.Printf("ssepg: shutdown context cancelled")
	}

	// Only close connections if notification loop finished gracefully
	if gracefulShutdown {
		_ = b.listenConn.Close(ctx)
		_ = b.notifyConn.Close(ctx)
	}
	// If not graceful, connections will be cleaned up by process exit

	// drain rings (bounded)
	deadline := time.Now().Add(b.cfg.GracefulDrain)
	for time.Now().Before(deadline) {
		if b.allRingsEmpty() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// close hubs across all shards
	for i := range b.topicShards {
		shard := &b.topicShards[i]
		shard.mu.Lock()
		for t, h := range shard.topics {
			close(h.quit)
			delete(shard.topics, t)
		}
		shard.mu.Unlock()
	}
}

func (b *broker) allRingsEmpty() bool {
	for i := range b.topicShards {
		shard := &b.topicShards[i]
		shard.mu.RLock()
		for _, h := range shard.topics {
			h.ringMu.Lock()
			empty := h.size == 0
			h.ringMu.Unlock()
			if !empty {
				shard.mu.RUnlock()
				return false
			}
		}
		shard.mu.RUnlock()
	}
	return true
}

func (b *broker) lastNotificationAt() time.Time {
	ns := b.lastNote.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// ---------- small helpers & pools ----------

// Pool type definitions
type bufferPool struct{ pool sync.Pool }

func (p *bufferPool) Get() *bytes.Buffer  { return p.pool.Get().(*bytes.Buffer) }
func (p *bufferPool) Put(b *bytes.Buffer) { b.Reset(); p.pool.Put(b) } //nolint:staticcheck

type messagePool struct{ pool sync.Pool }

func (p *messagePool) Get() *Message { return p.pool.Get().(*Message) }
func (p *messagePool) Put(m *Message) {
	*m = Message{} // zero out for reuse
	p.pool.Put(m)
}

type byteSlicePool struct{ pool sync.Pool }

func (p *byteSlicePool) Get(minCap int) []byte {
	b := p.pool.Get().([]byte)
	if cap(b) < minCap {
		// If pooled slice is too small, return a new one
		p.pool.Put(b) //nolint:staticcheck
		return make([]byte, 0, minCap)
	}
	return b[:0] // reset length but keep capacity
}
func (p *byteSlicePool) Put(b []byte) {
	if cap(b) > 64*1024 { // Don't pool very large slices
		return
	}
	p.pool.Put(b) //nolint:staticcheck
}

// Memory pools for high-frequency allocations
var (
	jsonBuf      = &bufferPool{pool: sync.Pool{New: func() any { return new(bytes.Buffer) }}}
	messageBuf   = &messagePool{pool: sync.Pool{New: func() any { return new(Message) }}}
	byteSliceBuf = &byteSlicePool{pool: sync.Pool{New: func() any {
		// Start with 1KB slices, will grow as needed
		return make([]byte, 0, 1024)
	}}}
)

func compactJSON(raw json.RawMessage) (json.RawMessage, error) {
	buf := jsonBuf.Get()
	defer jsonBuf.Put(buf)
	if err := json.Compact(buf, raw); err != nil {
		return nil, err
	}
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, nil
}

func writeSQLEscaped(dst *bytes.Buffer, b []byte) {
	for {
		i := bytes.IndexByte(b, '\'')
		if i < 0 {
			dst.Write(b)
			return
		}
		dst.Write(b[:i])
		dst.WriteString("''")
		b = b[i+1:]
	}
}

func writeSSE(w io.Writer, event, id string, data []byte) error {
	buf := jsonBuf.Get()
	defer jsonBuf.Put(buf)

	if id != "" {
		buf.WriteString("id: ")
		buf.WriteString(id)
		buf.WriteByte('\n')
	}
	if event != "" {
		buf.WriteString("event: ")
		buf.WriteString(event)
		buf.WriteByte('\n')
	}
	// split by '\n': one "data: " per line
	start := 0
	for i := 0; i <= len(data); i++ {
		if i == len(data) || data[i] == '\n' {
			buf.WriteString("data: ")
			buf.Write(data[start:i])
			buf.WriteByte('\n')
			start = i + 1
		}
	}
	buf.WriteByte('\n')
	_, err := w.Write(buf.Bytes())
	return err
}
