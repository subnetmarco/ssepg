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
	"regexp"
	"runtime"
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
	KeepAlive  time.Duration // default 15s (SSE heartbeat)
	SSEBufSize int           // default 32KB buffered writer

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
	// Advanced (optional, requires superuser; 0 disables)
	AlterSystemMaxNotificationMB int
}

func DefaultConfig() Config {
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
		AlterSystemMaxNotificationMB: 0,
	}
}

// ---------- Public Service API ----------

type Service struct {
	cfg Config
	br  *broker
	mux *http.ServeMux // used in Attach
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

	br, err := newBroker(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Service{cfg: cfg, br: br}, nil
}

// Attach registers three handlers on the provided mux:
//
//	POST {BasePath}/{id}/events publish a message: {"data":<json>}
//	GET  {BasePath}/{id}/events SSE stream
//	GET  {Healthz}              JSON with per-topic and totals
func (s *Service) Attach(mux *http.ServeMux) {
	s.mux = mux
	mux.HandleFunc(strings.TrimRight(s.cfg.BasePath, "/")+"/", s.handleTopic())
	mux.HandleFunc(s.cfg.Healthz, s.handleHealthz())
}

// Publish allows programmatic publish (bypassing HTTP).
func (s *Service) Publish(ctx context.Context, topic string, data json.RawMessage) error {
	topic, ok := normalizeTopic(topic)
	if !ok {
		return fmt.Errorf("ssepg: invalid topic")
	}
	return s.br.Publish(ctx, topic, data)
}

// Close gracefully drains and shuts down the service.
func (s *Service) Close(ctx context.Context) error {
	s.br.Shutdown(ctx)
	return nil
}

// ---------- HTTP Handlers ----------

var topicRE = regexp.MustCompile(`^[a-z0-9_-]{1,128}$`)

func normalizeTopic(s string) (string, bool) {
	x := strings.ToLower(s)
	return x, topicRE.MatchString(x)
}

type postBody struct {
	Data json.RawMessage `json:"data"`
}

func (s *Service) handleTopic() http.HandlerFunc {
	base := strings.TrimRight(s.cfg.BasePath, "/") + "/"
	return func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, base) {
			http.NotFound(w, r)
			return
		}
		rest := strings.TrimPrefix(r.URL.Path, base)
		parts := strings.Split(rest, "/")
		if len(parts) == 0 || parts[0] == "" {
			http.NotFound(w, r)
			return
		}
		topic, ok := normalizeTopic(parts[0])
		if !ok {
			http.Error(w, "invalid topic (allowed [a-z0-9_-]{1,128})", http.StatusBadRequest)
			return
		}

		// Publish
		if r.Method == http.MethodPost && len(parts) == 2 && parts[1] == "events" {
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
			return
		}

		// Subscribe SSE
		if r.Method == http.MethodGet && len(parts) == 2 && parts[1] == "events" {
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

			_, _ = io.WriteString(out, ": listening "+topic+"\n\n")
			if zw != nil {
				_ = zw.Flush()
			}
			_ = bw.Flush()
			flusher.Flush()

			t := time.NewTicker(s.cfg.KeepAlive)
			defer t.Stop()

			for {
				select {
				case <-r.Context().Done():
					return
				case <-t.C:
					_, _ = io.WriteString(out, ": keep-alive\n\n")
					if zw != nil {
						_ = zw.Flush()
					}
					_ = bw.Flush()
					flusher.Flush()
				case payload, ok := <-stream:
					if !ok {
						_, _ = io.WriteString(out, ": server shutting down\n\n")
						if zw != nil {
							_ = zw.Flush()
							_ = zw.Close()
						}
						_ = bw.Flush()
						flusher.Flush()
						return
					}
					if err := writeSSE(out, "message", "", payload); err != nil {
						return
					}
					if zw != nil {
						_ = zw.Flush()
					}
					_ = bw.Flush()
					flusher.Flush()
				}
			}
		}

		http.NotFound(w, r)
	}
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

				topicList = append(topicList, topicSnapshot{
					Topic:            name,
					Subscribers:      subs,
					DispatchDepth:    depth,
					PendingToClients: pending,
					Published:        h.published.Load(),
					Broadcast:        h.broadcast.Load(),
					DeliveredFrames:  h.deliveredFrames.Load(),
					DroppedClients:   h.droppedClients.Load(),
				})
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
}

// enqueue payload; drop oldest if full; aggressive buffer reuse
func (h *topicHub) enqueue(p []byte) {
	h.ringMu.Lock()
	if h.size == h.cap { // drop-oldest
		// Return dropped buffer to pool before overwriting
		if dropped := h.ring[h.tail]; dropped != nil {
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
			byteSliceBuf.Put(slot) // Return old buffer to pool
		}
		newSlot := byteSliceBuf.Get(len(p))
		newSlot = newSlot[:len(p)]
		copy(newSlot, p)
		h.ring[h.head] = newSlot
	}

	h.head = (h.head + 1) & h.mask
	if h.size < h.cap {
		h.size++
	}
	h.ringMu.Unlock()

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
	// init shards
	h.subsMu = make([]sync.RWMutex, b.cfg.FanoutShards)
	h.subs = make([]map[chan []byte]struct{}, b.cfg.FanoutShards)
	h.fanout = make([]chan []byte, b.cfg.FanoutShards)
	for i := 0; i < b.cfg.FanoutShards; i++ {
		h.subs[i] = make(map[chan []byte]struct{})
		h.fanout[i] = make(chan []byte, 64)
	}
	shard.topics[topic] = h

	// fanout workers
	for i := 0; i < b.cfg.FanoutShards; i++ {
		idx := i
		go func(tp string, hub *topicHub, shard int) {
			// Reusable snapshot buffer to reduce allocations
			var snapshotBuf []chan []byte
			for {
				select {
				case <-hub.quit:
					hub.subsMu[shard].Lock()
					for ch := range hub.subs[shard] {
						close(ch)
						delete(hub.subs[shard], ch)
					}
					hub.subsMu[shard].Unlock()
					return
				case payload := <-hub.fanout[shard]:
					// snapshot then deliver - reuse slice to reduce allocations
					hub.subsMu[shard].RLock()
					subCount := len(hub.subs[shard])
					if subCount == 0 {
						hub.subsMu[shard].RUnlock()
						// Return payload buffer to pool when no subscribers
						byteSliceBuf.Put(payload)
						continue
					}
					// Reuse snapshot slice - allocate once per worker
					var snapshot []chan []byte
					if cap(snapshotBuf) >= subCount {
						snapshot = snapshotBuf[:0]
					} else {
						snapshot = make([]chan []byte, 0, subCount)
					}
					for ch := range hub.subs[shard] {
						snapshot = append(snapshot, ch)
					}
					hub.subsMu[shard].RUnlock()
					for _, ch := range snapshot {
						select {
						case ch <- payload:
							hub.deliveredFrames.Add(1)
							b.totals.addDelivered(hashTopic(tp), 1)
						default:
							hub.subsMu[shard].Lock()
							if _, ok := hub.subs[shard][ch]; ok {
								close(ch)
								delete(hub.subs[shard], ch)
								hub.droppedClients.Add(1)
								b.totals.addDropped(hashTopic(tp), 1)
							}
							hub.subsMu[shard].Unlock()
						}
					}
					// Store snapshot buffer for reuse
					snapshotBuf = snapshot
					// Return payload buffer to pool after all deliveries
					byteSliceBuf.Put(payload)
				}
			}
		}(topic, h, idx)
	}

	// dispatcher: drain ring & hand payload to each fanout shard
	go func(tp string, hub *topicHub) {
		for {
			select {
			case <-hub.quit:
				return
			case <-hub.notify:
				for {
					payload, ok := hub.dequeue()
					if !ok {
						break
					}
					hub.broadcast.Add(1)
					b.totals.addBroadcast(hashTopic(tp), 1)
					for i := 0; i < b.cfg.FanoutShards; i++ {
						hub.fanout[i] <- payload
					}
				}
			}
		}
	}(topic, h)

	return h
}

func (b *broker) Subscribe(topic string, clientBuf int) (<-chan []byte, func()) {
	h := b.hub(topic)
	ch := make(chan []byte, clientBuf)
	if b.cfg.FanoutShards <= 0 {
		b.cfg.FanoutShards = 1 // Safety check
	}
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

		n, err := b.listenConn.WaitForNotification(ctx)
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
	select {
	case <-b.shutdownDone:
		// Notification loop finished gracefully
	case <-time.After(2 * time.Second):
		// Timeout waiting for notification loop
		log.Printf("ssepg: timeout waiting for notification loop to stop")
	}

	// Now it's safe to close connections
	_ = b.listenConn.Close(ctx)
	_ = b.notifyConn.Close(ctx)

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
func (p *bufferPool) Put(b *bytes.Buffer) { b.Reset(); p.pool.Put(b) }

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
		p.pool.Put(b)
		return make([]byte, 0, minCap)
	}
	return b[:0] // reset length but keep capacity
}
func (p *byteSlicePool) Put(b []byte) {
	if cap(b) > 64*1024 { // Don't pool very large slices
		return
	}
	p.pool.Put(b)
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
