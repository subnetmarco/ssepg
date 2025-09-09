package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	_ "go.uber.org/automaxprocs"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/subnetmarco/ssepg"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("Set DATABASE_URL (e.g. postgres://postgres@localhost:5432/postgres?sslmode=disable)")
	}

	// Validate DSN
	if _, err := pgxpool.ParseConfig(dsn); err != nil {
		log.Fatalf("bad DATABASE_URL: %v", err)
	}

	// Start with adaptive configuration, then apply high-scale overrides
	cfg := ssepg.DefaultConfig()
	cfg.DSN = dsn
	cfg.HealthPort = ":9090" // Separate health port for security
	
	// Manual overrides for extreme scale (500K+ concurrent clients)
	cfg.RingCapacity = 32768                             // 4x larger ring buffers
	cfg.ClientChanBuf = 2048                             // 4x larger client buffers  
	cfg.MemoryPressureThreshold = 50 * 1024 * 1024 * 1024 // 50GB threshold
	cfg.GracefulDrain = 60 * time.Second                 // Longer drain time
	cfg.QueuePollInterval = 5 * time.Second              // More aggressive monitoring
	cfg.MemoryCleanupInterval = 1 * time.Minute          // More frequent cleanup

	log.Println("🚀 Starting ssepg in HIGH-SCALE mode (adaptive + manual overrides)")
	log.Printf("   📊 NotifyShards: %d (auto-adapted to CPU cores)", cfg.NotifyShards)
	log.Printf("   ⚡ FanoutShards: %d (auto-adapted to CPU cores)", cfg.FanoutShards)
	log.Printf("   💾 RingCapacity: %d (manually set for extreme scale)", cfg.RingCapacity)
	log.Printf("   📡 ClientChanBuf: %d (manually set for extreme scale)", cfg.ClientChanBuf)
	log.Printf("   🧠 MemoryThreshold: %d GB (manually set for extreme scale)", cfg.MemoryPressureThreshold/(1024*1024*1024))
	log.Printf("   🔧 PostgreSQL queue: %d MB (requires superuser)", cfg.AlterSystemMaxNotificationMB)
	log.Println()

	svc, err := ssepg.New(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		log.Println("🛑 Shutting down...")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := svc.Close(ctx); err != nil {
			log.Printf("Error closing service: %v", err)
		}
	}()

	// Main application server (optimized for high concurrency)
	mux := http.NewServeMux()
	svc.Attach(mux)

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      0,  // No write timeout for SSE
		IdleTimeout:       120 * time.Second, // Longer for many connections
		MaxHeaderBytes:    8 << 10, // 8KB headers
	}

	log.Println("🌐 High-scale server ready:")
	log.Println("   📡 Topics: http://localhost:8080/topics/{id}/events")
	log.Println("   📊 Health: http://localhost:9090/healthz")
	log.Println()
	log.Println("💡 Performance tips:")
	log.Println("   - Use many small topics rather than few large ones")
	log.Println("   - Monitor /healthz for memory usage and client drops")
	log.Println("   - Keep message payloads < 4KB for best performance")
	log.Println("   - Use compression for bandwidth efficiency")
	log.Println()
	log.Println("🔍 Monitoring:")
	log.Println("   curl http://localhost:9090/healthz | jq '.totals'")

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
