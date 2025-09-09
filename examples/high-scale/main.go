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

	// Use high-scale configuration optimized for 100K+ concurrent clients
	cfg := ssepg.HighScaleConfig()
	cfg.DSN = dsn
	cfg.HealthPort = ":9090" // Separate health port for security

	log.Println("🚀 Starting ssepg in HIGH-SCALE mode")
	log.Printf("   📊 NotifyShards: %d (distributes PostgreSQL load)", cfg.NotifyShards)
	log.Printf("   ⚡ FanoutShards: %d (parallelizes message delivery)", cfg.FanoutShards)
	log.Printf("   💾 RingCapacity: %d (buffers traffic spikes)", cfg.RingCapacity)
	log.Printf("   📡 ClientChanBuf: %d (prevents client drops)", cfg.ClientChanBuf)
	log.Printf("   🧠 MemoryThreshold: %d MB (allows large memory usage)", cfg.MemoryPressureThreshold/(1024*1024))
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
