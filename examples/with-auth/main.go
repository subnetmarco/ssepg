package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	_ "go.uber.org/automaxprocs"

	"github.com/jackc/pgx/v5/pgxpool" // optional, just to validate DSN early
	"github.com/subnetmarco/ssepg"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("Set DATABASE_URL (e.g. postgres://postgres@localhost:5432/postgres?sslmode=disable)")
	}

	// optional: validate DSN quickly
	if _, err := pgxpool.ParseConfig(dsn); err != nil {
		log.Fatalf("bad DATABASE_URL: %v", err)
	}

	cfg := ssepg.DefaultConfig()
	cfg.DSN = dsn
	cfg.HealthPort = ":9090"                           // Health on separate port
	cfg.PublishToken = os.Getenv("PUBLISH_TOKEN")     // e.g. "pub-secret-abc123"
	cfg.ListenToken = os.Getenv("LISTEN_TOKEN")       // e.g. "sub-secret-def456"

	if cfg.PublishToken == "" || cfg.ListenToken == "" {
		log.Fatal("Set PUBLISH_TOKEN and LISTEN_TOKEN environment variables for authentication")
	}

	svc, err := ssepg.New(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := svc.Close(context.Background()); err != nil {
			log.Printf("Error closing service: %v", err)
		}
	}()

	// Main application server (public-facing, token-protected)
	mux := http.NewServeMux()
	svc.Attach(mux) // Topics endpoints with token auth

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       70 * time.Second, // SSE friendly
	}

	log.Println("🚀 Main server on :8080 (token-protected topics)")
	log.Println("📊 Health server on :9090 (metrics)")
	log.Println("")
	log.Println("Authentication required:")
	log.Printf("  PUBLISH_TOKEN: %s", cfg.PublishToken)
	log.Printf("  LISTEN_TOKEN:  %s", cfg.ListenToken)
	log.Println("")
	log.Println("Try:")
	log.Println("  # Subscribe (needs LISTEN_TOKEN)")
	log.Printf("  curl -N -H 'Authorization: Bearer %s' http://localhost:8080/topics/test/events\n", cfg.ListenToken)
	log.Println("")
	log.Println("  # Publish (needs PUBLISH_TOKEN)")
	log.Printf("  curl -X POST http://localhost:8080/topics/test/events \\\n")
	log.Printf("    -H 'Content-Type: application/json' \\\n")
	log.Printf("    -H 'Authorization: Bearer %s' \\\n", cfg.PublishToken)
	log.Printf("    -d '{\"data\":{\"msg\":\"authenticated hello\"}}'\n")
	log.Println("")
	log.Println("  # Health (no auth needed)")
	log.Println("  curl http://localhost:9090/healthz")

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
