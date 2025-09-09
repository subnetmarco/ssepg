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
	cfg.HealthPort = ":9090" // Health metrics on separate port

	svc, err := ssepg.New(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := svc.Close(context.Background()); err != nil {
			log.Printf("Error closing service: %v", err)
		}
	}()

	// Main application server (public-facing)
	mux := http.NewServeMux()
	svc.Attach(mux) // Only topics endpoints, no health

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       70 * time.Second, // SSE friendly
	}

	log.Println("🚀 Main server on :8080 (topics)")
	log.Println("📊 Health server on :9090 (metrics)")
	log.Println("")
	log.Println("Try:")
	log.Println("  curl -N http://localhost:8080/topics/test/events")
	log.Println("  curl -X POST http://localhost:8080/topics/test/events -d '{\"data\":{\"msg\":\"hello\"}}'")
	log.Println("  curl http://localhost:9090/healthz")

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

