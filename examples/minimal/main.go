package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	_ "go.uber.org/automaxprocs"

	"github.com/jackc/pgx/v5/pgxpool" // optional, just to validate DSN early
	"github.com/subnetmarcop/ssepg"
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
	// cfg.BasePath = "/topics"
	// cfg.Healthz = "/healthz"
	// cfg.AlterSystemMaxNotificationMB = 64 // only if superuser; 0 disables

	svc, err := ssepg.New(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer svc.Close(context.Background())

	mux := http.NewServeMux()
	svc.Attach(mux)

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       70 * time.Second, // SSE friendly
	}
	log.Println("HTTP on :8080")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

