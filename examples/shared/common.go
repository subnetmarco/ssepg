package shared

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/subnetmarco/ssepg"
)

// MustGetDSN retrieves and validates the DATABASE_URL environment variable
func MustGetDSN() string {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("Set DATABASE_URL (e.g. postgres://postgres@localhost:5432/postgres?sslmode=disable)")
	}
	
	// Validate DSN format
	if _, err := pgxpool.ParseConfig(dsn); err != nil {
		log.Fatalf("Invalid DATABASE_URL: %v", err)
	}
	
	return dsn
}

// MustCreateService creates a new ssepg service or exits on error
func MustCreateService(cfg ssepg.Config) *ssepg.Service {
	svc, err := ssepg.New(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}
	return svc
}

// CreateHTTPServer creates a standard HTTP server optimized for SSE
func CreateHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       70 * time.Second, // SSE-friendly timeout
	}
}

// GracefulServiceShutdown provides a standard service shutdown with error logging
func GracefulServiceShutdown(svc *ssepg.Service) {
	if err := svc.Close(context.Background()); err != nil {
		log.Printf("Error closing service: %v", err)
	}
}
