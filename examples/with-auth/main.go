package main

import (
	"log"
	"net/http"
	"os"

	_ "go.uber.org/automaxprocs"

	"github.com/subnetmarco/ssepg"
	"github.com/subnetmarco/ssepg/examples/shared"
)

func main() {
	// Configuration with token authentication and separate health port
	cfg := ssepg.DefaultConfig()
	cfg.DSN = shared.MustGetDSN()
	cfg.HealthPort = ":9090"                      // Health on separate port
	cfg.PublishToken = os.Getenv("PUBLISH_TOKEN") // e.g. "pub-secret-abc123"
	cfg.ListenToken = os.Getenv("LISTEN_TOKEN")   // e.g. "sub-secret-def456"

	if cfg.PublishToken == "" || cfg.ListenToken == "" {
		log.Fatal("Set PUBLISH_TOKEN and LISTEN_TOKEN environment variables for authentication")
	}

	svc := shared.MustCreateService(cfg)
	defer shared.GracefulServiceShutdown(svc)

	// Main application server (public-facing, token-protected)
	mux := http.NewServeMux()
	svc.Attach(mux) // Topics endpoints with token auth

	srv := shared.CreateHTTPServer(":8080", mux)

	log.Println("🚀 Authenticated SSE server on :8080")
	log.Println("📊 Health server on :9090 (no auth required)")
	log.Println("")
	log.Println("🔐 Token Authentication Active:")
	log.Printf("   PUBLISH_TOKEN: %s", cfg.PublishToken)
	log.Printf("   LISTEN_TOKEN:  %s", cfg.ListenToken)
	log.Println("")
	log.Println("✨ Security Features:")
	log.Println("   • Separate tokens for publish/subscribe operations")
	log.Println("   • Health metrics isolated on separate port")
	log.Println("   • Bearer token authentication on all endpoints")
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
