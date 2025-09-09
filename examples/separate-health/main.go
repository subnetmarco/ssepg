package main

import (
	"log"
	"net/http"

	_ "go.uber.org/automaxprocs"

	"github.com/subnetmarco/ssepg"
	"github.com/subnetmarco/ssepg/examples/shared"
)

func main() {
	// Configuration with separate health port for security isolation
	cfg := ssepg.DefaultConfig()
	cfg.DSN = shared.MustGetDSN()
	cfg.HealthPort = ":9090" // Health metrics on separate port

	svc := shared.MustCreateService(cfg)
	defer shared.GracefulServiceShutdown(svc)

	// Main application server (public-facing, no health endpoint)
	mux := http.NewServeMux()
	svc.Attach(mux) // Only topics endpoints, health is on separate port

	srv := shared.CreateHTTPServer(":8080", mux)

	log.Println("🚀 Main server on :8080 (topics only)")
	log.Println("📊 Health server on :9090 (metrics only)")
	log.Println("")
	log.Println("✨ Benefits of separate health port:")
	log.Println("   • Security: Health metrics isolated from public traffic")
	log.Println("   • Monitoring: Dedicated port for load balancers/monitoring")
	log.Println("   • Performance: No health check overhead on main port")
	log.Println("")
	log.Println("Try:")
	log.Println("  curl -N http://localhost:8080/topics/test/events")
	log.Println("  curl -X POST http://localhost:8080/topics/test/events -d '{\"data\":{\"msg\":\"hello\"}}'")
	log.Println("  curl http://localhost:9090/healthz")

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
