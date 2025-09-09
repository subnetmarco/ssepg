package main

import (
	"log"
	"net/http"

	_ "go.uber.org/automaxprocs"

	"github.com/subnetmarco/ssepg"
	"github.com/subnetmarco/ssepg/examples/shared"
)

func main() {
	// Basic configuration with adaptive defaults
	cfg := ssepg.DefaultConfig()
	cfg.DSN = shared.MustGetDSN()
	
	// Optional customizations:
	// cfg.BasePath = "/topics"
	// cfg.Healthz = "/healthz"
	// cfg.AlterSystemMaxNotificationMB = 64 // only if superuser; 0 disables

	svc := shared.MustCreateService(cfg)
	defer shared.GracefulServiceShutdown(svc)

	mux := http.NewServeMux()
	svc.Attach(mux)

	srv := shared.CreateHTTPServer(":8080", mux)
	
	log.Println("🚀 Minimal SSE server ready on :8080")
	log.Println("   📡 Try: curl -N http://localhost:8080/topics/test/events")
	log.Println("   📤 Try: curl -X POST http://localhost:8080/topics/test/events -d '{\"data\":{\"hello\":\"world\"}}'")
	
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
