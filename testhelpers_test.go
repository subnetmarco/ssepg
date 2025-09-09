package ssepg_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// StartPostgresContainer starts a PostgreSQL container for testing
func StartPostgresContainer(ctx context.Context) (testcontainers.Container, string, error) {
	req := testcontainers.ContainerRequest{
		Image:        "postgres:15-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_PASSWORD": "testpass",
			"POSTGRES_USER":     "testuser",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(60 * time.Second),
	}

	postgres, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", err
	}

	host, err := postgres.Host(ctx)
	if err != nil {
		return postgres, "", err
	}

	port, err := postgres.MappedPort(ctx, "5432")
	if err != nil {
		return postgres, "", err
	}

	dsn := fmt.Sprintf("postgres://testuser:testpass@%s:%s/testdb?sslmode=disable", host, port.Port())
	return postgres, dsn, nil
}

// Integration test that uses testcontainers
func TestWithTestcontainers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start PostgreSQL container
	postgres, dsn, err := StartPostgresContainer(ctx)
	if err != nil {
		t.Skipf("Failed to start PostgreSQL container (Docker required): %v", err)
	}
	defer func() {
		if err := postgres.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	// Set the DSN for tests
	t.Setenv("TEST_DATABASE_URL", dsn)

	// Run a subset of tests with the containerized database
	t.Run("BasicPublishSubscribe", TestBasicPublishSubscribe)
	t.Run("TopicIsolation", TestTopicIsolation)
	t.Run("HorizontalScaling", TestHorizontalScaling)
	t.Run("HealthEndpoint", TestHealthEndpoint)
	t.Run("SeparateHealthPort", TestSeparateHealthPort)
	t.Run("HealthMetricsOnSeparatePort", TestHealthMetricsOnSeparatePort)
	t.Run("TokenAuthentication", TestTokenAuthentication)
	t.Run("PartialAuthentication", TestPartialAuthentication)
	t.Run("TokenCrossValidation", TestTokenCrossValidation)
	t.Run("MemoryManagement", TestMemoryManagement)
	t.Run("HighScaleConfig", TestHighScaleConfig)
}
