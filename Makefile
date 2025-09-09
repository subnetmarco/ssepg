.PHONY: help build test test-short test-integration lint clean fmt vet mod-tidy

# Default target
help: ## Show this help message
	@echo 'Usage: make <target>'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the project
	go build -v ./...

test: ## Run all tests
	go test -v -race ./...

test-short: ## Run tests in short mode (skip integration tests)
	go test -v -short -race ./...

test-integration: ## Run integration tests only
	go test -v -race -run TestWithTestcontainers ./...

test-coverage: ## Run tests with coverage
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

lint: ## Run linter
	golangci-lint run

security: ## Run security scan
	gosec ./...

fmt: ## Format code
	gofmt -s -w .
	goimports -w .

vet: ## Run go vet
	go vet ./...

mod-tidy: ## Tidy go modules
	go mod tidy
	go mod verify

clean: ## Clean build artifacts
	go clean -cache -testcache -modcache
	rm -f coverage.out coverage.html

install-tools: ## Install development tools
	go install golang.org/x/tools/cmd/goimports@latest
	go install github.com/securego/gosec/v2/cmd/gosec@latest
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin

dev-setup: install-tools mod-tidy ## Set up development environment
	@echo "Development environment ready!"

example: ## Run the minimal example (requires DATABASE_URL)
	@if [ -z "$(DATABASE_URL)" ]; then \
		echo "Please set DATABASE_URL environment variable"; \
		echo "Example: make example DATABASE_URL=postgres://postgres@localhost:5432/postgres?sslmode=disable"; \
		exit 1; \
	fi
	go run examples/minimal/main.go

example-health: ## Run example with separate health port (requires DATABASE_URL)
	@if [ -z "$(DATABASE_URL)" ]; then \
		echo "Please set DATABASE_URL environment variable"; \
		echo "Example: make example-health DATABASE_URL=postgres://postgres@localhost:5432/postgres?sslmode=disable"; \
		exit 1; \
	fi
	go run examples/separate-health/main.go

example-auth: ## Run example with token authentication (requires DATABASE_URL, PUBLISH_TOKEN, LISTEN_TOKEN)
	@if [ -z "$(DATABASE_URL)" ]; then \
		echo "Please set DATABASE_URL environment variable"; \
		exit 1; \
	fi
	@if [ -z "$(PUBLISH_TOKEN)" ] || [ -z "$(LISTEN_TOKEN)" ]; then \
		echo "Please set PUBLISH_TOKEN and LISTEN_TOKEN environment variables"; \
		echo "Example: PUBLISH_TOKEN=pub123 LISTEN_TOKEN=sub456 make example-auth DATABASE_URL=..."; \
		exit 1; \
	fi
	go run examples/with-auth/main.go

example-scale: ## Run high-scale example optimized for 100K+ clients (requires DATABASE_URL)
	@if [ -z "$(DATABASE_URL)" ]; then \
		echo "Please set DATABASE_URL environment variable"; \
		echo "Example: make example-scale DATABASE_URL=postgres://postgres@localhost:5432/postgres?sslmode=disable"; \
		exit 1; \
	fi
	go run examples/high-scale/main.go

postgres-up: ## Start PostgreSQL with Docker Compose
	docker-compose up -d postgres

postgres-down: ## Stop PostgreSQL
	docker-compose down
