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
	go fmt ./...
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

postgres-up: ## Start PostgreSQL with Docker Compose
	docker-compose up -d postgres

postgres-down: ## Stop PostgreSQL
	docker-compose down
