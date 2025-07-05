.PHONY: proto clean build e2e e2e-down test deps fmt dev-setup help

# Generate protobuf files
proto:
	@echo "Generating protobuf files..."
	@protoc -I pkg/proto --go_out=. --go-grpc_out=. pkg/proto/*.proto
	@echo "Protobuf generation complete"

# Clean generated files
clean:
	@echo "Cleaning generated files..."
	@find pkg/proto -name "*.pb.go" -delete
	@rm -rf bin/

# Build all binaries
build: proto
	@echo "Building binaries..."
	@mkdir -p bin
	@go build -o bin/coordinator ./cmd/coordinator
	@go build -o bin/datanode ./cmd/datanode
	@go build -o bin/client ./cmd/client
	@echo "Build complete"

# Optional log file for docker compose output (can be overridden: `make e2e LOG_FILE=my.log`)
LOG_FILE ?= ../logs/e2e_run.log

e2e:
	@echo "Running e2e tests (logs -> $(LOG_FILE))"
	@docker compose -f docker-compose.test.yml up --build 2>&1 | tee $(LOG_FILE)

e2e-down:
	@echo "Downing containers..."
	@docker compose -f docker-compose.test.yml down --volumes

# Run tests
test:
	@echo "Running tests..."
	@go test -v $$(go list ./... | grep -v 'tests/e2e') -race

# Install dependencies
deps:
	@echo "Installing dependencies..."
	@go mod tidy
	@go mod download

# Format code
fmt:
	@echo "Formatting code..."
	@go fmt ./...

# Development setup
dev-setup:
	@echo "Setting up development environment..."
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Help
help:
	@echo "Available targets:"
	@echo "  proto         - Generate protobuf files"
	@echo "  clean         - Clean generated files"
	@echo "  build         - Build all binaries"
	@echo "  test          - Run tests"
	@echo "  deps          - Install/update dependencies"
	@echo "  fmt           - Format code"
	@echo "  dev-setup     - Install development tools"
	@echo "  help          - Show this help"