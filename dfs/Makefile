.PHONY: proto clean build test run-coordinator run-datanode run-client

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

integration:
	@echo "Running integration tests"
	@docker compose -f docker-compose.test.yml up --build

integration-down:
	@echo "Downing containers..."
	@docker compose -f docker-compose.test.yml down

# Run tests
test:
	@echo "Running tests..."
	@go test -v ./...

# Run coordinator
run-coordinator: build
	@echo "Starting coordinator..."
	@./bin/coordinator

# Run datanode
run-datanode: build
	@echo "Starting datanode..."
	@./bin/datanode

# Run client
run-client: build
	@echo "Starting client..."
	@./bin/client

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
	@echo "  run-coordinator - Run coordinator service"
	@echo "  run-datanode    - Run datanode service"
	@echo "  run-client      - Run client"
	@echo "  deps          - Install/update dependencies"
	@echo "  fmt           - Format code"
	@echo "  dev-setup     - Install development tools"
	@echo "  help          - Show this help"