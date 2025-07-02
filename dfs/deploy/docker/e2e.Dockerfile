FROM golang:1.24-alpine

# Install necessary tools for testing
RUN apk add --no-cache wget curl

# Install Delve debugger
RUN go install github.com/go-delve/delve/cmd/dlv@latest

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum first for better caching
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the entire dfs directory
COPY . .

# Sets up test-files directory in client for upload tests
RUN chmod +x /app/deploy/scripts/client-test.sh
ENTRYPOINT ["/app/deploy/scripts/client-test.sh"]