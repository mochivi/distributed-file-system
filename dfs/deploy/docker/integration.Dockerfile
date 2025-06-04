FROM golang:1.24-alpine

# Install necessary tools for testing
RUN apk add --no-cache wget curl

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum first for better caching
COPY dfs/go.mod dfs/go.sum ./

# Download dependencies
RUN go mod download

# Copy the entire dfs directory
COPY dfs/ .

ENTRYPOINT ["go", "test", "-v", "-timeout=300s", "./tests/integration/..."]