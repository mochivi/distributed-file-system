FROM golang:1.24-alpine AS builder

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum first for better caching
COPY dfs/go.mod dfs/go.sum ./

# Download dependencies
RUN go mod download

# Copy the entire dfs directory
COPY dfs/ .

# Build the datanode binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o datanode ./cmd/datanode/main.go

# Final stage - minimal runtime image
FROM alpine:latest

# Install ca-certificates for HTTPS calls if needed
RUN apk --no-cache add ca-certificates

# Create non-root user for security
RUN addgroup -g 1001 -S dfs && \
    adduser -u 1001 -S dfs -G dfs

# Set working directory
WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app .

# Change ownership to non-root user
RUN chown dfs:dfs /app

# Switch to non-root user
USER dfs

EXPOSE 8081

ENTRYPOINT ["./datanode"]