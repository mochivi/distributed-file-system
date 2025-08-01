# Configuration Reference

DFS services are currently configured through **environment variables** with sensible defaults that allow the demo cluster to start with minimal configuration. YAML configuration files are planned for future releases to provide more detailed operational settings.

## Implementation Status

### ✅ Currently Implemented
- **Environment Variables**: Service discovery and basic configuration
- **Default Values**: Hardcoded sensible defaults for all components
- **Logging Configuration**: Environment variable-based log level control
- **Basic CLI**: Command-line interface with minimal configuration

### 🚧 In Progress
- **Configuration File Support**: YAML configuration files for detailed settings
- **Configuration Validation**: Input validation and error checking
- **Hot-Reload**: Dynamic configuration updates without restart

### 📋 Planned Features
- **Advanced Configuration**: Detailed operational settings via YAML
- **Configuration Management**: Centralized configuration management
- **Configuration Templates**: Pre-built configurations for different environments

---

## Environment Variables

Environment variables are primarily used for service discovery and initial node registration:

| Variable | Component | Default | Description |
|----------|-----------|---------|-------------|
| `COORDINATOR_HOST` | all | `coordinator` | Hostname (or IP) where coordinator is reachable by all nodes. |
| `COORDINATOR_PORT` | all | `8080` | gRPC port exposed by coordinator service. |
| `DATANODE_HOST` | datanode | container hostname | Address advertised to coordinator and peers – must be reachable by all nodes. |
| `DATANODE_PORT` | datanode | `8081` | gRPC port that the DataNode listens on for client and peer connections. |

Environment variables are read via helper functions in `pkg/utils/env.go` and used primarily for:
- **Service Discovery:** How nodes find the coordinator
- **Node Registration:** How nodes advertise themselves to the cluster
- **Docker Integration:** Container hostname resolution in Docker Compose environments

---

## Current Configuration (Environment Variables)

The system currently uses environment variables for all configuration. These provide the essential settings needed for operation:

### Core Environment Variables

| Variable | Component | Default | Description |
|----------|-----------|---------|-------------|
| `COORDINATOR_HOST` | all | `coordinator` | Hostname (or IP) where coordinator is reachable by all nodes. |
| `COORDINATOR_PORT` | all | `8080` | gRPC port exposed by coordinator service. |
| `DATANODE_HOST` | datanode | container hostname | Address advertised to coordinator and peers – must be reachable by all nodes. |
| `DATANODE_PORT` | datanode | `8081` | gRPC port that the DataNode listens on for client and peer connections. |
| `LOG_LEVEL` | all | `error` | Log verbosity level (debug, info, warn, error). |

### Current Default Values

Many of the Host/Port values are hardcoded. This is 'OK' for current e2e testing with Docker, this is an area that requires intensive work for future releases. 

The system uses these hardcoded defaults when environment variables are not provided:

#### Coordinator Defaults
- **ID**: Auto-generated UUID
- **Host**: `localhost`
- **Port**: `8080`
- **Chunk Size**: 8MB (8388608 bytes)
- **Replication Factor**: 3
- **Metadata Commit Timeout**: 15 minutes
- **Max History Size**: 1000 entries

#### DataNode Defaults
- **Streaming Session Timeout**: 5 minutes
- **Replication Timeout**: 10 minutes
- **Storage Root**: `/app/data` (configurable via `DISK_STORAGE_BASE_DIR`)
- **Stream Frame Size**: 256KB (262144 bytes)
- **Max Chunk Retries**: 3
- **Bulk Delete Timeout**: 1 minute
- **Max Concurrent Deletes**: 10

#### Agent Defaults
- **Heartbeat Interval**: 2 seconds
- **Heartbeat Timeout**: 1 second
- **Orphaned Chunks GC Scan Interval**: 15 minutes
- **Orphaned Chunks GC Timeout**: 5 minutes
- **Cleanup Batch Size**: 50 chunks
- **Max Concurrent Deletes**: 10

#### Garbage Collection Defaults
- **Deleted Files GC Interval**: 1 hour
- **Deleted Files GC Timeout**: 1 minute
- **Recovery Timeout**: 2 hours
- **Batch Size**: 1000 files
- **Concurrent Requests**: 5

---

## Planned YAML Configuration Files

Detailed operational configuration will be managed through YAML files located in the `configs/` directory. These files will provide:

### Coordinator Configuration (`coordinator.yaml`)
```yaml
coordinator:
  id: "coordinator-1"
  host: "localhost"
  port: 8080
  chunk_size: 8388608  # 8MB default chunk size
  
  replication:
    factor: 3  # Number of replicas per chunk
    
  metadata:
    commit_timeout: "15m"  # How long to wait for upload confirmation
    
  state:
    max_history_size: 1000  # Node state change history retention

agent:
  deleted_files_gc:
    interval: "1h"           # How often to run garbage collection
    timeout: "1m"            # Timeout for GC operations
    recovery_timeout: "2h"   # Recovery timeout for failed operations
    batch_size: 1000         # Number of files to process per batch
    concurrent_requests: 5   # Number of concurrent delete requests

logging:
  level: "info"
  format: "json"
```

### DataNode Configuration (`datanode.yaml`)
```yaml
node:
  streaming_session:
    session_timeout: "5m"  # Streaming session idle timeout
    
  replication:
    replicate_timeout: "10m"  # Timeout for each replica upload
    
  disk_storage:
    enabled: true
    kind: "block"
    root_dir: "/app/data"  # Configurable via DISK_STORAGE_BASE_DIR
    
  streamer:
    max_chunk_retries: 3
    chunk_stream_size: 262144    # 256 KiB per frame
    backpressure_time: "0s"
    wait_replicas: false  # Only true for client operations
    
  bulk_delete:
    max_concurrent_deletes: 10
    timeout: "1m"

agent:
  heartbeat:
    interval: "2s"        # How often to send heartbeats
    timeout: "1s"         # Heartbeat request timeout
    
  orphaned_chunks_gc:
    inventory_scan_interval: "15m"  # How often to scan for orphaned chunks
    timeout: "5m"                   # Timeout for cleanup operations
    cleanup_batch_size: 50          # Number of chunks to process per batch
    max_concurrent_deletes: 10      # Maximum concurrent delete operations
    
logging:
  level: "info"
  format: "json"
```

### Client Configuration (`client.yaml`)
```yaml
client:
  uploader:
    num_workers: 10       # Concurrent upload workers
    chunk_retry_count: 3
    
  downloader:
    num_workers: 10       # Concurrent download workers  
    chunk_retry_count: 3
    temp_dir: "/tmp"      # Temporary file storage
    
  client_pool:
    max_retries: 3        # Maximum retry attempts for failed connections
    retry_delay: "1s"     # Delay between retry attempts
    failover_timeout: "5s" # Timeout for failover to next node
    
  streamer:
    chunk_stream_size: 262144
    wait_replicas: true   # Wait for replica confirmation
    
logging:
  level: "info"
  format: "json"
```

> **Note**: Client configuration is not yet implemented in the config package. The client currently uses hardcoded defaults and environment variables only.

---

## Current Configuration Loading

The system currently loads configuration using the following precedence (highest to lowest):

1. **Environment Variables** - Override any default setting
2. **Built-in Defaults** - Hardcoded fallback values

### Planned Configuration Loading (Future)

When YAML configuration files are implemented, the precedence will be:

1. **Environment Variables** - Override any config file setting
2. **Explicit Config File** - Specified via command line flag
3. **Default Config File** - `configs/<component>.yaml`
4. **Built-in Defaults** - Hardcoded fallback values

### Current Environment Variable Usage
Environment variables are used directly for configuration:

```bash
# Service discovery
COORDINATOR_HOST=localhost
COORDINATOR_PORT=8080

# Node registration
DATANODE_HOST=localhost
DATANODE_PORT=8081

# Logging
LOG_LEVEL=debug
```

### Planned Environment Variable Override Pattern (Future)
When YAML configuration files are implemented, any YAML setting can be overridden using environment variables with this pattern:
```bash
# Override coordinator.chunk_size setting
DFS_COORDINATOR_CHUNK_SIZE=16777216

# Override node.streamer.chunk_stream_size setting  
DFS_NODE_STREAMER_CHUNK_STREAM_SIZE=524288
```

---

## Session Management Configuration

The system manages two types of sessions with different configuration requirements:

### Streaming Sessions (DataNode)
```yaml
node:
  streaming_session:
    session_timeout: "5m"  # How long before idle streams are cleaned up
```

### Metadata Sessions (Coordinator)  
```yaml
coordinator:
  metadata:
    commit_timeout: "15m"  # How long to wait for upload confirmation
```

> **Note**: Session cleanup intervals and max concurrent sessions are not yet configurable in the current implementation.

---

## Example Configuration Files

### Development Environment (`.env`)
```env
# Service Discovery
COORDINATOR_HOST=localhost
COORDINATOR_PORT=8080

# Node Registration
DATANODE_HOST=localhost
DATANODE_PORT=8081

# Logging
DFS_LOGGING_LEVEL=debug
DFS_LOGGING_FORMAT=text
```

### Docker Compose Environment
```env
# Service Discovery (using Docker DNS)
COORDINATOR_HOST=coordinator
COORDINATOR_PORT=8080

# Node Registration (using container names)
DATANODE_HOST=datanode1
DATANODE_PORT=8081

# Production Settings
DFS_COORDINATOR_CHUNK_SIZE=16777216    # 16MB chunks for larger files
DFS_NODE_REPLICATION_TIMEOUT=5m        # Longer timeout for slower networks
```

### Production Environment
```yaml
# coordinator-prod.yaml
coordinator:
  id: "coord-prod-01"
  port: 8080
  chunk_size: 16777216  # 16MB for production workloads
  
  metadata:
    commit_timeout: "10m"  # Longer timeout for large uploads
    
cluster_state:
  max_history_size: 1000   # More history for production monitoring

logging:
  level: "warn"
  format: "json"
  output: "/var/log/dfs/coordinator.log"
```

---

## Current Configuration Validation

Configuration validation is performed using `github.com/go-playground/validator/v10` with:

- **Required fields** validation
- **Range checks** for numeric values (timeouts, sizes, worker counts)
- **Format validation** for durations, file paths, network addresses
- **Cross-field validation** for dependent settings

### Validation Rules

The configuration uses the following validation rules:
- **Hostnames**: Must be valid RFC1123 hostnames
- **Ports**: Must be between 1-65535
- **Timeouts**: Must be greater than 0
- **Sizes**: Must be greater than 0
- **Required fields**: Marked with `validate:"required"`

### Planned Configuration Validation (Future)

- **Required fields** validation
- **Range checks** for numeric values (timeouts, sizes, worker counts)
- **Format validation** for durations, file paths, network addresses
- **Cross-field validation** for dependent settings

Default values are designed to work in development environments while being easily tunable for production deployments.

---

## Configuration Hot-Reload

**Current Status:** Configuration changes require service restart. The system uses Viper for configuration loading with environment variable support.

**Planned Enhancement:** Hot-reload capability for non-critical settings like:
- Log levels
- Timeout values  
- Worker pool sizes
- Session cleanup intervals

Critical settings (ports, storage paths, cluster topology) will continue to require restart for safety.

---

## Logging

All binaries (coordinator, datanode, tests & CLI) now share one environment variable that tunes log verbosity:

```bash
LOG_LEVEL=debug   # debug | info | warn | error (default=error)
```

If the variable is omitted the services fall back to `error`; the Docker-compose test harness sets `LOG_LEVEL=info` for developer convenience.

---

## Current Default Timeouts

| Component | Setting | Value | Description |
|-----------|---------|-------|-------------|
| **Streaming session** | `node.streaming_session.session_timeout` | 5 m | How long before idle streams are cleaned up |
| **Replication** | `node.replication.replicate_timeout` | 10 m | Timeout for each replica upload |
| **Metadata commit** | `coordinator.metadata.commit_timeout` | 15 m | How long to wait for upload confirmation |
| **Heartbeat interval** | `agent.heartbeat.interval` | 2 s | How often to send heartbeats |
| **Heartbeat timeout** | `agent.heartbeat.timeout` | 1 s | Heartbeat request timeout |
| **Orphaned chunks GC scan** | `agent.orphaned_chunks_gc.inventory_scan_interval` | 15 m | How often to scan for orphaned chunks |
| **Deleted files GC** | `agent.deleted_files_gc.interval` | 1 h | How often to run garbage collection |

> **Note**: These values are optimized for low-latency environments. Consider increasing timeouts for high-latency networks.

---

## Loading Configuration in Code

### Current Usage
```bash
# Run with default configuration
./coordinator

# Run with environment variable overrides
COORDINATOR_PORT=9090 ./coordinator

# Run with custom logging level
LOG_LEVEL=debug ./datanode
```

### Planned Usage (Future)
```bash
# Load from default config file
./coordinator

# Load from custom config file
./coordinator --config=/path/to/custom-config.yaml

# Load with environment overrides
DFS_COORDINATOR_PORT=9090 ./coordinator
```

See `internal/config/` package for configuration loading implementation and validation logic.