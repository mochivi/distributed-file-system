# Configuration Reference

DFS services are configured through a combination of **environment variables** (for bootstrap and service discovery) and **YAML configuration files** (for detailed operational settings). All variables are optional with sensible defaults that allow the demo cluster to start with minimal configuration.

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

## YAML Configuration Files

Detailed operational configuration is managed through YAML files located in the `configs/` directory. These files provide:

### Coordinator Configuration (`coordinator.yaml`)
```yaml
coordinator:
  id: "coordinator-1"
  port: 8080
  chunk_size: 8388608  # 8MB default chunk size
  
  metadata:
    commit_timeout: "5m"  # How long to wait for upload confirmation
    
cluster_state:
  max_history_size: 100   # Node state change history retention

logging:
  level: "info"
  format: "json"
```

### DataNode Configuration (`datanode.yaml`)
```yaml
node:
  disk_storage:
    root_dir: "/data/chunks"
    
  replication:
    timeout: "2m"         # Timeout for each replica upload
    max_retries: 3
    
  session:
    timeout: "1m"         # Streaming session idle timeout
    cleanup_interval: "30s"
    
  streamer:
    chunk_stream_size: 262144    # 256 KiB per frame
    max_retries: 3
    backpressure_delay: "0s"
    wait_replicas: true

agent:
  heartbeat:
    interval: "30s"       # How often to send heartbeats
    timeout: "10s"        # Heartbeat request timeout
    
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
    
  streamer:
    chunk_stream_size: 262144
    wait_replicas: true
    
logging:
  level: "info"
  format: "json"
```

---

## Configuration Loading

Configuration files are loaded using the following precedence (highest to lowest):

1. **Environment Variables** - Override any config file setting
2. **Explicit Config File** - Specified via command line flag
3. **Default Config File** - `configs/<component>.yaml`
4. **Built-in Defaults** - Hardcoded fallback values

### Environment Variable Override Pattern
Any YAML setting can be overridden using environment variables with this pattern:
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
  session:
    timeout: "1m"           # How long before idle streams are cleaned up
    cleanup_interval: "30s" # How often to check for expired sessions
    max_concurrent: 100     # Maximum concurrent streams per node
```

### Metadata Sessions (Coordinator)  
```yaml
coordinator:
  metadata:
    commit_timeout: "5m"    # How long to wait for upload confirmation
    session_cleanup: "1m"   # How often to clean expired sessions
    max_pending: 1000       # Maximum pending upload sessions
```

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

## Validation and Defaults

Configuration validation is performed using `github.com/go-playground/validator/v10` with:

- **Required fields** validation
- **Range checks** for numeric values (timeouts, sizes, worker counts)
- **Format validation** for durations, file paths, network addresses
- **Cross-field validation** for dependent settings

Default values are designed to work in development environments while being easily tunable for production deployments.

---

## Configuration Hot-Reload

**Current Status:** Configuration changes require service restart.

**Planned Enhancement:** Hot-reload capability for non-critical settings like:
- Log levels
- Timeout values  
- Worker pool sizes
- Session cleanup intervals

Critical settings (ports, storage paths, cluster topology) will continue to require restart for safety.

---

## Loading Configuration in Code

```bash
# Load from default config file
./coordinator

# Load from custom config file
./coordinator --config=/path/to/custom-config.yaml

# Load with environment overrides
DFS_COORDINATOR_PORT=9090 ./coordinator
```

See `internal/config/` package for configuration loading implementation and validation logic.