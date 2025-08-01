# Distributed File System (DFS) – Go Implementation

[![Go](https://img.shields.io/badge/go-1.24+-00ADD8?logo=go)](https://golang.org/) [![CI](https://github.com/mochivi/distributed-file-system/actions/workflows/go-tests.yml/badge.svg)](https://github.com/mochivi/distributed-file-system/actions) [![License](https://img.shields.io/github/license/mochivi/distributed-file-system)](LICENSE)

A **distributed file system** written in Go that demonstrates chunk-based storage, streaming gRPC replication, and metadata coordination. Designed as a learning project with production-ready architectural patterns.

> 📖 **Complete Documentation:** [System Design](docs/design.md) | [Architecture](docs/architecture.md) | [Protocol](docs/protocol.md) | [Configuration](docs/configuration.md) | [Development Roadmap](docs/roadmap.md)

---

## Table of Contents
1. [Features](#features)
2. [Prerequisites](#prerequisites)
3. [Quick Start](#quick-start)
4. [Architecture Overview](#architecture-overview)
5. [Configuration](#configuration)
6. [Project Structure](#project-structure)
7. [Development](#development)
8. [Testing](#testing)
9. [Documentation](#documentation)
10. [Roadmap](#roadmap)

---

## Features

### Core Functionality ✅
* **Upload/Download Operations**: Complete file transfer with chunking and parallel processing
* **Delete**: Implemented file deletion (with GC) and recursive directory listing APIs
* **Chunk-based Storage**: 8MB default chunks with configurable sizes up to 64MB
* **Data Replication**: Default factor of 3 (1 primary + 2 replicas) with parallel replication
* **Streaming Protocol**: Bidirectional (upload) and unidirectional (download) gRPC streams with back-pressure control and SHA-256 checksums
* **Node Management**: Automatic registration, heartbeat monitoring, and cluster state management
* **Resilient Client Connections**: Rotating client pool with automatic failover over multiple DataNodes
* **Session Management**: Two session types (streaming + metadata) ensuring operation atomicity

### Architecture Highlights
* **Coordinator Service**: Metadata-only service (file → chunk mapping, cluster membership)
* **Datanode Service**: Distributed storage nodes with peer-to-peer parallel replication
* **Client SDK**: Go SDK with parallel upload/download
* **Protocol Buffers**: Efficient serialization over gRPC (HTTP/2)
* **Docker Integration**: Full e2e test environment with 1 coordinator + 6 datanodes

### Planned Features 🚧
* **File Listing**: Clients must be able to list stored files
* **Persistent Metadata**: etcd-based (or similar) metadata storage (currently in-memory on coordinator)
* **Garbage Collection Cycles**: Partially implemented, require persistent metadata and testing
* **Observability**: Metrics, tracing, and operational monitoring
* **Enhanced Security**: TLS, JWT authentication, RBAC access control
* **Chunk encryption**: Chunk encryption storage options
* **API Gateway**: HTTP/REST interface with authentication and authorization

> **See Complete Feature List:** [docs/roadmap.md](docs/roadmap.md) | **Technical Details:** [docs/architecture.md](docs/architecture.md)

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| **Go** | 1.24.4+ | Building binaries and running unit tests |
| **Docker** | 20.10+ | Local multi-node cluster and e2e testing |
| **Protocol Buffers** | Latest | Required only when modifying `.proto` files |

### Development Tools (Optional)
```bash
# Install protobuf tools for gRPC development
make dev-setup
```

---

## Quick Start

### Launch Full Cluster
```bash
# Start 1 coordinator + 6 datanodes with e2e tests
make e2e
```

### Monitor and Manage
```bash
# View aggregated logs
tail -f ./logs/e2e_run.log

# Stop and cleanup
make e2e-down
```

### Development Workflow
```bash
# Run unit tests
make test

# Generate protobuf (after .proto changes)
make clean && make proto
```

> **Detailed Commands:** [PROJECT.mdc](PROJECT.mdc) | **Architecture Guide:** [docs/architecture.md](docs/architecture.md)

---

## Current Architecture Overview

```mermaid
flowchart TB
    subgraph "Client"
        CLI["CLI Client"]
        SDK["Go SDK"]
    end

    subgraph "Control Plane"
        COORD["Coordinator<br/>• Metadata Management<br/>• Node Selection<br/>• Chunk Planning"]
    end

    subgraph "Data Plane"
        DN1["DataNode 1<br/>• Chunk Storage<br/>• Replication<br/>• Heartbeat Loop"]
        DN2["DataNode 2"]
        DN3["DataNode N"]
    end

    CLI --> COORD
    SDK --> COORD
    CLI --> DN1
    SDK --> DN1

    DN1 --> DN2
    DN1 --> DN3
    DN2 --> DN3

    DN1 --> COORD
    DN2 --> COORD
    DN3 --> COORD

    style COORD fill:#e1f5fe
    style DN1 fill:#f3e5f5
    style DN2 fill:#f3e5f5
    style DN3 fill:#f3e5f5
```

### Planned architecture design
```mermaid
flowchart TB
    subgraph "External Access"
        WEB["Web Dashboard"]
        MOBILE["Mobile App"]
        API_CLIENT["API Clients"]
    end

    subgraph "API Gateway Layer"
        GATEWAY["API Gateway<br/>• HTTP/REST API<br/>• Authentication<br/>• Rate Limiting<br/>• Load Balancing"]
        ADMIN_API["Admin API<br/>• Cluster Management<br/>• User Management<br/>• System Monitoring"]
    end

    subgraph "Authentication & Authorization"
        AUTH["Auth Service<br/>• JWT Tokens<br/>• RBAC<br/>• API Keys"]
        IAM["Identity Management<br/>• User Accounts<br/>• Permissions<br/>• Audit Logs"]
    end

    subgraph "Control Plane"
        COORD_CLUSTER["Coordinator Cluster<br/>• Leader Election<br/>• Metadata Consensus<br/>• Failover"]
        ETCD["etcd Cluster<br/>• Metadata Storage<br/>• Configuration<br/>• Service Discovery"]
    end

    subgraph "Data Plane"
        subgraph "Zone A"
            DNA1["DataNode A1"]
            DNA2["DataNode A2"]
        end
        subgraph "Zone B"
            DNB1["DataNode B1"]
            DNB2["DataNode B2"]
        end
        subgraph "Zone C"
            DNC1["DataNode C1"]
            DNC2["DataNode C2"]
        end
    end

    subgraph "Storage Backends"
        LOCAL["Local Disk"]
        S3["S3 Compatible"]
        GCS["Google Cloud Storage"]
        AZURE["Azure Blob Storage"]
    end

    subgraph "Observability"
        METRICS["Metrics<br/>• Prometheus*<br/>• Custom Metrics"]
        TRACING["Tracing<br/>• OpenTelemetry*<br/>"]
        LOGS["Logging<br/>• Structured Logs<br/>• Log Aggregation"]
    end

    WEB --> GATEWAY
    MOBILE --> GATEWAY
    API_CLIENT --> GATEWAY

    GATEWAY --> AUTH
    ADMIN_API --> AUTH
    AUTH --> IAM

    GATEWAY --> COORD_CLUSTER
    ADMIN_API --> COORD_CLUSTER
    COORD_CLUSTER --> ETCD

    COORD_CLUSTER --> DNA1
    COORD_CLUSTER --> DNA2
    COORD_CLUSTER --> DNB1
    COORD_CLUSTER --> DNB2

    DNA1 --> LOCAL
    DNA2 --> S3
    DNB1 --> GCS
    DNB2 --> AZURE

    DNA1 --> METRICS
    DNA2 --> TRACING
    COORD_CLUSTER --> LOGS

    style GATEWAY fill:#e8f5e8
    style AUTH fill:#fff3e0
    style COORD_CLUSTER fill:#e1f5fe
    style ETCD fill:#fce4ec
    style METRICS fill:#f3e5f5
```

### Key Concepts
- **Chunks**: Files split into 8MB pieces with SHA-256 checksums
- **Replication**: Each chunk stored on N nodes
- **Sessions**: Dual session management (streaming + metadata) for atomicity
- **Heartbeats**: N-second intervals with incremental cluster state updates

> **Complete Architecture:** [docs/architecture.md](docs/architecture.md) | **Protocol Details:** [docs/protocol.md](docs/protocol.md)

---

## Configuration

The system uses **environment variables** for service discovery and **YAML files** for operational configuration:

### Service Discovery (Environment Variables)
```bash
# Coordinator location (required by all nodes)
COORDINATOR_HOST=coordinator
COORDINATOR_PORT=8080

# New: global log level (default=error, dev=info)
LOG_LEVEL=info

# Node registration (required by datanodes)  
DATANODE_HOST=datanode1
DATANODE_PORT=8081
```

### Operational Settings (YAML Configuration)
```yaml
# configs/coordinator.yaml
coordinator:
  chunk_size: 8388608      # 8MB default
  metadata:
    commit_timeout: "5m"

# configs/datanode.yaml  
node:
  replication:
    timeout: "2m"
  session:
    timeout: "1m"
```

> **Complete Configuration Guide:** [docs/configuration.md](docs/configuration.md)

---

## Project Structure

```
dfs/
├── cmd/                   # Entry points
│   ├── coordinator/       # Coordinator service main
│   ├── datanode/          # DataNode service main
│   └── client/            # Client CLI main
├── internal/              # Private application code
│   ├── client/            # Client SDK (uploader, downloader)
│   ├── clients/           # gRPC client wrappers
│   ├── cluster/           # Node management & selection
│   ├── common/            # Shared types, proto -> internal type conversion & validation
│   ├── config/            # Configuration management
│   ├── coordinator/       # Coordinator business logic
│   ├── datanode/          # DataNode business logic
│   └── storage/           # Storage interfaces and implementations
│       ├── chunk/         # Chunk storage (disk-based)
│       ├── encoding/      # Serialization interface and protocol (protobuf)
│       └── metadata/      # Metadata storage (currently in-memory)
├── pkg/                   # Public library code
│   ├── proto/             # Generated protobuf files
│   ├── logging/           # Structured logging utilities
│   ├── streamer/          # Streaming utilities
│   ├── utils/             # General utilities, small functions, prototyping
│   ├── client_pool/       # Client connection pools (rotating etc.)
│   ├── testutils/         # Test utilities
├── tests/                 # Test files
│   └── e2e/              # End-to-end tests
├── configs/              # Example yaml configuration files
├── deploy/               # Deployment configurations
├── docs/                 # Documentation
└── logs/                 # Log output directory, local testing
```

> **Detailed Structure:** [PROJECT.mdc](PROJECT.mdc)

---

## Development

### Making Changes
```bash
# 1. Feature development
make test                   # Run unit tests
make e2e                    # Run full integration tests

# 2. Protocol changes
make clean && make proto    # Regenerate after .proto edits
make test                   # Verify changes
```

---

## Testing

### Test Coverage
- **Unit Tests**: `make test` - >80% coverage target with race detection
- **End-to-End**: `make e2e` - Full cluster scenarios with varying file sizes
- **Integration**: Component interaction testing

### Test Environment
```bash
# Local development
make test

# Full cluster simulation  
make e2e

# Continuous integration
# All tests run automatically on pull requests
```

### Debugging Tests
```bash
# Enable debug logging - modify in .env file
LOG_LEVEL=info # debug, info, warn, error
ENVIRONMENT=development # development, production
DEBUG_E2E_TESTS=false # true, false

# View specific logs
docker logs dfs_coordinator_1
docker logs dfs_datanode_1
```

> **Testing Strategy:** [docs/architecture.md#testing-infrastructure](docs/architecture.md)

---

## Documentation

### Complete Documentation Index

| Document | Purpose | Audience |
|----------|---------|----------|
| **[README.md](README.md)** | Project overview and quick start | All users |
| **[docs/design.md](docs/design.md)** | System design and future architecture | Architects, senior developers |
| **[docs/architecture.md](docs/architecture.md)** | Current implementation details | Developers, contributors |
| **[docs/protocol.md](docs/protocol.md)** | Wire protocol and API specifications | Protocol developers |
| **[docs/configuration.md](docs/configuration.md)** | Configuration reference and examples | Operators, DevOps |
| **[docs/roadmap.md](docs/roadmap.md)** | Development phases and feature planning | Project managers, stakeholders |
| **[PROJECT.mdc](PROJECT.mdc)** | Developer reference and AI assistant guide | Developers, AI tools |

### Quick Navigation
- **🚀 Getting Started**: [Prerequisites](#prerequisites) → [Quick Start](#quick-start)
- **🏗️ Understanding the System**: [Architecture Overview](#architecture-overview) → [docs/architecture.md](docs/architecture.md)
- **⚙️ Configuration**: [Configuration](#configuration) → [docs/configuration.md](docs/configuration.md)
- **🔧 Development**: [Development](#development) → [PROJECT.mdc](PROJECT.mdc)
- **📋 Planning**: [Roadmap](#roadmap) → [docs/roadmap.md](docs/roadmap.md)

---

## Roadmap

### Current Development Status
- ✅ **Phase 0**: Core file operations (upload/download) with replication
- 🚧 **Phase 1**: Missing file operations (delete/list) and persistent metadata
- 📋 **Phase 2**: Enhanced CLI and HTTP API gateway
- 📋 **Phase 3**: Security and authentication
- 📋 **Phase 4**: Performance optimization and scaling
- 📋 **Phase 5**: Observability and operations

### Immediate Next Steps
1. **Complete DeleteFile and ListFiles** operations
2. **Implement persistent metadata storage** (etcd integration)
3. **Add garbage collection** for orphaned chunks
4. **Enhanced CLI interface** with user-friendly commands

### Long-term Vision
- **Production-ready distributed file system** with enterprise features
- **Multi-cloud storage backends** (S3, GCS, Azure)
- **Advanced features**: Encryption, compression, deduplication
- **Operational excellence**: Monitoring, alerting, automated operations

> **Detailed Roadmap**: [docs/roadmap.md](docs/roadmap.md) | **Technical Planning**: [docs/design.md](docs/design.md)

---

## Contributing

Contribution acceptance is planned for August/September.
* Discord communitity: [Go DFS](https://discord.gg/pF3xG6Wb)
* Contribution guidelines
* Repository configuration

## License


This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
