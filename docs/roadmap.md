# Distributed File System - Development Roadmap

## Current State Analysis

### ✅ Implemented Features

**Core Architecture**
- **Coordinator Service**: Manages metadata (file → chunk mapping) and cluster state (node membership)
- **DataNode Service**: Stores chunk data on disk with replication to peers
- **Client SDK**: Handles file chunking, parallel uploads/downloads, and metadata confirmation
- **gRPC Streaming**: Bidirectional streaming with back-pressure control and checksums

**File Operations**
- **Upload**: Complete implementation with chunking, replication, and metadata commit only upon client confirmation
- **Download**: Complete implementation with chunk retrieval from multiple possible replica nodes
- **Node Management**: Registration, heartbeat, and incremental cluster state updates with versioning

**Storage Layer**
- **Chunk Storage**: Disk-based storage with nested directory structure (avoids filesystem limitations)
- **Serialization**: Protobuf-based chunk header serialization with data integrity checks
- **Replication**: Parallel replication to multiple nodes with configurable timeout

**Testing & Infrastructure**
- **Unit Tests**: Comprehensive coverage for core components (coordinator, datanode, client)
- **E2E Tests**: Upload functionality with varying file sizes and chunk sizes
- **Docker Compose**: Local development environment with 1 coordinator + 6 datanodes

### ❌ Missing/Incomplete Features

**Critical Gaps**
1. **Metadata Persistence**: Currently in-memory only (lost on coordinator restart)
2. **Garbage Collection**: GCs are not fully implemented or tested, require persistent metadata
3. **CLI Interface**: Basic client exists but lacks user-friendly commands

**Operational Features**
1. **File Listing**: Clients must be able to list stored files
2. **API Gateway**: Entrypoint into the system, transcoding HTTP -> gRPC
3. **Observability**: No metrics, tracing, or structured monitoring
4. **Configuration**: Runtime configuration needs improvement (currently compile-time constants)
5. **Capacity Management**: No load balancing or storage rebalancing
6. **Self Healing**: Coordinator must be aware of nodes dropping for replication consistency

**Security & Reliability**
1. **Authentication**: No TLS, JWT, or mTLS implementation
2. **Authorization**: No access control or user management
3. **Data Integrity**: No at-rest encryption or erasure coding

---

## Development Phases

### Phase 1: Core Functionality Completion

**Priority 1: Essential File Operations**
- [x] **Implement DeleteFile operation**
  - [x] Add coordinator logic for file deletion
  - [x] Implement chunk cleanup on datanodes
  - [x] Add client SDK support
  - [x] Write comprehensive tests

- [ ] **Implement ListFiles operation**
  - [ ] Add directory listing in coordinator
  - [ ] Implement metadata querying
  - [ ] Add pagination support
  - [ ] Create client SDK methods

- [ ] **Persistent Metadata Storage**
  - [ ] Choose distributed key-value of choice -> etcd?

**Priority 2: Garbage Collection**
- [ ] **Orphaned Chunk Detection**
  - [x] Add periodic cleanup jobs -> if some chunk is not referenced in the metadata, it is marked for deletion, actually deleted after a certain period (grace period for file recovery).
  - [ ] Create chunk verification tools -> integrity verification of chunks
  - [ ] Add tests

- [ ] **Deleted Files Cleanup**
  - [x] Add gargabe collection cycle to check for files marked for deletion and emit requests for datanodes for deletion.
  - [ ] Add tests

**Priority 3: Enhanced Testing**
- [ ] **E2E Test Coverage**
  - [x] Add download operation tests
  - [x] Add delete operation tests
  - [ ] Add list operation tests
  - [ ] Add failure scenario tests

## **Phases listed below are subject to large changes, most are quick ideas of new features**

### Phase 2: User Experience & CLI

**Enhanced CLI Interface**
- [ ] **User-Friendly Commands**
  - `dfs connect <HOST:PORT>` -> authentication options tbd -> required gateway API and RBAC
  - `dfs upload <file> <remote-path>`
  - `dfs download <remote-path> <local-path>`
  - `dfs ls <remote-path>`
  - `dfs rm <remote-path>`
  - `dfs status` (cluster health)

- [ ] **CLI Features**
  - Progress bars for uploads/downloads
  - Batch operations
  - Configuration management
  - Help documentation

**API Gateway**
- [ ] **REST API Implementation**
  - File upload/download endpoints
  - Directory listing API
  - Cluster status endpoints
  - OpenAPI documentation

### Phase 3: Security & Reliability

**Security Implementation**
- [ ] **Transport Security**
  - TLS configuration for all gRPC connections
  - Certificate management
  - Secure key exchange

- [ ] **Authentication & Authorization**
  - JWT-based authentication
  - Role-based access control (RBAC)
  - User management system
  - API key management

**Enhanced Reliability**
- [ ] **Failure Recovery**
  - Automatic node failure detection
  - Chunk re-replication on node failure
  - Coordinator failover mechanism
  - Data consistency checks

- [ ] **Data Integrity**
  - At-rest encryption options
  - Chunk corruption detection
  - Automatic repair mechanisms
  - Backup strategies

### Phase 4: Performance & Scale

**Performance Optimization**
- [ ] **Load Balancing**
  - Intelligent node selection
  - Capacity-aware chunk placement -> requires some sort of system resources package
  - Dynamic rebalancing
  - Performance monitoring

- [ ] **Advanced Storage Features**
  - Erasure coding experiments
  - Compression options
  - Deduplication capabilities
  - Tiered storage support

**Configuration Management**
- [ ] **Runtime Configuration**
  - Make replication factor configurable
  - Dynamic configuration updates
  - Environment-specific configs
  - Configuration validation

### Phase 5: Observability & Operations

**Monitoring & Observability**
- [ ] **Metrics Collection**
  - Prometheus metrics export -> TBD
  - Custom dashboards (Grafana) -> TBD
  - Alert rules configuration
  - Performance tracking

- [ ] **Distributed Tracing**
  - OpenTelemetry integration
  - Request flow visualization
  - Performance bottleneck identification
  - Error correlation

**Operational Tools**
- [ ] **Administration Interface**
  - Web-based admin panel
  - Node management tools
  - Storage analytics
  - Health monitoring

### Phase 6: Advanced Features

**Enhanced Storage**
- [ ] **Multi-Backend Support**
  - S3-compatible storage adapters
  - Google Cloud Storage integration
  - Azure Blob Storage support
  - Hybrid storage configurations

**Advanced Operations**
- [ ] **Cluster Management**
  - Automated node provisioning
  - Rolling updates
  - Backup/restore operations
  - Disaster recovery procedures

**Integration & Extensibility**
- [ ] **Plugin Architecture**
  - Custom storage backends
  - Authentication providers
  - Compression algorithms
  - Monitoring integrations

---

## Technical Debt & Improvements

### Code Quality
- [ ] **Code Organization**
  - Refactor large functions in `cmd/` directories
  - Improve error handling patterns
  - Add comprehensive documentation
  - Standardize logging practices

- [ ] **Performance Optimizations**
  - Optimize memory usage in streaming operations
  - Improve concurrent upload/download performance
  - Add connection pooling
  - Implement smart retry mechanisms

### Testing Infrastructure
- [ ] **Test Coverage**
  - Increase unit test coverage to >80%
  - Add integration tests for all components
  - Create performance benchmarks
  - Add chaos engineering tests

### Documentation
- [ ] **User Documentation**
  - Getting started guide
  - API reference documentation
  - Deployment guides
  - Troubleshooting manual

- [ ] **Developer Documentation**
  - Architecture decision records (ADRs)
  - Contributing guidelines
  - Code review checklist
  - Release process documentation
---

*This roadmap is a living document and should be updated as the project evolves and requirements change.* 