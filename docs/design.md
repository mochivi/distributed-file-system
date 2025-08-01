# Distributed File System (DFS) – System Design

## Overview

This document outlines the comprehensive system design for the DFS project, covering both the current implementation and the planned architecture evolution. The design follows a microservices approach with clear separation of concerns and scalable components.

## Design Principles

- **Scalability**: Horizontal scaling through node addition
- **Reliability**: Data replication and fault tolerance
- **Security**: End-to-end encryption and RBAC
- **Observability**: Comprehensive monitoring and tracing
- **Modularity**: Pluggable components and interfaces

---

## Current Architecture

### Core Components

```mermaid
flowchart TB
    subgraph "Client Layer"
        CLI["CLI Client"]
        SDK["Go SDK"]
        CPOOL["Client Pool<br/>• Rotating connections<br/>• Failover handling<br/>• Retry logic"]
    end

    subgraph "Control Plane"
        COORD["Coordinator<br/>• Metadata Management<br/>• Node Selection<br/>• Chunk Planning<br/>• Session Management"]
    end

    subgraph "Data Plane"
        DN1["DataNode 1<br/>• Chunk Storage<br/>• Replication<br/>• Heartbeat Loop<br/>• Session Management"]
        DN2["DataNode 2"]
        DN3["DataNode 3"]
        DN4["DataNode 4"]
        DNn["DataNode N"]
    end

    subgraph "Client Operations"
        UPLOADER["Uploader<br/>• Chunk planning<br/>• Replication coordination<br/>• Session management"]
        DOWNLOADER["Downloader<br/>• Chunk assembly<br/>• Client pool integration<br/>• Error handling"]
    end

    subgraph "Streaming Layer"
        STREAMING["Streaming Package<br/>• Bidirectional streams<br/>• Server-side streams<br/>• Flow control<br/>• Error handling"]
    end

    subgraph "Storage Layer"
        DISK1[("Local Disk")]
        DISK2[("Local Disk")]
        DISK3[("Local Disk")]
    end

    CLI --> CPOOL
    SDK --> CPOOL
    CPOOL --> COORD
    CPOOL --> DN1
    CPOOL --> DN2

    UPLOADER --> CPOOL
    DOWNLOADER --> CPOOL

    UPLOADER --> STREAMING
    DOWNLOADER --> STREAMING

    DN1 --> DN2
    DN1 --> DN3
    DN2 --> DN3
    DN2 --> DN4

    DN1 --> DISK1
    DN2 --> DISK2
    DN3 --> DISK3

    DN1 --> COORD
    DN2 --> COORD
    DN3 --> COORD
    DN4 --> COORD
    DNn --> COORD

    style COORD fill:#e1f5fe
    style DN1 fill:#f3e5f5
    style DN2 fill:#f3e5f5
    style DN3 fill:#f3e5f5
    style CPOOL fill:#fff3e0
    style UPLOADER fill:#e8f5e8
    style DOWNLOADER fill:#e8f5e8
    style STREAMING fill:#fce4ec
```

---

## Protocol Integration

### Session Management Architecture

The system implements two distinct session types that align with the[protocol specification](docs/protocol.md):

```mermaid
flowchart TB
    subgraph "Session Types"
        METADATA_SESSION["Metadata Sessions<br/>• Coordinator-managed<br/>• File operation scope<br/>• 5-minute timeout<br/>• Atomic operations"]
        STREAMING_SESSION["Streaming Sessions<br/>• DataNode-managed<br/>• Chunk transfer scope<br/>• 1-minute timeout<br/>• Bidirectional/Server-side"]
    end

    subgraph "Session Lifecycle"
        CREATE["Session Creation<br/>• UUID generation<br/>• State initialization<br/>• Timeout setup"]
        ACTIVE["Active Session<br/>• Request processing<br/>• State updates<br/>• Heartbeat monitoring"]
        CLEANUP["Session Cleanup<br/>• State cleanup<br/>• Resource release<br/>• Timeout handling"]
    end

    subgraph "Session Management"
        COORD_SESSION["Coordinator Session Manager<br/>• Metadata session tracking<br/>• File operation coordination<br/>• Atomic commit handling"]
        DN_SESSION["DataNode Session Manager<br/>• Streaming session tracking<br/>• Chunk transfer coordination<br/>• Flow control management"]
    end

    METADATA_SESSION --> CREATE
    STREAMING_SESSION --> CREATE
    CREATE --> ACTIVE
    ACTIVE --> CLEANUP

    COORD_SESSION --> METADATA_SESSION
    DN_SESSION --> STREAMING_SESSION

    style METADATA_SESSION fill:#e1f5fe
    style STREAMING_SESSION fill:#f3e5f5
    style CREATE fill:#e8f5e8
    style ACTIVE fill:#fff3e0
    style CLEANUP fill:#ffebee
```

### Replication Protocol Implementation

The replication protocol is implemented through a multi-tier architecture:

```mermaid
flowchart TB
    subgraph "Replication Flow"
        CLIENT_UPLOAD["Client Upload<br/>• PrepareChunkUpload<br/>• UploadChunkStream<br/>• propagate=true"]
        PRIMARY_REPLICATE["Primary Replication<br/>• PrepareChunkUpload<br/>• UploadChunkStream<br/>• propagate=false"]
        REPLICA_STORE["Replica Storage<br/>• Chunk validation<br/>• Checksum verification<br/>• Storage commit"]
    end

    subgraph "Replication Management"
        REPLICA_COORD["Replication Coordinator<br/>• Node selection<br/>• Failure handling<br/>• Consistency checks"]
        REPLICA_MONITOR["Replica Monitor<br/>• Health tracking<br/>• Performance metrics<br/>• Failure detection"]
    end

    CLIENT_UPLOAD --> PRIMARY_REPLICATE
    PRIMARY_REPLICATE --> REPLICA_STORE
    REPLICA_COORD --> PRIMARY_REPLICATE
    REPLICA_MONITOR --> REPLICA_STORE

    style CLIENT_UPLOAD fill:#e8f5e8
    style PRIMARY_REPLICATE fill:#e1f5fe
    style REPLICA_STORE fill:#f3e5f5
```

---

## Future Architecture

### Extended System Design

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

### API Gateway Design

The API Gateway serves as the single entry point for all external requests, providing:

```mermaid
flowchart LR
    subgraph "API Gateway Components"
        ROUTER["Request Router<br/>• Path-based routing<br/>• Protocol translation<br/>• Request validation"]
        AUTH_MW["Auth Middleware<br/>• Token validation<br/>• Permission checks<br/>• Rate limiting"]
        LB["Load Balancer<br/>• Backend selection<br/>• Health checks<br/>• Circuit breaker"]
    end

    subgraph "Backend Services"
        FILE_API["File Operations API<br/>• Upload/Download<br/>• List/Delete<br/>• Metadata queries"]
        ADMIN_API["Admin API<br/>• Node management<br/>• User administration<br/>• System monitoring"]
        CLUSTER_API["Cluster API<br/>• Health status<br/>• Configuration<br/>• Metrics"]
    end

    ROUTER --> AUTH_MW
    AUTH_MW --> LB
    LB --> FILE_API
    LB --> ADMIN_API
    LB --> CLUSTER_API

    style ROUTER fill:#e8f5e8
    style AUTH_MW fill:#fff3e0
    style LB fill:#e1f5fe
```

### Authentication & Authorization Flow

```mermaid
sequenceDiagram
    participant U as User/Client
    participant GW as API Gateway
    participant AUTH as Auth Service
    participant IAM as Identity Management
    participant COORD as Coordinator
    participant DN as DataNode

    Note over U,DN: Authentication Flow

    U->>GW: Login Request (credentials)
    GW->>AUTH: Authenticate(credentials)
    AUTH->>IAM: Validate credentials
    IAM-->>AUTH: User profile + permissions
    AUTH-->>GW: JWT Token + refresh token
    GW-->>U: Authentication response

    Note over U,DN: Authorized Request Flow

    U->>GW: File Operation (JWT token)
    GW->>AUTH: Validate token
    AUTH->>AUTH: Check permissions (RBAC)
    AUTH-->>GW: Authorization result
    
    alt Authorized
        GW->>COORD: Forward request (with user context)
        COORD->>DN: Execute operation
        DN-->>COORD: Operation result
        COORD-->>GW: Response
        GW-->>U: Success response
    else Unauthorized
        GW-->>U: 403 Forbidden
    end
```

### Cluster Management API

```mermaid
flowchart TB
    subgraph "Admin Dashboard"
        DASH["Web Dashboard<br/>• Real-time monitoring<br/>• Node management<br/>• User administration"]
    end

    subgraph "Cluster Management API"
        NODE_MGR["Node Manager<br/>• Add/Remove nodes<br/>• Health monitoring<br/>• Load balancing"]
        USER_MGR["User Manager<br/>• Account creation<br/>• Permission management<br/>• Audit logging"]
        SYSTEM_MGR["System Manager<br/>• Configuration updates<br/>• Backup/Restore<br/>• Performance tuning"]
    end

    subgraph "Monitoring & Alerting"
        ALERTS["Alert Manager<br/>• Health alerts<br/>• Performance alerts<br/>• Security alerts"]
        REPORTS["Report Generator<br/>• Usage statistics<br/>• Performance reports<br/>• Audit reports"]
    end

    DASH --> NODE_MGR
    DASH --> USER_MGR
    DASH --> SYSTEM_MGR

    NODE_MGR --> ALERTS
    USER_MGR --> REPORTS
    SYSTEM_MGR --> ALERTS

    style DASH fill:#e8f5e8
    style NODE_MGR fill:#e1f5fe
    style USER_MGR fill:#fff3e0
    style SYSTEM_MGR fill:#f3e5f5
```

---

## Data Storage Design

### Metadata Architecture

```mermaid
flowchart TB
    subgraph "Metadata Storage Evolution"
        subgraph "Current (v1.0)"
            MEM["In-Memory Storage<br/>• Fast access<br/>• Lost on restart<br/>• Single point of failure"]
        end
        
        subgraph "Planned (v2.0)"
            ETCD_CLUSTER["etcd Cluster<br/>• Distributed consensus<br/>• Automatic failover<br/>• Transaction support"]
            BACKUP["Backup Storage<br/>• Periodic snapshots<br/>• Point-in-time recovery<br/>• Cross-region replication"]
        end
    end

    subgraph "Metadata Schema"
        FILES["File Metadata<br/>• Path mapping<br/>• Access permissions<br/>• Creation timestamps"]
        CHUNKS["Chunk Metadata<br/>• Chunk locations<br/>• Replication status<br/>• Integrity checksums"]
        NODES["Node Metadata<br/>• Health status<br/>• Capacity information<br/>• Performance metrics"]
    end

    MEM -.-> ETCD_CLUSTER
    ETCD_CLUSTER --> BACKUP
    
    ETCD_CLUSTER --> FILES
    ETCD_CLUSTER --> CHUNKS
    ETCD_CLUSTER --> NODES

    style MEM fill:#ffebee
    style ETCD_CLUSTER fill:#e8f5e8
    style BACKUP fill:#e1f5fe
```

### Storage Backend Abstraction

```mermaid
flowchart TB
    subgraph "Storage Interface Layer"
        IFACE["Storage Interface<br/>• Pluggable backends<br/>• Unified API<br/>• Configuration-driven"]
    end

    subgraph "Backend Implementations"
        LOCAL_IMPL["Local Disk Backend<br/>• Nested directories<br/>• Direct file I/O<br/>• Fast access"]
        S3_IMPL["S3 Backend<br/>• Cloud storage<br/>• Infinite capacity<br/>• Cost-effective"]
        HYBRID_IMPL["Hybrid Backend<br/>• Hot/Cold tiers<br/>• Automatic migration<br/>• Cost optimization"]
    end

    subgraph "Storage Features"
        COMPRESS["Compression<br/>• Configurable algorithms<br/>• CPU vs storage trade-off"]
        ENCRYPT["Encryption<br/>• At-rest encryption<br/>• Key management<br/>• Compliance"]
        DEDUP["Deduplication<br/>• Content-based<br/>• Storage efficiency<br/>• Performance impact"]
    end

    IFACE --> LOCAL_IMPL
    IFACE --> S3_IMPL
    IFACE --> HYBRID_IMPL

    LOCAL_IMPL --> COMPRESS
    S3_IMPL --> ENCRYPT
    HYBRID_IMPL --> DEDUP

    style IFACE fill:#e8f5e8
    style LOCAL_IMPL fill:#e1f5fe
    style S3_IMPL fill:#fff3e0
    style HYBRID_IMPL fill:#f3e5f5
```

### Garbage Collection & Cleanup Architecture

```mermaid
flowchart TB
    subgraph "Garbage Collection"
        GC_SCANNER["Garbage Collector<br/>• Periodic scanning<br/>• Orphaned chunk detection<br/>• Metadata validation"]
        CHUNK_INVENTORY["Chunk Inventory<br/>• Local chunk tracking<br/>• Metadata verification<br/>• Storage reconciliation"]
    end

    subgraph "Cleanup Operations"
        INDIVIDUAL_DELETE["Individual Delete<br/>• DeleteChunk(chunkID)<br/>• Per-chunk cleanup<br/>• Success/failure tracking"]
        BULK_DELETE["Bulk Delete<br/>• BulkDeleteChunk(chunkIDs[])<br/>• Batch operations<br/>• Failed chunk reporting"]
        ORPHANED_CLEANUP["Orphaned Cleanup<br/>• Local inventory scan<br/>• Metadata validation<br/>• Automatic cleanup"]
    end

    subgraph "Cleanup Triggers"
        FILE_DELETE["File Deletion<br/>• Coordinator-initiated<br/>• All replica cleanup<br/>• Soft delete marking"]
        GC_TRIGGER["Garbage Collection<br/>• Periodic scanning<br/>• Orphaned detection<br/>• Storage optimization"]
        REBALANCE["Rebalancing<br/>• Storage rebalancing<br/>• Chunk migration<br/>• Capacity optimization"]
    end

    GC_SCANNER --> CHUNK_INVENTORY
    CHUNK_INVENTORY --> ORPHANED_CLEANUP

    FILE_DELETE --> INDIVIDUAL_DELETE
    GC_TRIGGER --> BULK_DELETE
    REBALANCE --> BULK_DELETE

    ORPHANED_CLEANUP --> INDIVIDUAL_DELETE

    style GC_SCANNER fill:#e8f5e8
    style CHUNK_INVENTORY fill:#e1f5fe
    style INDIVIDUAL_DELETE fill:#fff3e0
    style BULK_DELETE fill:#f3e5f5
    style ORPHANED_CLEANUP fill:#ffebee
```

---

## Security Architecture

### Zero-Trust Security Model

```mermaid
flowchart TB
    subgraph "Security Layers"
        TLS["Transport Security<br/>• TLS 1.3 everywhere<br/>• Certificate management<br/>• Mutual authentication"]
        AUTH["Authentication<br/>• JWT tokens<br/>• Multi-factor auth<br/>• API key management"]
        AUTHZ["Authorization<br/>• Role-based access<br/>• Resource permissions<br/>• Policy engine"]
        AUDIT["Audit & Compliance<br/>• Access logging<br/>• Security events<br/>• Compliance reports"]
    end

    subgraph "Data Protection"
        ENCRYPT_TRANSIT["Encryption in Transit<br/>• End-to-end encryption<br/>• Perfect forward secrecy"]
        ENCRYPT_REST["Encryption at Rest<br/>• AES-256 encryption<br/>• Key rotation<br/>• Hardware security modules"]
        INTEGRITY["Data Integrity<br/>• SHA-256 checksums<br/>• Digital signatures<br/>• Tamper detection"]
    end

    TLS --> ENCRYPT_TRANSIT
    AUTH --> AUTHZ
    AUTHZ --> AUDIT
    ENCRYPT_REST --> INTEGRITY

    style TLS fill:#e8f5e8
    style AUTH fill:#fff3e0
    style AUTHZ fill:#e1f5fe
    style AUDIT fill:#f3e5f5
```

### Role-Based Access Control (RBAC)

```mermaid
flowchart LR
    subgraph "Users"
        ADMIN["System Admin<br/>• Full system access<br/>• User management<br/>• Configuration"]
        OPERATOR["Operator<br/>• Node management<br/>• Monitoring<br/>• Maintenance"]
        USER["Regular User<br/>• File operations<br/>• Own data access<br/>• Limited quota"]
        SERVICE["Service Account<br/>• API access<br/>• Automation<br/>• Limited scope"]
    end

    subgraph "Permissions"
        SYSTEM["System Permissions<br/>• cluster.admin<br/>• nodes.manage<br/>• users.manage"]
        FILE["File Permissions<br/>• files.read<br/>• files.write<br/>• files.delete"]
        MONITOR["Monitor Permissions<br/>• metrics.read<br/>• logs.read<br/>• health.read"]
    end

    ADMIN --> SYSTEM
    ADMIN --> FILE
    ADMIN --> MONITOR

    OPERATOR --> SYSTEM
    OPERATOR --> MONITOR

    USER --> FILE

    SERVICE --> FILE
    SERVICE --> MONITOR

    style ADMIN fill:#ffebee
    style OPERATOR fill:#fff3e0
    style USER fill:#e8f5e8
    style SERVICE fill:#e1f5fe
```

---

## Observability & Operations

### Monitoring Architecture

```mermaid
flowchart TB
    subgraph "Data Collection"
        METRICS["Metrics Exporters<br/>• Prometheus format<br/>• Custom metrics<br/>• Performance counters"]
        TRACES["Trace Collection<br/>• OpenTelemetry<br/>• Distributed tracing<br/>• Request correlation"]
        LOGS["Log Aggregation<br/>• Structured logging<br/>• Centralized collection<br/>• Search & analysis"]
    end

    subgraph "Storage & Processing"
        PROMETHEUS["Prometheus<br/>• Time-series storage<br/>• Alert rules<br/>• Query engine"]
        JAEGER["Jaeger<br/>• Trace storage<br/>• Performance analysis<br/>• Bottleneck detection"]
        ELK["ELK Stack<br/>• Log storage<br/>• Search & visualization<br/>• Dashboard creation"]
    end

    subgraph "Visualization & Alerting"
        GRAFANA["Grafana<br/>• Custom dashboards<br/>• Real-time monitoring<br/>• Historical analysis"]
        ALERTS["Alert Manager<br/>• Notification routing<br/>• Escalation policies<br/>• Incident tracking"]
    end

    METRICS --> PROMETHEUS
    TRACES --> JAEGER
    LOGS --> ELK

    PROMETHEUS --> GRAFANA
    PROMETHEUS --> ALERTS
    JAEGER --> GRAFANA
    ELK --> GRAFANA

    style METRICS fill:#e8f5e8
    style TRACES fill:#e1f5fe
    style LOGS fill:#fff3e0
    style GRAFANA fill:#f3e5f5
```

---

### Critical Design Decisions

1. **Metadata Storage**: Transition from in-memory to etcd cluster for persistence and consensus
2. **API Gateway**: Implement as separate, external service
3. **Authentication**: JWT RBAC
4. **Storage Backend**: Pluggable storage backend, must support local disk & cloud storage integration
5. **Monitoring**: Build custom vs. adopt existing observability stack (TDB)
6. **Client Pool**: Rotating client pool for improved reliability and failover
7. **Session Management**: Two-tier session system for metadata and streaming operations
8. **Garbage Collection**: Local inventory scanning for orphaned chunk cleanup

---

## Conclusion

This design provides a comprehensive roadmap for evolving the DFS from a functional prototype to a production-ready distributed storage system. The architecture emphasizes modularity, security, and observability while maintaining the core simplicity that makes the system effective.

Key architectural principles:
- **Progressive enhancement**: Each phase builds upon previous foundations
- **Operational excellence**: Design for monitoring, debugging, and maintenance
- **Security by design**: Zero-trust model with defense in depth
- **Performance focus**: Optimize for both throughput and latency
- **Extensibility**: Plugin architecture for future enhancements

The design balances immediate functionality needs with long-term scalability requirements, ensuring the system can grow from a development prototype to an enterprise-grade storage solution.