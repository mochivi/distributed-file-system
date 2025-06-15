# Distributed File System (DFS) – Design Overview

## 1. High-level Architecture

```mermaid
flowchart LR
    subgraph "Client"
        A["CLI / SDK"]
    end

    subgraph "Coordinator"
        C1["gRPC API<br/>Metadata manager"]
    end

    subgraph "Storage cluster"
        DN1["DataNode – primary"]
        DN2["DataNode – replica 1"]
        DN3["DataNode – replica 2"]
    end

    A -- "Upload / Download" --> C1
    C1 -- "Chunk plan" --> A

    %% Client writes chunk to primary
    A -- "StoreChunk (stream)" --> DN1

    %% Primary replicates chunk via streaming gRPC
    DN1 -- "ReplicateChunk (stream)" --> DN2
    DN1 -- "ReplicateChunk (stream)" --> DN3

    %% Reads can hit any replica
    A -- "RetrieveChunk" --> DN2
    A -- "RetrieveChunk" --> DN3

    %% Health-checks
    DN1 -- "Heartbeat" --> C1
    DN2 -- "Heartbeat" --> C1
    DN3 -- "Heartbeat" --> C1
```

### Component Responsibilities

* **Coordinator** – Stateless service that maintains metadata only: file paths, chunk-to-node mapping and cluster membership / health.  It never stores file bytes.
* **DataNode** – Stores chunk bytes on local disk, streams them via gRPC and replicates to sibling nodes.
* **Client SDK / CLI** – Splits files into chunks, orchestrates uploads/downloads in parallel, and confirms uploads.  
  All streaming logic (chunk framing, back-pressure, retries) is implemented once in a reusable component **`common.Streamer`** and reused by both the client and DataNode replication paths.

*Default replication factor today is 3 (1 primary + 2 replicas) but it is configurable via node and coordinator config.*

---

## 2. Data-flow – Upload (current implemented feature)

```mermaid
sequenceDiagram
    participant Client as Client
    participant Coord as Coordinator
    participant Primary as DataNode Primary
    participant Replica1 as DataNode Replica
    participant Replica2 as DataNode Replica

    Client->>Coord: UploadRequest (path, size, desiredChunkSize)
    Coord-->>Client: Chunk plan (ChunkID → Node)

    %% === Primary write path ===
    Client->>Primary: PrepareChunkUpload (ChunkMeta)
    Primary-->>Client: ChunkUploadReady (sessionID)
    Client-->>Primary: ChunkDataStream (stream)

    %% Primary replicates (same two-phase handshake internally)
    Primary->>Replica1: PrepareChunkUpload
    Replica1-->>Primary: ChunkUploadReady
    Primary-->>Replica1: ChunkDataStream (stream)
    Primary->>Replica2: PrepareChunkUpload
    Replica2-->>Primary: ChunkUploadReady
    Primary-->>Replica2: ChunkDataStream (stream)

    %% === Commit ===
    Client->>Coord: ConfirmUpload (chunk list + checksums)
```

Steps in detail:

1. **UploadRequest** – Client sends path, size & optional chunk size to `Coordinator.UploadFile`.
2. **Chunk plan** – Coordinator shards the file logically and picks a *primary* and *n-1 replicas* for each chunk using the pluggable `NodeSelector`.
3. **PrepareChunkUpload** – Client sends `ChunkMeta` to the primary. Node answers `ChunkUploadReady(sessionID)` if it can accept the chunk.  
4. **ChunkDataStream** – Client opens a bidirectional stream (`ChunkDataStream`) identified by the session ID and pushes data via `common.Streamer` until `isFinal=true`.
5. **ReplicateChunk** – After persisting, the primary performs the same *prepare → stream* handshake with each replica. Back-pressure and framing are identical because it reuses `common.Streamer`.
6. **ConfirmUpload** – Once all chunks are safely stored and replicated, the client finalises the session so the coordinator commits metadata atomically.

---

## 3. Roadmap / Future Work

1. **Unit test coverage** across all packages.
2. **Additional integration tests**: Download, List, Delete, etc.
3. **Garbage collection** for orphaned chunks.
4. **Security** – TLS transport & optional at-rest encryption.
5. **Access control** – JWT or mTLS based authentication & authorisation.
6. **HTTP gateway** and richer CLI UX.
7. **Observability** – metrics, tracing and log streaming.
8. **Capacity-aware rebalancer** & erasure coding experiments.