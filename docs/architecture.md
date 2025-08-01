# Architecture

This document complements [`docs/design.md`](design.md) by focusing on the
responsibilities of every component and how they interact at runtime.

---

## Components

| Component | Responsibility | Key packages |
|-----------|----------------|--------------|
| **Coordinator** | • Maintain file-system metadata (path → chunk list).  \
  • Track DataNode membership & health via heartbeats.  \
  • Plan uploads / downloads.  \
  • Manage metadata sessions for atomicity. | `internal/coordinator` |
| **DataNode** | • Store chunk bytes on local disk.  \
  • Serve uploads & downloads via gRPC streams.  \
  • Replicate chunks to peer nodes.  \
  • Participate in heartbeat loop. | `internal/datanode` + `internal/storage` |
| **Client SDK / CLI** | • Split files into chunks & compute checksums.  \
  • Drive upload/download flows.  \
  • Expose CLI for file operations. | `internal/client` |
| **Client Pool** | • Rotating client connections with failover.  \
  • Retry logic and load distribution.  \
  • Connection management for DataNode operations. | `pkg/client_pool` |
| **Streamer** | Zero-copy framing, retries and back-pressure for any
  `ChunkDataStream` (client → node or node → node). | `pkg/streamer` |
| **Node Agent Controllers** | • Heartbeat management and health monitoring.  \
  • Garbage collection for orphaned chunks and deleted files.  \
  • Background maintenance operations. | `internal/cluster/*/controllers` |

---

## Implemented Feature Summary

This section provides a high-level summary of the features that are currently implemented and functional in the codebase.

### Client-Facing Operations (via Coordinator)

* **File Upload:** A client can request to upload a file. The coordinator determines the chunking strategy and assigns data nodes for each chunk. The client then uploads the chunks directly to the specified data nodes using client pool for connection management.
* **File Download:** A client can request to download a file. The coordinator provides the locations of the file's chunks, which the client can then download from the data nodes using replica selection algorithms and client pool for failover.
* **File Delete:** A client can request to delete a file. The coordinator marks the file as deleted and background garbage collection processes clean up the chunks.
* **Upload Confirmation:** After successfully uploading all chunks to their respective data nodes, the client must send a confirmation to the coordinator. This action commits the file's metadata, making it "officially" part of the filesystem.

### Coordinator Functionality

* **Node Management:** The coordinator maintains a list of active data nodes. Data nodes register themselves on startup and send periodic heartbeats to signal their liveness. The coordinator tracks node status and cluster topology changes using incremental versioning.
* **Chunk Placement Strategy:** For a file upload, the coordinator selects a primary data node and a set of replica nodes for each chunk using pluggable node selection algorithms.
* **Metadata Management:** The coordinator manages all filesystem metadata (file-to-chunk mappings). This metadata is currently **stored in-memory** and is not persistent across coordinator restarts.
* **Session Management:** The coordinator manages metadata sessions to ensure atomicity between chunk uploads and metadata commits. Sessions have configurable timeouts to prevent resource leaks.

### Data Node Functionality

* **Chunk Storage:** Data nodes store chunk data on local disk using a nested directory structure based on chunk IDs to avoid filesystem limitations with too many files in a single directory.
* **gRPC Service:** Each data node exposes a gRPC service for handling chunk operations (uploads, downloads, deletion, bulk deletion, health checks).
* **Chunk Replication:** When a data node receives a chunk from a client (acting as the "primary"), it replicates that chunk in parallel to the other replica nodes assigned by the coordinator.
* **Streaming Session Management:** Data nodes manage streaming sessions with configurable timeouts to handle client disconnections gracefully.
* **Bulk Operations:** Data nodes support bulk chunk deletion for garbage collection and storage optimization.

### Missing/Incomplete Features

* **ListFiles Client Support:** gRPC method is implemented in coordinator but client-side implementation is incomplete.
* **Metadata Persistence:** Coordinator metadata is lost on restart - requires implementation of persistent storage backend (planned: etcd).
* **Garbage Collection:** Partially implemented but not tested - orphaned chunks GC and deleted files GC controllers exist but need testing and integration.

---

## Coordinator endpoints

| RPC | Description |
|-----|-------------|
| `UploadFile(UploadRequest)` | Returns chunk plan (primary + replicas) and metadata session ID. |
| `ConfirmUpload(ConfirmUploadRequest)` | Commits metadata after client confirms all chunks uploaded. |
| `DownloadFile(DownloadRequest)` | Returns chunk map + file info with available replicas. |
| `DeleteFile(DeleteRequest)` | ✅ **Implemented** - Remove file metadata and trigger chunk cleanup. |
| `ListFiles(ListRequest)` | ✅ **Implemented** - List files in directory (coordinator only, client support incomplete). |
| `RegisterDataNode(RegisterDataNodeRequest)` | Called once at node start-up to join cluster. |
| `DataNodeHeartbeat(HeartbeatRequest)` | Periodic health + disk usage update; returns incremental node updates. |
| `ListNodes(ListNodesRequest)` | Returns current cluster state and version information. |

`internal/common/types.go` contains Go equivalents of most protobuf messages with validation and conversion logic.

---

## DataNode gRPC surface

| RPC | Direction | Purpose |
|-----|-----------|---------|
| `PrepareChunkUpload` | unary | Ask a node if it can accept the chunk; returns streaming session ID. |
| `UploadChunkStream` | bidi stream | Push chunk bytes (client → primary, primary → replica). |
| `PrepareChunkDownload` | unary | Validate chunk exists and create download session. |
| `DownloadChunkStream` | server stream | Stream chunk bytes to client / peer. |
| `DeleteChunk` | unary | Remove a chunk from storage. |
| `BulkDeleteChunk` | unary | Delete multiple chunks in one request (garbage collection, rebalancing). |
| `HealthCheck` | unary | Liveness probe for monitoring. |

---

## Streaming protocol (ChunkDataStream)

Every `ChunkDataStream` message contains:

* `session_id` – UUID from the *Prepare* phase.
* `chunk_id` – unique per chunk.
* `data` – chunk of actual file data.
* `offset` – byte offset of `data` relative to start of chunk.
* `is_final` – last frame flag.
* `partial_checksum` – SHA-256 of this frame (allows early detection).

The receiver responds with `ChunkDataAck{bytes_received, ready_for_next, replicas}` which
enables back-pressure: when the DataNode's buffer exceeds a threshold it can
set `ready_for_next=false` so the sender pauses. The final ack includes replica information.

---

## Session Management

The system uses two types of sessions for different purposes:

### Streaming Sessions
* **Purpose:** Manage individual chunk upload/download streams
* **Scope:** DataNode to Client or DataNode to DataNode
* **Timeout:** Configurable per-node (default: stream expires if no activity)
* **Cleanup:** Automatic cleanup on timeout or stream completion

### Metadata Sessions  
* **Purpose:** Ensure atomicity between chunk uploads and metadata commits
* **Scope:** Coordinator to Client
* **Timeout:** Configurable per-coordinator (prevents indefinite pending metadata)
* **Cleanup:** Coordinator tracks sessions and expires them after timeout

---

## Failure handling

| Failure | Detection | Recovery |
|---------|-----------|----------|
| **Client disconnects** while primary is still replicating | Replica's `stream.Recv()` returns `Canceled`. | Primary aborts replication; client may retry the whole chunk. |
| **Replica timeout** | Primary waits per replica (configurable timeout). | If replicas < required, primary returns error → client retry. |
| **Checksum mismatch** | Verified after stream closes using SHA-256. | Chunk is discarded; upload fails; client retry. |
| **Session timeout** | Both streaming and metadata sessions have timeouts. | Resources are cleaned up; client must restart operation. |
| **Node failure** | Heartbeat mechanism detects unresponsive nodes. | Coordinator marks node as unhealthy; affects chunk placement. |

---

## Configuration Management

Refer to [Configuration Management](docs/configuration.md)
---

## Current Technical Specifications

### Chunk Management
* **Chunk ID Format:** `<path_hash>_<chunk_index>` (e.g., `f1d2d2f924e9_0`)
* **Default Chunk Size:** 8MB (configurable up to 64MB)
* **Replication Factor:** Default 3 (1 primary + 2 replicas), configurable per user plan
* **Storage Path:** `<rootDir>/<dir1>/<dir2>/<chunkID>` (nested structure avoids filesystem limits)

### Network Protocol
* **Transport:** gRPC over HTTP/2
* **Serialization:** Protocol Buffers (proto3)
* **Streaming:** Bidirectional streams with back-pressure control
* **Checksums:** SHA-256 at multiple levels (frame, chunk, file)

### Node Communication
* **Service Discovery:** Environment variables + Docker DNS (temporary solution)
* **Health Monitoring:** Periodic heartbeat with incremental state updates
* **Cluster State:** Version-based incremental updates to avoid full state transfers

---

## Glossary
* **Chunk** – Immutable byte sequence (default 8MB) identified by ID `hash_index`.
* **Primary** – DataNode that first receives a chunk from the client and coordinates replication.
* **Replica** – DataNode that receives the chunk from the primary during replication.
* **Streaming Session** – Temporary state (checksum, buffer) keyed by `session_id` during chunk transfer.
* **Metadata Session** – Coordinator-managed session ensuring atomicity of file operations.
* **Node Selection** – Algorithm for choosing optimal primary and replica nodes for chunk placement.