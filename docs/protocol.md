# Wire protocol

DFS uses **Protocol Buffers** over **gRPC (HTTP/2)** for all service‐to‐service
communication.  This document summarises the key message types and RPCs.  Full
IDL lives in [`pkg/proto/*.proto`](../pkg/proto/).

---

## Messages
| Message | Purpose |
|---------|---------|
| `ChunkMeta` | Identifier of a chunk (`chunk_id`, `checksum`, `size`). |
| `ChunkData` | Frame inside `ChunkDataStream` (data + offset + flags). |
| `ChunkDataAck` | Acknowledgement for a `ChunkData` frame. |
| `ChunkReplicas` | Final ack that carries the list of replicas that succeeded. |
| `UploadPlan` | Returned by coordinator – mapping of chunk → nodes. |
| `NodeInfo` | Heart-beat payload (free, total, health). |

See `common.proto`, `coordinator.proto`, `datanode.proto` for exact fields.

---

## RPC matrix

| Service | RPC | Type | Notes |
|---------|-----|------|-------|
| Coordinator | `UploadFile` | unary | Plan upload, returns `UploadPlan`. |
|   | `ConfirmUpload` | unary | Atomically commit metadata. |
|   | `DownloadFile` | unary | Returns chunk map. |
|   | `RegisterDataNode` | unary | Node joins cluster. |
|   | `DataNodeHeartbeat` | bidi stream | Node sends `Heartbeat`; server streams delta updates. |
| DataNode | `PrepareChunkUpload` | unary | Capacity & checksum pre-flight; returns session ID. |
|   | `UploadChunkStream` | bidi stream | Client sends `ChunkData`; server acks and finally `ChunkReplicas`. |
|   | `PrepareChunkDownload` | unary | Validate chunk exists; returns session ID. |
|   | `DownloadChunkStream` | server stream | Stream `ChunkData` frames. |
|   | `ReplicateChunk` | bidi stream | Same as upload but node→node. |

---

## Session lifecycle
1. Client calls `PrepareChunkUpload` on primary node → gets `session_id`.
2. Opens `UploadChunkStream` with that ID.
3. Streams data frames; each frame is 256 KiB by default.
4. Primary replies with `ChunkDataAck` frames; when `is_final=true` verifies
   checksum and stores to disk.
5. Primary fans out to replicas via `ReplicateChunk` (same session ID to ease
   tracing).
6. When the required number of replicas acknowledge, primary sends
   `ChunkReplicas{replicas[]}` back to the client and closes the stream.

---

## Forward / backward compatibility
* **Field additions** are safe – proto3 reserves unknown fields.
* **Required to optional** – avoid; prefer new optional field.
* **Tag number changes** – not allowed; they are the wire key.

Protobuf compiler version is pinned in `go.mod` (`google.golang.org/protobuf`)
so regeneration is reproducible.
