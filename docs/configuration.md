# Configuration reference

DFS services are configured primarily via **environment variables**.  All
variables are optional; sensible defaults allow the demo cluster to start with
no additional configuration.

| Variable | Component | Default | Description |
|----------|-----------|---------|-------------|
| `COORDINATOR_HOST` | all | `coordinator` | Hostname (or IP) where coordinator is reachable. |
| `COORDINATOR_PORT` | all | `8080` | gRPC port exposed by coordinator. |
| `DATANODE_HOST`    | datanode | container hostname | Address advertised to peers – must be reachable by all nodes. |
| `DATANODE_PORT`    | datanode | `8081` | gRPC port that the DataNode listens on. |
| `DISK_STORAGE_BASE_DIR` | datanode | `/app` | Root directory where chunks are stored on disk. |
| `REPLICATE_TIMEOUT`| datanode | `2m` | Timeout for each replica upload. |
| `SESSION_TIMEOUT`  | datanode | `1m` | Streaming-session idle timeout. |
| `CHUNK_STREAM_SIZE`| streamer | `262144` (256 KiB) | Size of each frame sent over the `ChunkDataStream`. |
| `MAX_CHUNK_RETRIES`| streamer | `3` | How many times the streamer retries sending the same frame. |
| `BACKPRESSURE_TIME`| streamer | `0s` | Optional delay when replica responds `readyForNext=false`. |
| `WAIT_REPLICAS`    | client   | `true` | Whether the client waits for the replica-ack frame. |

Environment variables are read in `internal/.../config.go` via helper
`pkg/utils/env.go`.

---

## Example `.env` file
```dotenv
# coordinator
COORDINATOR_PORT=9090

# datanode
DATANODE_HOST=datanode1
DATANODE_PORT=9091
DISK_STORAGE_BASE_DIR=/data
REPLICATE_TIMEOUT=5m

# streamer
CHUNK_STREAM_SIZE=1048576   # 1 MiB
```

Load it with Docker Compose:
```bash
docker compose --env-file .env up -d 