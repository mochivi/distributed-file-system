package common

// Logging params
const (
	// Request lifecycle
	LogMethod    = "method"
	LogTimestamp = "timestamp"
	LogStatus    = "status"
	LogDuration  = "duration"
	LogError     = "error"

	// Request/Operation tracking
	LogRequestID          = "req_id"
	LogOperation          = "op"
	LogStreamingSessionID = "streaming_session_id"
	LogMetadataSessionID  = "metadata_session_id"

	// Component and service identification
	LogNodeID    = "node_id"
	LogComponent = "component"
	LogService   = "service"

	// File/Chunk/Frame details
	LogFileID    = "file_id"
	LogFileSize  = "file_size"
	LogFilePath  = "file_path"
	LogDirectory = "directory"
	LogChunkID   = "chunk_id"
	LogChunkSize = "chunk_size"
	LogFrameID   = "frame_id"
	LogFrameSize = "frame_size"
	LogReplicas  = "replicas"

	// Cluster state
	LogClusterVersion = "version"
	LogNumNodes       = "num_nodes"

	// File/Chunk/Frame bulk operations
	LogNumFiles    = "num_files"
	LogNumChunks   = "num_chunks"
	LogNumReplicas = "num_replicas"

	// Streaming/Frame logging
	LogOffset = "offset"

	// Performance and metrics
	LogBytes        = "bytes"
	LogLatency      = "latency"
	LogRetries      = "retries"
	LogSuccess      = "success"
	LogSuccessCount = "success_count"
	LogFailureCount = "failure_count"
)

// Component names
const (
	ComponentCoordinator = "coordinator"
	ComponentDatanode    = "datanode"
	ComponentClient      = "client"
	ComponentUploader    = "uploader"
	ComponentDownloader  = "downloader"
	ComponentStreamer    = "streamer"
)

// Operation names
const (
	OpUpload     = "upload"
	OpDownload   = "download"
	OpDelete     = "delete"
	OpBulkDelete = "bulk_delete"
	OpList       = "list"
	OpCommit     = "commit"
	OpRegister   = "register"
	OpReplicate  = "replicate"
	OpGC         = "gc"
	OpHeartbeat  = "heartbeat"
)
