package datanode

import (
	"context"

	"github.com/mochivi/distributed-file-system/internal/common"
)

type DataNodeService interface {
	// Chunk operations
	StoreChunk(ctx context.Context, chunkID string, data []byte) error
	RetrieveChunk(ctx context.Context, chunkID string) ([]byte, error)
	DeleteChunk(ctx context.Context, chunkID string) error

	// Replication
	ReplicateChunk(ctx context.Context, chunkID string, sourceNode string) error
}

type StoreChunkRequest struct {
	ChunkID  string
	Data     []byte
	Checksum string
}

type StoreChunkResponse struct {
	Success bool
	Message string
}

type RetrieveChunkRequest struct {
	ChunkID string
}

type RetrieveChunkResponse struct {
	Data     []byte
	Checksum string
}

type DeleteChunkRequest struct {
	ChunkID string
}

type DeleteChunkResponse struct {
	Success bool
	Message string
}

type ReplicateChunkRequest struct {
	ChunkID      string
	SourceNodeID string
}

type ReplicateChunkResponse struct {
	Success bool
	Message string
}

type HealthCheckRequest struct{}
type HealthCheckResponse struct {
	Status common.HealthStatus
}
