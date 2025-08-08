package chunk

import (
	"context"

	"github.com/mochivi/distributed-file-system/internal/common"
)

type StorageBackendType string

const (
	StorageBackendDisk StorageBackendType = "disk"
)

// TODO: add context awareness
type ChunkStorage interface {
	Store(ctx context.Context, chunkHeader common.ChunkHeader, data []byte) error

	GetHeader(ctx context.Context, chunkID string) (common.ChunkHeader, error)
	GetData(ctx context.Context, chunkID string) ([]byte, error)
	Get(ctx context.Context, chunkID string) (common.ChunkHeader, []byte, error)

	// Reads all headers for provided chunkIDs, maps them
	// Returns all headers if chunkIDs is not provided
	GetHeaders(ctx context.Context) (map[string]common.ChunkHeader, error)

	Delete(ctx context.Context, chunkID string) error
	BulkDelete(ctx context.Context, maxConcurrentDeletes int, chunkIDs []string) ([]string, error)
	Exists(ctx context.Context, chunkID string) error
	List(ctx context.Context) ([]string, error)
}
