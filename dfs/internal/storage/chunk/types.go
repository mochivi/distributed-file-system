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

	GetHeader(ctx context.Context, chunkID common.ChunkID) (common.ChunkHeader, error)
	GetData(ctx context.Context, chunkID common.ChunkID) ([]byte, error)
	Get(ctx context.Context, chunkID common.ChunkID) (common.ChunkHeader, []byte, error)

	// Reads all headers for provided chunkIDs, maps them
	// Returns all headers if chunkIDs is not provided
	GetHeaders(ctx context.Context) (map[common.ChunkID]common.ChunkHeader, error)

	Delete(ctx context.Context, chunkID common.ChunkID) error
	BulkDelete(ctx context.Context, maxConcurrentDeletes int, chunkIDs []common.ChunkID) ([]common.ChunkID, error)
	Exists(ctx context.Context, chunkID common.ChunkID) error
	List(ctx context.Context) ([]common.ChunkID, error)
}
