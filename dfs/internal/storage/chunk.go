package storage

import (
	"context"

	"github.com/mochivi/distributed-file-system/internal/common"
)

// TODO: add context awareness
type ChunkStorage interface {
	Store(chunkHeader common.ChunkHeader, data []byte) error

	GetHeader(chunkID string) (common.ChunkHeader, error)
	GetData(chunkID string) ([]byte, error)
	Get(chunkID string) (common.ChunkHeader, []byte, error)

	// Reads all headers for provided chunkIDs, maps them
	// Returns all headers if chunkIDs is not provided
	GetHeaders(ctx context.Context) (map[string]common.ChunkHeader, error)

	Delete(chunkID string) error
	BulkDelete(ctx context.Context, maxConcurrentDeletes int, chunkIDs []string) ([]string, error)
	Exists(chunkID string) bool
	List() ([]string, error)
}
