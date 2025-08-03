package metadata

import (
	"context"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
)

type MetadataStore interface {
	PutFile(ctx context.Context, path string, info *common.FileInfo) error
	GetFile(ctx context.Context, path string) (*common.FileInfo, error)
	DeleteFile(ctx context.Context, path string) error
	ListFiles(ctx context.Context, directory string, recursive bool) ([]*common.FileInfo, error)
	GetChunksForNode(ctx context.Context, nodeID string) (map[string]*common.ChunkHeader, error)
	GetDeletedFiles(ctx context.Context, olderThan time.Time) ([]*common.FileInfo, error)
}
