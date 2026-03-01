package metadata

import (
	"context"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
)

type MetadataStore interface {
	PutFile(ctx context.Context, path common.FilePath, info *common.FileInfo) error
	GetFile(ctx context.Context, path common.FilePath) (*common.FileInfo, error)
	DeleteFile(ctx context.Context, path common.FilePath) error
	ListFiles(ctx context.Context, directory common.FilePath, recursive bool) ([]*common.FileInfo, error)
	GetChunksForNode(ctx context.Context, nodeID string) (map[common.ChunkID]*common.ChunkHeader, error)
	GetDeletedFiles(ctx context.Context, olderThan time.Time) ([]*common.FileInfo, error)
}
