package metadata

import (
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
)

type MetadataStore interface {
	PutFile(path string, info *common.FileInfo) error
	GetFile(path string) (*common.FileInfo, error)
	DeleteFile(path string) error
	ListFiles(directory string, recursive bool) ([]*common.FileInfo, error)
	GetChunksForNode(nodeID string) (map[string]*common.ChunkHeader, error)
	GetDeletedFiles(olderThan time.Time) ([]*common.FileInfo, error)
}
