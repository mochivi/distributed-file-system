package metadata

import (
	"sync"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
)

const BASE_DIR = "./metadata"

type MetadataDiskStorage struct {
	baseDir   string
	mu        sync.RWMutex
	cacheSize int // max entries in cache
	cache     map[string]*common.FileInfo
	autoFlush bool // autoFlush changes to disk
}

func NewMetadataLocalStorage() *MetadataDiskStorage {
	return &MetadataDiskStorage{
		baseDir:   BASE_DIR,
		cacheSize: 1000,
		cache:     make(map[string]*common.FileInfo),
		autoFlush: true,
	}
}

func (m *MetadataDiskStorage) PutFile(path string, info *common.FileInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return nil
}

func (m *MetadataDiskStorage) GetFile(path string) (*common.FileInfo, error) {
	return nil, nil
}

func (m *MetadataDiskStorage) DeleteFile(path string) error {
	return nil
}

func (m *MetadataDiskStorage) ListFiles(directory string, recursive bool) ([]*common.FileInfo, error) {
	return nil, nil
}

func (m *MetadataDiskStorage) GetChunksForNode(nodeID string) (map[string]common.ChunkHeader, error) {
	return nil, nil
}

func (m *MetadataDiskStorage) GetDeletedFiles(olderThan time.Time) ([]*common.FileInfo, error) {
	return nil, nil
}
