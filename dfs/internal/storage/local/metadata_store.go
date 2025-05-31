package local

import (
	"sync"

	"github.com/mochivi/distributed-file-system/internal/common"
)

const BASE_DIR = "./metadata"

type MetadataLocalStorage struct {
	baseDir   string
	mu        sync.RWMutex
	cacheSize int // max entries in cache
	cache     map[string]*common.FileInfo
	autoFlush bool // autoFlush changes to disk
}

func NewMetadataLocalStorage() *MetadataLocalStorage {
	return &MetadataLocalStorage{
		baseDir:   BASE_DIR,
		cacheSize: 1000,
		cache:     make(map[string]*common.FileInfo),
		autoFlush: true,
	}
}

func (m *MetadataLocalStorage) PutFile(path string, info *common.FileInfo) error {
	return nil
}
func (m *MetadataLocalStorage) GetFile(path string) (*common.FileInfo, error) {
	return nil, nil
}
func (m *MetadataLocalStorage) DeleteFile(path string) error {
	return nil
}
func (m *MetadataLocalStorage) ListFiles(directory string) ([]*common.FileInfo, error) {
	return nil, nil
}
