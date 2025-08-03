package metadata

import (
	"context"
	"fmt"
	"strings"
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

func (m *MetadataDiskStorage) GetFile(ctx context.Context, path string) (*common.FileInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info, ok := m.cache[path]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, path)
	}

	return info, nil
}

func (m *MetadataDiskStorage) PutFile(ctx context.Context, path string, info *common.FileInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.cache[path] = info

	return nil
}

func (m *MetadataDiskStorage) DeleteFile(ctx context.Context, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.cache[path]; !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, path)
	}

	delete(m.cache, path)

	return nil
}

func (m *MetadataDiskStorage) ListFiles(ctx context.Context, directory string, recursive bool) ([]*common.FileInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	files := make([]*common.FileInfo, 0)
	for _, info := range m.cache {
		if strings.HasPrefix(info.Path, directory) {
			files = append(files, info)
		}
	}

	return files, nil
}

func (m *MetadataDiskStorage) GetChunksForNode(ctx context.Context, nodeID string) (map[string]*common.ChunkHeader, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	chunks := make(map[string]*common.ChunkHeader)
	for _, info := range m.cache {
		for _, chunk := range info.Chunks {
			if chunk.Header.ID == nodeID {
				chunks[chunk.Header.ID] = &chunk.Header
			}
		}
	}

	return nil, nil
}

func (m *MetadataDiskStorage) GetDeletedFiles(ctx context.Context, olderThan time.Time) ([]*common.FileInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	deletedFiles := make([]*common.FileInfo, 0)
	for _, info := range m.cache {
		if info.Deleted {
			if info.DeletedAt.Before(olderThan) && !info.DeletedAt.IsZero() {
				deletedFiles = append(deletedFiles, info)
			}
		}
	}

	return deletedFiles, nil
}
