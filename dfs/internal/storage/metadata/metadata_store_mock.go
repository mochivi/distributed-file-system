package metadata

import (
	"context"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/mock"
)

type MockMetadataStore struct {
	mock.Mock
}

func (m *MockMetadataStore) PutFile(ctx context.Context, path string, info *common.FileInfo) error {
	args := m.Called(ctx, path, info)
	return args.Error(0)
}

func (m *MockMetadataStore) GetFile(ctx context.Context, path string) (*common.FileInfo, error) {
	args := m.Called(ctx, path)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.FileInfo), args.Error(1)
}

func (m *MockMetadataStore) DeleteFile(ctx context.Context, path string) error {
	args := m.Called(ctx, path)
	return args.Error(0)
}

func (m *MockMetadataStore) ListFiles(ctx context.Context, directory string, recursive bool) ([]*common.FileInfo, error) {
	args := m.Called(ctx, directory, recursive)
	return args.Get(0).([]*common.FileInfo), args.Error(1)
}

func (m *MockMetadataStore) GetChunksForNode(ctx context.Context, nodeID string) (map[string]*common.ChunkHeader, error) {
	args := m.Called(ctx, nodeID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]*common.ChunkHeader), args.Error(1)
}

func (m *MockMetadataStore) GetDeletedFiles(ctx context.Context, olderThan time.Time) ([]*common.FileInfo, error) {
	args := m.Called(ctx, olderThan)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*common.FileInfo), args.Error(1)
}
