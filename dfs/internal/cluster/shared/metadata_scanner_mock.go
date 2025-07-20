package shared

import (
	"context"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/mock"
)

type MockMetadataScanner struct {
	mock.Mock
}

func (m *MockMetadataScanner) GetDeletedFiles(ctx context.Context, olderThan time.Time) ([]*common.FileInfo, error) {
	args := m.Called(ctx, olderThan)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*common.FileInfo), args.Error(1)
}

func (m *MockMetadataScanner) GetChunksForNode(ctx context.Context, nodeID string) (map[string]*common.ChunkHeader, error) {
	args := m.Called(ctx, nodeID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]*common.ChunkHeader), args.Error(1)
}
