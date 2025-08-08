package chunk

import (
	"context"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/mock"
)

type MockChunkStore struct {
	mock.Mock
}

func (m *MockChunkStore) Store(ctx context.Context, chunkHeader common.ChunkHeader, data []byte) error {
	args := m.Called(ctx, chunkHeader, data)
	return args.Error(0)
}

func (m *MockChunkStore) Get(ctx context.Context, chunkID string) (common.ChunkHeader, []byte, error) {
	args := m.Called(ctx, chunkID)
	return args.Get(0).(common.ChunkHeader), args.Get(1).([]byte), args.Error(2)
}

func (m *MockChunkStore) GetData(ctx context.Context, chunkID string) ([]byte, error) {
	args := m.Called(ctx, chunkID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockChunkStore) GetHeader(ctx context.Context, chunkID string) (common.ChunkHeader, error) {
	args := m.Called(ctx, chunkID)
	return args.Get(0).(common.ChunkHeader), args.Error(1)
}

func (m *MockChunkStore) GetHeaders(ctx context.Context) (map[string]common.ChunkHeader, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]common.ChunkHeader), args.Error(1)
}

func (m *MockChunkStore) Delete(ctx context.Context, chunkID string) error {
	args := m.Called(ctx, chunkID)
	return args.Error(0)
}

func (m *MockChunkStore) BulkDelete(ctx context.Context, maxConcurrentDeletes int, chunkIDs []string) ([]string, error) {
	args := m.Called(ctx, maxConcurrentDeletes, chunkIDs)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockChunkStore) Exists(ctx context.Context, chunkID string) error {
	args := m.Called(ctx, chunkID)
	return args.Error(0)
}

func (m *MockChunkStore) List(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}
