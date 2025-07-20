package chunk

import (
	"context"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/mock"
)

type MockChunkStore struct {
	mock.Mock
}

func (m *MockChunkStore) Store(chunkHeader common.ChunkHeader, data []byte) error {
	args := m.Called(chunkHeader, data)
	return args.Error(0)
}

func (m *MockChunkStore) Get(chunkID string) (common.ChunkHeader, []byte, error) {
	args := m.Called(chunkID)
	return args.Get(0).(common.ChunkHeader), args.Get(1).([]byte), args.Error(2)
}

func (m *MockChunkStore) GetData(chunkID string) ([]byte, error) {
	args := m.Called(chunkID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockChunkStore) GetHeader(chunkID string) (common.ChunkHeader, error) {
	args := m.Called(chunkID)
	return args.Get(0).(common.ChunkHeader), args.Error(1)
}

func (m *MockChunkStore) GetHeaders(ctx context.Context) (map[string]common.ChunkHeader, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]common.ChunkHeader), args.Error(1)
}

func (m *MockChunkStore) Delete(chunkID string) error {
	args := m.Called(chunkID)
	return args.Error(0)
}

func (m *MockChunkStore) BulkDelete(ctx context.Context, maxConcurrentDeletes int, chunkIDs []string) ([]string, error) {
	args := m.Called(ctx, maxConcurrentDeletes, chunkIDs)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockChunkStore) Exists(chunkID string) bool {
	args := m.Called(chunkID)
	return args.Bool(0)
}

func (m *MockChunkStore) List() ([]string, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}
