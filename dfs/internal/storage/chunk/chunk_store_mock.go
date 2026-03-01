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

func (m *MockChunkStore) Get(ctx context.Context, chunkID common.ChunkID) (common.ChunkHeader, []byte, error) {
	args := m.Called(ctx, chunkID)
	return args.Get(0).(common.ChunkHeader), args.Get(1).([]byte), args.Error(2)
}

func (m *MockChunkStore) GetData(ctx context.Context, chunkID common.ChunkID) ([]byte, error) {
	args := m.Called(ctx, chunkID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockChunkStore) GetHeader(ctx context.Context, chunkID common.ChunkID) (common.ChunkHeader, error) {
	args := m.Called(ctx, chunkID)
	return args.Get(0).(common.ChunkHeader), args.Error(1)
}

func (m *MockChunkStore) GetHeaders(ctx context.Context) (map[common.ChunkID]common.ChunkHeader, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[common.ChunkID]common.ChunkHeader), args.Error(1)
}

func (m *MockChunkStore) Delete(ctx context.Context, chunkID common.ChunkID) error {
	args := m.Called(ctx, chunkID)
	return args.Error(0)
}

func (m *MockChunkStore) BulkDelete(ctx context.Context, maxConcurrentDeletes int, chunkIDs []common.ChunkID) ([]common.ChunkID, error) {
	args := m.Called(ctx, maxConcurrentDeletes, chunkIDs)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]common.ChunkID), args.Error(1)
}

func (m *MockChunkStore) Exists(ctx context.Context, chunkID common.ChunkID) error {
	args := m.Called(ctx, chunkID)
	return args.Error(0)
}

func (m *MockChunkStore) List(ctx context.Context) ([]common.ChunkID, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]common.ChunkID), args.Error(1)
}
