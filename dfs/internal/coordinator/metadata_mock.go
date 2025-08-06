package coordinator

import (
	"context"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage/metadata"
	"github.com/stretchr/testify/mock"
)

type MockMetadataSessionManager struct {
	mock.Mock
}

func (m *MockMetadataSessionManager) trackUpload(sessionID string, req common.UploadRequest, numChunks int) {
	m.Called(sessionID, req, numChunks)
}

func (m *MockMetadataSessionManager) commit(ctx context.Context, sessionID string, chunkInfos []common.ChunkInfo, metaStore metadata.MetadataStore) error {
	args := m.Called(ctx, sessionID, chunkInfos, metaStore)
	return args.Error(0)
}
