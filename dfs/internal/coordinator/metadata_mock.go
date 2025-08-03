package coordinator

import (
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

func (m *MockMetadataSessionManager) commit(sessionID string, chunkInfos []common.ChunkInfo, metaStore metadata.MetadataStore) error {
	args := m.Called(sessionID, chunkInfos, metaStore)
	return args.Error(0)
}
