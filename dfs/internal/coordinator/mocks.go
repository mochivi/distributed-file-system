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

func (m *MockMetadataSessionManager) trackUpload(sessionID common.MetadataSessionID, req common.UploadRequest, chunkIDs []string) {
	m.Called(sessionID, req, chunkIDs)
}

func (m *MockMetadataSessionManager) commit(ctx context.Context, sessionID common.MetadataSessionID, chunkInfos []common.ChunkInfo, metaStore metadata.MetadataStore) error {
	args := m.Called(ctx, sessionID, chunkInfos, metaStore)
	return args.Error(0)
}

type MockService struct {
	mock.Mock
}

func (s *MockService) uploadFile(ctx context.Context, req common.UploadRequest) (common.UploadResponse, error) {
	args := s.Called(ctx, req)
	return args.Get(0).(common.UploadResponse), args.Error(1)
}

func (s *MockService) downloadFile(ctx context.Context, req common.DownloadRequest) (common.DownloadResponse, error) {
	args := s.Called(ctx, req)
	return args.Get(0).(common.DownloadResponse), args.Error(1)
}

func (s *MockService) listFiles(ctx context.Context, req common.ListRequest) (common.ListResponse, error) {
	args := s.Called(ctx, req)
	return args.Get(0).(common.ListResponse), args.Error(1)
}

func (s *MockService) deleteFile(ctx context.Context, req common.DeleteRequest) (common.DeleteResponse, error) {
	args := s.Called(ctx, req)
	return args.Get(0).(common.DeleteResponse), args.Error(1)
}

func (s *MockService) registerDataNode(ctx context.Context, req common.RegisterDataNodeRequest) (common.RegisterDataNodeResponse, error) {
	args := s.Called(ctx, req)
	return args.Get(0).(common.RegisterDataNodeResponse), args.Error(1)
}

func (s *MockService) heartbeat(ctx context.Context, req common.HeartbeatRequest) (common.HeartbeatResponse, error) {
	args := s.Called(ctx, req)
	return args.Get(0).(common.HeartbeatResponse), args.Error(1)
}

func (s *MockService) listNodes(ctx context.Context, req common.ListNodesRequest) (common.ListNodesResponse, error) {
	args := s.Called(ctx, req)
	return args.Get(0).(common.ListNodesResponse), args.Error(1)
}

func (s *MockService) confirmUpload(ctx context.Context, req common.ConfirmUploadRequest) (common.ConfirmUploadResponse, error) {
	args := s.Called(ctx, req)
	return args.Get(0).(common.ConfirmUploadResponse), args.Error(1)
}
