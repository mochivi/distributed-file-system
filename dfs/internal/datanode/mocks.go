package datanode

import (
	"context"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/client_pool"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/stretchr/testify/mock"
)

type MockParalellReplicationService struct {
	mock.Mock
}

func (m *MockParalellReplicationService) Replicate(clientPool client_pool.ClientPool, chunkHeader common.ChunkHeader, data []byte, requiredReplicas int) ([]*common.NodeInfo, error) {
	args := m.Called(clientPool, chunkHeader, data, requiredReplicas)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*common.NodeInfo), args.Error(1)
}

type MockService struct {
	mock.Mock
}

func (m *MockService) prepareChunkUpload(ctx context.Context, req common.UploadChunkRequest) (common.NodeReady, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(common.NodeReady), args.Error(1)
}

func (m *MockService) prepareChunkDownload(ctx context.Context, req common.DownloadChunkRequest) (common.DownloadReady, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(common.DownloadReady), args.Error(1)
}

func (m *MockService) deleteChunk(ctx context.Context, req common.DeleteChunkRequest) (common.DeleteChunkResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(common.DeleteChunkResponse), args.Error(1)
}

func (m *MockService) bulkDeleteChunk(ctx context.Context, req common.BulkDeleteChunkRequest) (common.BulkDeleteChunkResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(common.BulkDeleteChunkResponse), args.Error(1)
}

func (m *MockService) uploadChunkStream(stream proto.DataNodeService_UploadChunkStreamServer) error {
	args := m.Called(stream)
	return args.Error(0)
}

func (m *MockService) downloadChunkStream(req common.DownloadStreamRequest, stream proto.DataNodeService_DownloadChunkStreamServer) error {
	args := m.Called(req, stream)
	return args.Error(0)
}

func (m *MockService) healthCheck(ctx context.Context, req common.HealthCheckRequest) (common.HealthCheckResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(common.HealthCheckResponse), args.Error(1)
}
