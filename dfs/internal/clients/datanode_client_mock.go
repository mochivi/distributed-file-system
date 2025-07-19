package clients

import (
	"context"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

type MockDataNodeClient struct {
	mock.Mock
	node *common.NodeInfo
}

func NewMockDataNodeClient(node *common.NodeInfo) *MockDataNodeClient {
	return &MockDataNodeClient{
		Mock: mock.Mock{},
		node: node,
	}
}

func (m *MockDataNodeClient) StoreChunk(ctx context.Context, req common.ChunkHeader, opts ...grpc.CallOption) (common.NodeReady, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(common.NodeReady), args.Error(1)
}

func (m *MockDataNodeClient) PrepareChunkDownload(ctx context.Context, req common.DownloadChunkRequest, opts ...grpc.CallOption) (common.DownloadReady, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(common.DownloadReady), args.Error(1)
}

func (m *MockDataNodeClient) DeleteChunk(ctx context.Context, req common.DeleteChunkRequest, opts ...grpc.CallOption) (common.DeleteChunkResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(common.DeleteChunkResponse), args.Error(1)
}

func (m *MockDataNodeClient) BulkDeleteChunk(ctx context.Context, req common.BulkDeleteChunkRequest, opts ...grpc.CallOption) (common.BulkDeleteChunkResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(common.BulkDeleteChunkResponse), args.Error(1)
}

func (m *MockDataNodeClient) ReplicateChunk(ctx context.Context, req common.ChunkHeader, opts ...grpc.CallOption) (common.NodeReady, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(common.NodeReady), args.Error(1)
}

func (m *MockDataNodeClient) UploadChunkStream(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[proto.ChunkDataStream, proto.ChunkDataAck], error) {
	args := m.Called(ctx)
	return args.Get(0).(grpc.BidiStreamingClient[proto.ChunkDataStream, proto.ChunkDataAck]), args.Error(1)
}

func (m *MockDataNodeClient) DownloadChunkStream(ctx context.Context, req common.DownloadStreamRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[proto.ChunkDataStream], error) {
	args := m.Called(ctx, req)
	return args.Get(0).(grpc.ServerStreamingClient[proto.ChunkDataStream]), args.Error(1)
}

func (m *MockDataNodeClient) HealthCheck(ctx context.Context, req common.HealthCheckRequest, opts ...grpc.CallOption) (common.HealthCheckResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(common.HealthCheckResponse), args.Error(1)
}

func (m *MockDataNodeClient) Close() error {
	return m.Called().Error(0)
}

func (m *MockDataNodeClient) Node() *common.NodeInfo {
	return m.node
}
