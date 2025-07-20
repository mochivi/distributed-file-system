package clients

import (
	"context"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

type MockCoordinatorClient struct {
	mock.Mock
	node *common.NodeInfo
}

func NewMockCoordinatorClient(node *common.NodeInfo) *MockCoordinatorClient {
	return &MockCoordinatorClient{
		Mock: mock.Mock{},
		node: node,
	}
}

func (m *MockCoordinatorClient) Node() *common.NodeInfo {
	return m.node
}

func (m *MockCoordinatorClient) Close() error {
	return m.Called().Error(0)
}

func (c *MockCoordinatorClient) UploadFile(ctx context.Context, req common.UploadRequest, opts ...grpc.CallOption) (common.UploadResponse, error) {
	args := c.Called(ctx, req, opts)
	return args.Get(0).(common.UploadResponse), args.Error(1)
}

func (c *MockCoordinatorClient) DownloadFile(ctx context.Context, req common.DownloadRequest, opts ...grpc.CallOption) (common.DownloadResponse, error) {
	args := c.Called(ctx, req, opts)
	return args.Get(0).(common.DownloadResponse), args.Error(1)
}

func (c *MockCoordinatorClient) DeleteFile(ctx context.Context, req common.DeleteRequest, opts ...grpc.CallOption) (common.DeleteResponse, error) {
	args := c.Called(ctx, req, opts)
	return args.Get(0).(common.DeleteResponse), args.Error(1)
}

func (c *MockCoordinatorClient) ConfirmUpload(ctx context.Context, req common.ConfirmUploadRequest, opts ...grpc.CallOption) (common.ConfirmUploadResponse, error) {
	args := c.Called(ctx, req, opts)
	return args.Get(0).(common.ConfirmUploadResponse), args.Error(1)
}

func (c *MockCoordinatorClient) ListFiles(ctx context.Context, req common.ListRequest, opts ...grpc.CallOption) (common.ListResponse, error) {
	args := c.Called(ctx, req, opts)
	return args.Get(0).(common.ListResponse), args.Error(1)
}

func (c *MockCoordinatorClient) RegisterDataNode(ctx context.Context, req common.RegisterDataNodeRequest, opts ...grpc.CallOption) (common.RegisterDataNodeResponse, error) {
	args := c.Called(ctx, req, opts)
	return args.Get(0).(common.RegisterDataNodeResponse), args.Error(1)
}

func (c *MockCoordinatorClient) DataNodeHeartbeat(ctx context.Context, req common.HeartbeatRequest, opts ...grpc.CallOption) (common.HeartbeatResponse, error) {
	args := c.Called(ctx, req, opts)
	return args.Get(0).(common.HeartbeatResponse), args.Error(1)
}

func (c *MockCoordinatorClient) ListNodes(ctx context.Context, rqe common.ListNodesRequest, opts ...grpc.CallOption) (common.ListNodesResponse, error) {
	args := c.Called(ctx, rqe, opts)
	return args.Get(0).(common.ListNodesResponse), args.Error(1)
}

func (c *MockCoordinatorClient) GetChunksForNode(ctx context.Context, req common.GetChunksForNodeRequest, opts ...grpc.CallOption) (common.GetChunksForNodeResponse, error) {
	args := c.Called(ctx, req, opts)
	return args.Get(0).(common.GetChunksForNodeResponse), args.Error(1)
}
