package clients

import (
	"context"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

type mockCoordinatorClient struct {
	mock.Mock
	node *common.DataNodeInfo
}

func NewMockCoordinatorClient(node *common.DataNodeInfo) *mockCoordinatorClient {
	return &mockCoordinatorClient{
		Mock: mock.Mock{},
		node: node,
	}
}

func (m *mockCoordinatorClient) Node() *common.DataNodeInfo {
	return m.node
}

func (m *mockCoordinatorClient) Close() error {
	return m.Called().Error(0)
}

func (c *mockCoordinatorClient) UploadFile(ctx context.Context, req common.UploadRequest, opts ...grpc.CallOption) (common.UploadResponse, error) {
	args := c.Called(ctx, req, opts)
	return args.Get(0).(common.UploadResponse), args.Error(1)
}

func (c *mockCoordinatorClient) DownloadFile(ctx context.Context, req common.DownloadRequest, opts ...grpc.CallOption) (common.DownloadResponse, error) {
	args := c.Called(ctx, req, opts)
	return args.Get(0).(common.DownloadResponse), args.Error(1)
}

func (c *mockCoordinatorClient) DeleteFile(ctx context.Context, req common.DeleteRequest, opts ...grpc.CallOption) (common.DeleteResponse, error) {
	args := c.Called(ctx, req, opts)
	return args.Get(0).(common.DeleteResponse), args.Error(1)
}

func (c *mockCoordinatorClient) ConfirmUpload(ctx context.Context, req common.ConfirmUploadRequest, opts ...grpc.CallOption) (common.ConfirmUploadResponse, error) {
	args := c.Called(ctx, req, opts)
	return args.Get(0).(common.ConfirmUploadResponse), args.Error(1)
}

func (c *mockCoordinatorClient) ListFiles(ctx context.Context, req common.ListRequest, opts ...grpc.CallOption) (common.ListResponse, error) {
	args := c.Called(ctx, req, opts)
	return args.Get(0).(common.ListResponse), args.Error(1)
}

func (c *mockCoordinatorClient) RegisterDataNode(ctx context.Context, req common.RegisterDataNodeRequest, opts ...grpc.CallOption) (common.RegisterDataNodeResponse, error) {
	args := c.Called(ctx, req, opts)
	return args.Get(0).(common.RegisterDataNodeResponse), args.Error(1)
}

func (c *mockCoordinatorClient) DataNodeHeartbeat(ctx context.Context, req common.HeartbeatRequest, opts ...grpc.CallOption) (common.HeartbeatResponse, error) {
	args := c.Called(ctx, req, opts)
	return args.Get(0).(common.HeartbeatResponse), args.Error(1)
}
func (c *mockCoordinatorClient) ListNodes(ctx context.Context, rqe common.ListNodesRequest, opts ...grpc.CallOption) (common.ListNodesResponse, error) {
	args := c.Called(ctx, rqe, opts)
	return args.Get(0).(common.ListNodesResponse), args.Error(1)
}
