package coordinator

import (
	"context"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

type MockCoordinatorClient struct {
	mock.Mock
}

func (c *MockCoordinatorClient) UploadFile(ctx context.Context, req UploadRequest, opts ...grpc.CallOption) (UploadResponse, error) {
	args := c.Called(ctx, req)
	return args.Get(0).(UploadResponse), args.Error(1)
}

func (c *MockCoordinatorClient) DownloadFile(ctx context.Context, req DownloadRequest, opts ...grpc.CallOption) (DownloadResponse, error) {
	args := c.Called(ctx, req)
	return args.Get(0).(DownloadResponse), args.Error(1)
}

func (c *MockCoordinatorClient) DeleteFile(ctx context.Context, req DeleteRequest, opts ...grpc.CallOption) (DeleteResponse, error) {
	args := c.Called(ctx, req)
	return args.Get(0).(DeleteResponse), args.Error(1)
}

func (c *MockCoordinatorClient) ConfirmUpload(ctx context.Context, req ConfirmUploadRequest, opts ...grpc.CallOption) (ConfirmUploadResponse, error) {
	args := c.Called(ctx, req)
	return args.Get(0).(ConfirmUploadResponse), args.Error(1)
}

func (c *MockCoordinatorClient) ListFiles(ctx context.Context, req ListRequest, opts ...grpc.CallOption) (ListResponse, error) {
	args := c.Called(ctx, req)
	return args.Get(0).(ListResponse), args.Error(1)
}

func (c *MockCoordinatorClient) RegisterDataNode(ctx context.Context, req RegisterDataNodeRequest, opts ...grpc.CallOption) (RegisterDataNodeResponse, error) {
	args := c.Called(ctx, req)
	return args.Get(0).(RegisterDataNodeResponse), args.Error(1)
}

func (c *MockCoordinatorClient) DataNodeHeartbeat(ctx context.Context, req HeartbeatRequest, opts ...grpc.CallOption) (HeartbeatResponse, error) {
	args := c.Called(ctx, req)
	return args.Get(0).(HeartbeatResponse), args.Error(1)
}

func (c *MockCoordinatorClient) ListNodes(ctx context.Context, rqe ListNodesRequest, opts ...grpc.CallOption) (ListNodesResponse, error) {
	args := c.Called(ctx, rqe)
	return args.Get(0).(ListNodesResponse), args.Error(1)
}

func (c *MockCoordinatorClient) Close() error {
	args := c.Called()
	return args.Error(0)
}

func (c *MockCoordinatorClient) Node() *common.DataNodeInfo {
	args := c.Called()
	return args.Get(0).(*common.DataNodeInfo)
}
