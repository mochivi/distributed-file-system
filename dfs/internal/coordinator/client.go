package coordinator

import (
	"context"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ICoordinatorClient interface {
	Close() error
	UploadFile(ctx context.Context, req UploadRequest, opts ...grpc.CallOption) (UploadResponse, error)
	DownloadFile(ctx context.Context, req DownloadRequest, opts ...grpc.CallOption) (DownloadResponse, error)
	DeleteFile(ctx context.Context, req DeleteRequest, opts ...grpc.CallOption) (DeleteResponse, error)
	ConfirmUpload(ctx context.Context, req ConfirmUploadRequest, opts ...grpc.CallOption) (ConfirmUploadResponse, error)
	ListFiles(ctx context.Context, req ListRequest, opts ...grpc.CallOption) (ListResponse, error)
	RegisterDataNode(ctx context.Context, req RegisterDataNodeRequest, opts ...grpc.CallOption) (RegisterDataNodeResponse, error)
	DataNodeHeartbeat(ctx context.Context, req HeartbeatRequest, opts ...grpc.CallOption) (HeartbeatResponse, error)
	ListNodes(ctx context.Context, rqe ListNodesRequest, opts ...grpc.CallOption) (ListNodesResponse, error)
	Node() *common.DataNodeInfo
}

// Wrapper over the proto.CoordinatorServiceClient interface
type CoordinatorClient struct {
	client proto.CoordinatorServiceClient
	conn   *grpc.ClientConn
	node   *common.DataNodeInfo
}

func NewCoordinatorClient(node *common.DataNodeInfo) (*CoordinatorClient, error) {
	conn, err := grpc.NewClient(
		node.Endpoint(),
		grpc.WithTransportCredentials(insecure.NewCredentials()), // Update to TLS in prod
	)
	if err != nil {
		return nil, err
	}

	client := proto.NewCoordinatorServiceClient(conn)

	return &CoordinatorClient{
		client: client,
		conn:   conn,
		node:   node,
	}, nil
}

// Close closes the underlying connection
func (c *CoordinatorClient) Close() error {
	return c.conn.Close()
}

func (c *CoordinatorClient) UploadFile(ctx context.Context, req UploadRequest, opts ...grpc.CallOption) (UploadResponse, error) {
	resp, err := c.client.UploadFile(ctx, req.ToProto(), opts...)
	if err != nil {
		return UploadResponse{}, err
	}
	return UploadResponseFromProto(resp), nil
}

func (c *CoordinatorClient) DownloadFile(ctx context.Context, req DownloadRequest, opts ...grpc.CallOption) (DownloadResponse, error) {
	resp, err := c.client.DownloadFile(ctx, req.ToProto(), opts...)
	if err != nil {
		return DownloadResponse{}, err
	}
	return DownloadResponseFromProto(resp), nil
}

func (c *CoordinatorClient) DeleteFile(ctx context.Context, req DeleteRequest, opts ...grpc.CallOption) (DeleteResponse, error) {
	resp, err := c.client.DeleteFile(ctx, req.ToProto(), opts...)
	if err != nil {
		return DeleteResponse{}, err
	}
	return DeleteResponseFromProto(resp), nil
}

func (c *CoordinatorClient) ConfirmUpload(ctx context.Context, req ConfirmUploadRequest, opts ...grpc.CallOption) (ConfirmUploadResponse, error) {
	resp, err := c.client.ConfirmUpload(ctx, req.ToProto(), opts...)
	if err != nil {
		return ConfirmUploadResponse{}, err
	}
	return ConfirmUploadResponseFromProto(resp), nil
}

func (c *CoordinatorClient) ListFiles(ctx context.Context, req ListRequest, opts ...grpc.CallOption) (ListResponse, error) {
	resp, err := c.client.ListFiles(ctx, req.ToProto(), opts...)
	if err != nil {
		return ListResponse{}, err
	}
	return ListResponseFromProto(resp), nil
}

func (c *CoordinatorClient) RegisterDataNode(ctx context.Context, req RegisterDataNodeRequest, opts ...grpc.CallOption) (RegisterDataNodeResponse, error) {
	resp, err := c.client.RegisterDataNode(ctx, req.ToProto(), opts...)
	if err != nil {
		return RegisterDataNodeResponse{}, err
	}
	return RegisterDataNodeResponseFromProto(resp), nil
}

func (c *CoordinatorClient) DataNodeHeartbeat(ctx context.Context, req HeartbeatRequest, opts ...grpc.CallOption) (HeartbeatResponse, error) {
	resp, err := c.client.DataNodeHeartbeat(ctx, req.ToProto(), opts...)
	if err != nil {
		return HeartbeatResponse{}, err
	}
	return HeartbeatResponseFromProto(resp), nil
}

func (c *CoordinatorClient) ListNodes(ctx context.Context, rqe ListNodesRequest, opts ...grpc.CallOption) (ListNodesResponse, error) {
	resp, err := c.client.ListNodes(context.Background(), &proto.ListNodesRequest{}, opts...)
	if err != nil {
		return ListNodesResponse{}, err
	}
	return ListNodesResponseFromProto(resp), nil
}

// Other methods not related to the gRPC server
func (c *CoordinatorClient) Node() *common.DataNodeInfo {
	return c.node
}
