package clients

import (
	"context"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ICoordinatorClient interface {
	UploadFile(ctx context.Context, req common.UploadRequest, opts ...grpc.CallOption) (common.UploadResponse, error)
	DownloadFile(ctx context.Context, req common.DownloadRequest, opts ...grpc.CallOption) (common.DownloadResponse, error)
	DeleteFile(ctx context.Context, req common.DeleteRequest, opts ...grpc.CallOption) (common.DeleteResponse, error)
	ConfirmUpload(ctx context.Context, req common.ConfirmUploadRequest, opts ...grpc.CallOption) (common.ConfirmUploadResponse, error)
	ListFiles(ctx context.Context, req common.ListRequest, opts ...grpc.CallOption) (common.ListResponse, error)
	RegisterDataNode(ctx context.Context, req common.RegisterDataNodeRequest, opts ...grpc.CallOption) (common.RegisterDataNodeResponse, error)
	DataNodeHeartbeat(ctx context.Context, req common.HeartbeatRequest, opts ...grpc.CallOption) (common.HeartbeatResponse, error)
	ListNodes(ctx context.Context, rqe common.ListNodesRequest, opts ...grpc.CallOption) (common.ListNodesResponse, error)
	Node() *common.NodeInfo
	Close() error
}

// Wrapper over the proto.CoordinatorServiceClient interface
type CoordinatorClient struct {
	client proto.CoordinatorServiceClient
	conn   *grpc.ClientConn
	node   *common.NodeInfo
}

func NewCoordinatorClient(node *common.NodeInfo, opts ...grpc.DialOption) (ICoordinatorClient, error) {
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(
		node.Endpoint(),
		opts...,
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

func (c *CoordinatorClient) UploadFile(ctx context.Context, req common.UploadRequest, opts ...grpc.CallOption) (common.UploadResponse, error) {
	resp, err := c.client.UploadFile(ctx, req.ToProto(), opts...)
	if err != nil {
		return common.UploadResponse{}, err
	}
	return common.UploadResponseFromProto(resp), nil
}

func (c *CoordinatorClient) DownloadFile(ctx context.Context, req common.DownloadRequest, opts ...grpc.CallOption) (common.DownloadResponse, error) {
	resp, err := c.client.DownloadFile(ctx, req.ToProto(), opts...)
	if err != nil {
		return common.DownloadResponse{}, err
	}
	return common.DownloadResponseFromProto(resp), nil
}

func (c *CoordinatorClient) DeleteFile(ctx context.Context, req common.DeleteRequest, opts ...grpc.CallOption) (common.DeleteResponse, error) {
	resp, err := c.client.DeleteFile(ctx, req.ToProto(), opts...)
	if err != nil {
		return common.DeleteResponse{}, err
	}
	return common.DeleteResponseFromProto(resp), nil
}

func (c *CoordinatorClient) ConfirmUpload(ctx context.Context, req common.ConfirmUploadRequest, opts ...grpc.CallOption) (common.ConfirmUploadResponse, error) {
	resp, err := c.client.ConfirmUpload(ctx, req.ToProto(), opts...)
	if err != nil {
		return common.ConfirmUploadResponse{}, err
	}
	return common.ConfirmUploadResponseFromProto(resp), nil
}

func (c *CoordinatorClient) ListFiles(ctx context.Context, req common.ListRequest, opts ...grpc.CallOption) (common.ListResponse, error) {
	resp, err := c.client.ListFiles(ctx, req.ToProto(), opts...)
	if err != nil {
		return common.ListResponse{}, err
	}
	return common.ListResponseFromProto(resp), nil
}

func (c *CoordinatorClient) RegisterDataNode(ctx context.Context, req common.RegisterDataNodeRequest, opts ...grpc.CallOption) (common.RegisterDataNodeResponse, error) {
	resp, err := c.client.RegisterDataNode(ctx, req.ToProto(), opts...)
	if err != nil {
		return common.RegisterDataNodeResponse{}, err
	}
	return common.RegisterDataNodeResponseFromProto(resp), nil
}

func (c *CoordinatorClient) DataNodeHeartbeat(ctx context.Context, req common.HeartbeatRequest, opts ...grpc.CallOption) (common.HeartbeatResponse, error) {
	resp, err := c.client.DataNodeHeartbeat(ctx, req.ToProto(), opts...)
	if err != nil {
		return common.HeartbeatResponse{}, err
	}
	return common.HeartbeatResponseFromProto(resp), nil
}

func (c *CoordinatorClient) ListNodes(ctx context.Context, rqe common.ListNodesRequest, opts ...grpc.CallOption) (common.ListNodesResponse, error) {
	resp, err := c.client.ListNodes(context.Background(), &proto.ListNodesRequest{}, opts...)
	if err != nil {
		return common.ListNodesResponse{}, err
	}
	return common.ListNodesResponseFromProto(resp), nil
}

// Other methods not related to the gRPC server
func (c *CoordinatorClient) Node() *common.NodeInfo {
	return c.node
}
