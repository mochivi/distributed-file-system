package clients

import (
	"context"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Wrapper over the proto.DataNodeServiceClient interface
type DataNodeClient struct {
	client proto.DataNodeServiceClient
	conn   *grpc.ClientConn
	Node   *common.NodeInfo
}

func NewDataNodeClient(node *common.NodeInfo, opts ...grpc.DialOption) (*DataNodeClient, error) {
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(
		node.Endpoint(),
		opts...,
	)
	if err != nil {
		return nil, err
	}

	client := proto.NewDataNodeServiceClient(conn)

	return &DataNodeClient{
		client: client,
		conn:   conn,
		Node:   node,
	}, nil
}

// client -> node
// This is the primary node that is receiving the chunk
func (c *DataNodeClient) StoreChunk(ctx context.Context, req common.ChunkHeader, opts ...grpc.CallOption) (common.NodeReady, error) {
	// Convert to expected request type
	uploadChunkRequest := common.UploadChunkRequest{
		ChunkHeader: req,
		Propagate:   true, // StoreChunk is always propagated
	}

	resp, err := c.client.PrepareChunkUpload(ctx, uploadChunkRequest.ToProto(), opts...)
	if err != nil {
		err = handlegRPCError(err, req.ID)
		return common.NodeReady{}, err
	}
	return common.NodeReadyFromProto(resp), nil
}

// client -> node
func (c *DataNodeClient) PrepareChunkDownload(ctx context.Context, req common.DownloadChunkRequest, opts ...grpc.CallOption) (common.DownloadReady, error) {
	resp, err := c.client.PrepareChunkDownload(ctx, req.ToProto(), opts...)
	if err != nil {
		err = handlegRPCError(err, req.ChunkID)
		return common.DownloadReady{}, err
	}
	return common.DownloadReadyFromProto(resp), nil
}

// client -> node
func (c *DataNodeClient) DeleteChunk(ctx context.Context, req common.DeleteChunkRequest, opts ...grpc.CallOption) (common.DeleteChunkResponse, error) {
	resp, err := c.client.DeleteChunk(ctx, req.ToProto(), opts...)
	if err != nil {
		err = handlegRPCError(err, req.ChunkID)
		return common.DeleteChunkResponse{}, err
	}
	return common.DeleteChunkResponseFromProto(resp), nil
}

// node -> node
// This is a replica node that is receiving the chunk
// This is a copy of StoreChunk, but with a different method name
func (c *DataNodeClient) ReplicateChunk(ctx context.Context, req common.ChunkHeader, opts ...grpc.CallOption) (common.NodeReady, error) {
	uploadChunkRequest := common.UploadChunkRequest{
		ChunkHeader: req,
		Propagate:   false, // ReplicateChunk is not propagated, as it is already received by a replica node
	}

	resp, err := c.client.PrepareChunkUpload(ctx, uploadChunkRequest.ToProto(), opts...)
	if err != nil {
		err = handlegRPCError(err, req.ID)
		return common.NodeReady{}, err
	}
	return common.NodeReadyFromProto(resp), nil
}

// node -> node
func (c *DataNodeClient) UploadChunkStream(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[proto.ChunkDataStream, proto.ChunkDataAck], error) {
	return c.client.UploadChunkStream(ctx, opts...)
}

// node -> node
func (c *DataNodeClient) DownloadChunkStream(ctx context.Context, req common.DownloadStreamRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[proto.ChunkDataStream], error) {
	return c.client.DownloadChunkStream(ctx, req.ToProto(), opts...)
}

// node -> node
func (c *DataNodeClient) HealthCheck(ctx context.Context, req common.HealthCheckRequest, opts ...grpc.CallOption) (common.HealthCheckResponse, error) {
	resp, err := c.client.HealthCheck(ctx, req.ToProto(), opts...)
	if err != nil {
		return common.HealthCheckResponse{}, err
	}
	return common.HealthCheckResponseFromProto(resp), nil
}

// Close closes the underlying connection
func (c *DataNodeClient) Close() error {
	return c.conn.Close()
}
