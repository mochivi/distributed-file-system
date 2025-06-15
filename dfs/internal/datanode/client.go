package datanode

import (
	"context"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
)

// client -> node
// This is the primary node that is receiving the chunk
func (c *DataNodeClient) StoreChunk(ctx context.Context, req common.ChunkMeta, opts ...grpc.CallOption) (common.ChunkUploadReady, error) {
	resp, err := c.client.PrepareChunkUpload(ctx, req.ToProto(), opts...)
	if err != nil {
		err = handlegRPCError(err, req.ChunkID)
		return common.ChunkUploadReady{}, err
	}
	return common.ChunkUploadReadyFromProto(resp), nil
}

// client -> node
func (c *DataNodeClient) RetrieveChunk(ctx context.Context, req common.RetrieveChunkRequest, opts ...grpc.CallOption) (common.RetrieveChunkResponse, error) {
	resp, err := c.client.RetrieveChunk(ctx, req.ToProto(), opts...)
	if err != nil {
		err = handlegRPCError(err, req.ChunkID)
		return common.RetrieveChunkResponse{}, err
	}
	return common.RetrieveChunkResponseFromProto(resp), nil
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
func (c *DataNodeClient) ReplicateChunk(ctx context.Context, req common.ChunkMeta, opts ...grpc.CallOption) (common.ChunkUploadReady, error) {
	resp, err := c.client.PrepareChunkUpload(ctx, req.ToProto(), opts...)
	if err != nil {
		err = handlegRPCError(err, req.ChunkID)
		return common.ChunkUploadReady{}, err
	}
	return common.ChunkUploadReadyFromProto(resp), nil
}

// node -> node
func (c *DataNodeClient) StreamChunk(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[proto.ChunkDataStream, proto.ChunkDataAck], error) {
	return c.client.StreamChunk(ctx, opts...)
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
