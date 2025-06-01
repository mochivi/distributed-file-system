package client

import (
	"context"
	"fmt"
	"log"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
)

// client -> node
func (c *DataNodeClient) StoreChunk(ctx context.Context, req common.StoreChunkRequest) error {
	resp, err := c.client.StoreChunk(ctx, req.ToProto())
	if err != nil {
		return handlegRPCError(err, req.ChunkID)
	}

	if !resp.Success {
		return fmt.Errorf("failed to store chunk: %s", resp.Message)
	}

	log.Printf("Successfully stored chunk %s", req.ChunkID)
	return nil
}

// client -> node
func (c *DataNodeClient) RetrieveChunk(ctx context.Context, req common.RetrieveChunkRequest) (common.RetrieveChunkResponse, error) {
	resp, err := c.client.RetrieveChunk(ctx, req.ToProto())
	if err != nil {
		err = handlegRPCError(err, req.ChunkID)
		return common.RetrieveChunkResponse{}, err
	}
	return common.RetrieveChunkResponseFromProto(resp), nil
}

// client -> node
func (c *DataNodeClient) DeleteChunk(ctx context.Context, req common.DeleteChunkRequest) (common.DeleteChunkResponse, error) {
	resp, err := c.client.DeleteChunk(ctx, req.ToProto())
	if err != nil {
		err = handlegRPCError(err, req.ChunkID)
		return common.DeleteChunkResponse{}, err
	}
	return common.DeleteChunkResponseFromProto(resp), nil
}

// node -> node
// add context cancellation support
func (c *DataNodeClient) ReplicateChunk(ctx context.Context, req common.ReplicateChunkRequest) (common.ReplicateChunkResponse, error) {
	resp, err := c.client.ReplicateChunk(ctx, req.ToProto())
	if err != nil {
		err = handlegRPCError(err, req.ChunkID)
		return common.ReplicateChunkResponse{}, err
	}
	return common.ReplicateChunkResponseFromProto(resp), nil
}

// node -> node
func (c *DataNodeClient) StreamChunkData(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[proto.ChunkDataStream, proto.ChunkDataAck], error) {
	return nil, nil
}

// node -> node
func (c *DataNodeClient) HealthCheck(ctx context.Context, in *proto.HealthCheckRequest, opts ...grpc.CallOption) (common.HealthCheckResponse, error) {
	return common.HealthCheckResponse{}, nil
}

// Close closes the underlying connection
func (c *DataNodeClient) Close() error {
	return c.conn.Close()
}
