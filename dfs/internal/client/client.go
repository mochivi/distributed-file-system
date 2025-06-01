package client

import (
	"context"
	"fmt"
	"log"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Close closes the underlying connection
func (c *DataNodeClient) Close() error {
	return c.conn.Close()
}

// StoreChunk stores a chunk of data
func (c *DataNodeClient) StoreChunk(ctx context.Context, req common.StoreChunkRequest) error {
	resp, err := c.client.StoreChunk(ctx, req.ToProto())
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("failed to store chunk: %s", resp.Message)
	}

	log.Printf("Successfully stored chunk %s", req.ChunkID)
	return nil
}

func (c *DataNodeClient) RetrieveChunk(ctx context.Context, req common.RetrieveChunkRequest) (*proto.RetrieveChunkResponse, error) {
	resp, err := c.client.RetrieveChunk(ctx, req.ToProto())
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.NotFound:
				log.Printf("Chunk %s not found: %s", req.ChunkID, st.Message())
				return nil, err
			case codes.Internal:
				log.Printf("Internal server error: %s", st.Message())
				return nil, err
			case codes.Unavailable:
				log.Printf("Server unavailable: %s", st.Message())
				return nil, err
			case codes.DeadlineExceeded:
				log.Printf("Request timeout: %s", st.Message())
				return nil, err
			default:
				log.Printf("Unexpected error (code: %s): %s", st.Code(), st.Message())
				return nil, err
			}
		} else {
			// Not a gRPC status error (network error, etc.)
			log.Printf("Non-gRPC error: %v", err)
			return nil, err
		}
	}
	return resp, nil
}

func (c *DataNodeClient) DeleteChunk(ctx context.Context, in *proto.DeleteChunkRequest, opts ...grpc.CallOption) (*proto.DeleteChunkResponse, error) {
	return nil, nil
}

func (c *DataNodeClient) ReplicateChunk(ctx context.Context, in *proto.ReplicateChunkRequest, opts ...grpc.CallOption) (*proto.ReplicateChunkResponse, error) {
	return nil, nil
}

func (c *DataNodeClient) StreamChunkData(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[proto.ChunkDataStream, proto.ChunkDataAck], error) {
	return nil, nil
}

func (c *DataNodeClient) HealthCheck(ctx context.Context, in *proto.HealthCheckRequest, opts ...grpc.CallOption) (*proto.HealthCheckResponse, error) {
	return nil, nil
}
