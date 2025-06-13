package datanode

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/google/uuid"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/coordinator"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TODO: make this configurable by the client
const (
	N_NODES    int = 1
	N_REPLICAS int = 1
)

// StoreChunk is received only if the node is a primary receiver
// 1. Verify chunk checksum is the same as expected
// 2. Store chunk, given storage method
// 3. Kickoff replication
func (s *DataNodeServer) StoreChunk(ctx context.Context, pb *proto.StoreChunkRequest) (*proto.StoreChunkResponse, error) {
	req := common.StoreChunkRequestFromProto(pb)

	logger := logging.OperationLogger(s.logger, "store_chunk", slog.String("chunk_id", req.ChunkID))
	logger.Info("Received StoreChunk request")

	calculatedChecksum := common.CalculateChecksum(req.Data)
	if calculatedChecksum != req.Checksum {
		logger.Error("Checksum mismatch", slog.String("expected", req.Checksum), slog.String("calculated", calculatedChecksum))
		return common.StoreChunkResponse{
			Success: false,
			Message: "checksum does not match",
		}.ToProto(), nil
	}

	if err := s.store.Store(req.ChunkID, req.Data); err != nil {
		logger.Error("Failed to store chunk", slog.String("error", err.Error()))
		return nil, status.Errorf(codes.Internal, "failed to store chunk: %v", err)
	}

	replicateReq := common.ReplicateChunkRequest{
		ChunkID:   req.ChunkID,
		ChunkSize: 4 * 1024 * 1024,
		Checksum:  req.Checksum,
	}

	// Select N_NODES possible nodes to replicate to
	nodes, err := s.nodeManager.SelectBestNodes(N_NODES)
	if err != nil {
		logger.Error("Failed to select nodes", slog.String("error", err.Error()))
		return nil, status.Errorf(codes.Internal, "failed to select nodes: %v", err)
	}

	// Replicate to N_REPLICAS nodes
	if err := s.replicationManager.paralellReplicate(nodes, replicateReq, req.Data, N_REPLICAS); err != nil {
		logger.Error("Failed to replicate chunk", slog.String("error", err.Error()))
		return nil, status.Errorf(codes.Internal, "failed to replicate chunk: %v", err)
	}
	logger.Info("Chunk replicated successfully")

	return common.StoreChunkResponse{
		Success: true,
		Message: "chunk stored",
	}.ToProto(), nil
}

// Any node, replica or primary, can serve chunk reads at any moment
func (s *DataNodeServer) RetrieveChunk(ctx context.Context, pb *proto.RetrieveChunkRequest) (*proto.RetrieveChunkResponse, error) {
	req := common.RetrieveChunkRequestFromProto(pb)

	logger := logging.OperationLogger(s.logger, "retrieve_chunk", slog.String("chunk_id", req.ChunkID))
	logger.Info("Received RetrieveChunk request")

	// Move to store layer later
	if !s.store.Exists(req.ChunkID) {
		logger.Error("Chunk not found")
		return nil, status.Errorf(codes.NotFound, "file not found")
	}

	data, err := s.store.Retrieve(req.ChunkID)
	if err != nil {
		logger.Error("Failed to retrieve chunk", slog.String("error", err.Error()))
		return nil, status.Errorf(codes.Internal, "failed to retrieve chunk: %v", err)
	}

	logger.Info("Chunk retrieved successfully")

	return common.RetrieveChunkResponse{
		Data:     data,
		Checksum: common.CalculateChecksum(data),
	}.ToProto(), nil
}

func (s *DataNodeServer) DeleteChunk(ctx context.Context, pb *proto.DeleteChunkRequest) (*proto.DeleteChunkResponse, error) {
	req := common.DeleteChunkRequestFromProto(pb)

	logger := logging.OperationLogger(s.logger, "delete_chunk", slog.String("chunk_id", req.ChunkID))
	logger.Info("Received DeleteChunk request")

	if !s.store.Exists(req.ChunkID) {
		logger.Error("Chunk not found")
		return common.DeleteChunkResponse{
			Success: false,
			Message: "chunk not found",
		}.ToProto(), nil
	}

	if err := s.store.Delete(req.ChunkID); err != nil {
		logger.Error("Failed to delete chunk", slog.String("error", err.Error()))
		return nil, status.Errorf(codes.Internal, "failed to delete chunk: %v", err)
	}

	logger.Info("Chunk deleted successfully")

	return common.DeleteChunkResponse{
		Success: true,
		Message: "chunk deleted",
	}.ToProto(), nil
}

// ReplicateChunk means node is currenly acting as a replica for some chunk
// The replication implementation here is paralell, which means this node does not need to care about forwarding
// the replication request to some new node, just replying with the status is enough
// 1. Check if node has enough capacity to handle storing the chunk
// 2. Send accept response to innitiate data stream for chunk
func (s *DataNodeServer) ReplicateChunk(ctx context.Context, pb *proto.ReplicateChunkRequest) (*proto.ReplicateChunkResponse, error) {
	req := common.ReplicateChunkRequestFromProto(pb)
	logger := logging.OperationLogger(s.logger, "receive_replicate_chunk", slog.String("chunk_id", req.ChunkID))

	logger.Info("Received ReplicateChunk request")

	if req.ChunkID == "" || req.ChunkSize <= 0 {
		logger.Error("Invalid chunk metadata")
		return common.ReplicateChunkResponse{
			Accept:  false,
			Message: "Invalid chunk metadata",
		}.ToProto(), nil
	}

	if !s.hasCapacity(req.ChunkSize) {
		logger.Error("Insufficient storage capacity")
		return &proto.ReplicateChunkResponse{
			Accept:  false,
			Message: "Insufficient storage capacity",
		}, nil
	}

	sessionID := uuid.NewString()
	s.createStreamingSession(sessionID, req, logger)

	return common.ReplicateChunkResponse{
		Accept:    true,
		Message:   "Ready to receive chunk data",
		SessionID: sessionID,
	}.ToProto(), nil
}

// This is the side that is responsible for receiving the chunk data from the client
func (s *DataNodeServer) StreamChunkData(stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) error {
	var session *StreamingSession
	var buffer *bytes.Buffer
	var totalReceived int

	for {
		chunkpb, err := stream.Recv()
		if err == io.EOF {
			if session != nil {
				session.logger.Debug("Received EOF, closing session")
			}
			break
		}
		if err != nil {
			if session != nil {
				session.logger.Error("Failed to receive chunk data", slog.String("error", err.Error()))
			}
			return err
		}

		// Convert to internal struct
		chunk := common.ChunkDataStreamFromProto(chunkpb)

		// First chunk establishes session
		if session == nil {
			session = s.getStreamingSession(chunk.SessionID)
			if session == nil {
				return errors.New("invalid session")
			}

			// Create buffer with pre-defined capacity for performance
			buf := make([]byte, 0, session.ExpectedSize)
			buffer = bytes.NewBuffer(buf)
		}

		// Verify data integrity and ordering
		if chunk.Offset != totalReceived {
			session.logger.Error("Data out of order", slog.Int("expected", chunk.Offset), slog.Int("got", totalReceived))
			return errors.New("data out of order")
		}

		// Write to buffer/temp file, update checksum
		session.logger.Debug("Writing chunk data to buffer", slog.Int("offset", chunk.Offset), slog.Int("chunk_size", len(chunk.Data)))
		buffer.Write(chunk.Data)
		session.Checksum.Write(chunk.Data)
		totalReceived += len(chunk.Data)

		// TODO: implement flushing buffer to disk whenever it hits the threshold
		// TODO: must setup temporary state materialization in some special folder
		// TODO: Ideally, we should avoid flushing to disk if there is enough available RAM
		// Flush to disk if buffer gets too big
		// if buffer.Len() >= int(s.config.FlushSize) {
		// 	if err := s.flushBufferToDisk(session, &buffer); err != nil {
		// 		return err
		// 	}
		// }

		// // Flow control decision
		// readyForNext := buffer.Len() < int(s.config.BufferThreshold)

		// Send acknowledgment with flow control
		ack := &common.ChunkDataAck{
			SessionID:     chunk.SessionID,
			Success:       true,
			BytesReceived: totalReceived,
			ReadyForNext:  true, // Requires flushing to be setup to work
		}

		session.logger.Debug("Sending acknowledgment", slog.Int("bytes_received", totalReceived))
		if err := stream.Send(ack.ToProto()); err != nil {
			session.logger.Error("Failed to send acknowledgment", slog.String("error", err.Error()))
			return err
		}

		// Handle final chunk
		if chunk.IsFinal {
			// Verify checksum immediately
			computedHash := session.Checksum.Sum(nil)
			expectedHash, _ := hex.DecodeString(session.ExpectedHash)

			if !bytes.Equal(computedHash, expectedHash) {
				session.logger.Error("Checksum mismatch", slog.String("expected", session.ExpectedHash), slog.String("computed", hex.EncodeToString(computedHash)))
				return errors.New("checksum mismatch")
			}

			// Store the chunk immediately
			err := s.store.Store(session.ChunkID, buffer.Bytes())
			if err != nil {
				session.logger.Error("Failed to store chunk", slog.String("error", err.Error()))
				return err
			}

			// Send final ack with verification result
			finalAck := common.ChunkDataAck{
				SessionID:     chunk.SessionID,
				Success:       true,
				Message:       "Chunk stored successfully",
				BytesReceived: totalReceived,
			}
			return stream.Send(finalAck.ToProto())
		}
	}

	session.logger.Debug("Received chunk data stream successfully")
	return nil
}

// Even though the general operation is based off of heartbeat requests from the datanode to the coordiantor
// It might be useful to still have a healthcheck endpoint for the datanode
func (s *DataNodeServer) HealthCheck(ctx context.Context, pb *proto.HealthCheckRequest) (*proto.HealthCheckResponse, error) {
	return nil, nil
}

func (s *DataNodeServer) RegisterWithCoordinator(ctx context.Context, coordinatorAddress string) error {
	logger := logging.OperationLogger(s.logger, "register", slog.String("coordinator_address", coordinatorAddress))
	logger.Info("Registering with coordinator")

	coordinatorClient, err := coordinator.NewCoordinatorClient(coordinatorAddress)
	if err != nil {
		logger.Error("Failed to create coordinator client", slog.String("error", err.Error()))
		return fmt.Errorf("failed to create coordinator client: %v", err)
	}

	req := coordinator.RegisterDataNodeRequest{NodeInfo: s.Config.Info}
	resp, err := coordinatorClient.RegisterDataNode(ctx, req)
	if err != nil {
		logger.Error("Failed to register datanode with coordinator", slog.String("error", err.Error()))
		return fmt.Errorf("failed to register datanode with coordinator: %v", err)
	}

	if !resp.Success {
		logger.Error("Failed to register datanode with coordinator", slog.String("error", resp.Message))
		return fmt.Errorf("failed to register datanode with coordinator: %s", resp.Message)
	}

	// Save information about all nodes
	s.nodeManager.InitializeNodes(resp.FullNodeList, resp.CurrentVersion)

	logger.Info("Datanode registered with coordinator successfully")
	return nil
}
