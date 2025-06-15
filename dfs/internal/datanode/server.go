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
	N_REPLICAS int = 3
	N_NODES    int = (N_REPLICAS-1)*2 + 1
)

// PrepareChunkUpload is received only if the node is a primary receiver, should reply to accept the chunk
// and create a streaming session for the chunk data stream
func (s *DataNodeServer) PrepareChunkUpload(ctx context.Context, pb *proto.ChunkMeta) (*proto.ChunkUploadReady, error) {
	req := common.ChunkMetaFromProto(pb)

	logger := logging.OperationLogger(s.logger, "prepare_chunk_upload", slog.String("chunk_id", req.ChunkID))
	logger.Info("Received PrepareChunkUpload request")

	// Validate request
	if req.ChunkID == "" || req.ChunkSize <= 0 {
		logger.Error("Invalid chunk metadata")
		return common.ChunkUploadReady{
			Accept:  false,
			Message: "Invalid chunk metadata",
		}.ToProto(), nil
	}
	if !s.hasCapacity(req.ChunkSize) {
		logger.Error("Insufficient storage capacity")
		return common.ChunkUploadReady{
			Accept:  false,
			Message: "Insufficient storage capacity",
		}.ToProto(), nil
	}

	sessionID := uuid.NewString()
	if err := s.createStreamingSession(sessionID, req, logger); err != nil {
		logger.Error("Failed to create streaming session", slog.String("error", err.Error()))
		return nil, status.Errorf(codes.AlreadyExists, "failed to create streaming session: %v", err)
	}

	return common.ChunkUploadReady{
		Accept:    true,
		Message:   "Ready to receive chunk data",
		SessionID: sessionID,
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

// This is the side that is responsible for receiving the chunk data from some peer (client or another node)
func (s *DataNodeServer) StreamChunk(stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) error {
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
			buf := make([]byte, 0, session.ChunkSize)
			buffer = bytes.NewBuffer(buf)
		}

		// Verify data integrity and ordering
		if chunk.Offset != totalReceived {
			session.logger.Error("Data out of order", slog.Int("expected", chunk.Offset), slog.Int("got", totalReceived))
			return errors.New("data out of order")
		}

		// Write to buffer/temp file, update checksum
		// session.logger.Debug("Writing chunk data to buffer", slog.Int("offset", chunk.Offset), slog.Int("chunk_size", len(chunk.Data)))
		buffer.Write(chunk.Data)
		session.RunningChecksum.Write(chunk.Data)
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

		// session.logger.Debug("Sending acknowledgment", slog.Int("bytes_received", totalReceived))
		if err := stream.Send(ack.ToProto()); err != nil {
			session.logger.Error("Failed to send acknowledgment", slog.String("error", err.Error()))
			return err
		}

		// Handle final chunk
		if chunk.IsFinal {
			// Verify checksum immediately
			computedHash := session.RunningChecksum.Sum(nil)
			expectedHash, _ := hex.DecodeString(session.Checksum)

			if !bytes.Equal(computedHash, expectedHash) {
				session.logger.Error("Checksum mismatch", slog.String("expected", session.Checksum), slog.String("computed", hex.EncodeToString(computedHash)))
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
	s.sessionManager.Delete(session.SessionID) // Delete session after successful stream

	// Propagate the chunk to other nodes if the flag is set
	if session.Propagate {
		if err := s.replicate(session.ChunkMeta, buffer.Bytes()); err != nil {
			session.logger.Error("Failed to replicate chunk", slog.String("error", err.Error()))
			return err
		}
	}

	return nil
}

// Even though the general operation is based off of heartbeat requests from the datanode to the coordiantor
// It might be useful to still have a healthcheck endpoint for the datanode
func (s *DataNodeServer) HealthCheck(ctx context.Context, pb *proto.HealthCheckRequest) (*proto.HealthCheckResponse, error) {
	return nil, nil
}

func (s *DataNodeServer) RegisterWithCoordinator(ctx context.Context, coordinatorNode *common.DataNodeInfo) error {
	logger := logging.OperationLogger(s.logger, "register", slog.String("coordinator_address", coordinatorNode.Endpoint()))
	logger.Info("Registering with coordinator")

	coordinatorClient, err := coordinator.NewCoordinatorClient(coordinatorNode)
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

// Actually replicates the chunk to the given nodes in parallel
func (s *DataNodeServer) replicate(chunkMeta common.ChunkMeta, data []byte) error {
	logger := logging.OperationLogger(s.logger, "replicate_chunk", slog.String("chunk_id", chunkMeta.ChunkID))

	// Select N_NODES possible nodes to replicate to
	nodes, err := s.nodeManager.SelectBestNodes(N_NODES)
	if err != nil {
		logger.Error("Failed to select nodes", slog.String("error", err.Error()))
		return fmt.Errorf("failed to select nodes: %v", err)
	}

	// Replicate to N_REPLICAS nodes
	if err := s.replicationManager.paralellReplicate(nodes, chunkMeta, data, N_REPLICAS-1); err != nil {
		logger.Error("Failed to replicate chunk", slog.String("error", err.Error()))
		return fmt.Errorf("failed to replicate chunk: %v", err)
	}
	logger.Info("Chunk replicated successfully")

	return nil
}
