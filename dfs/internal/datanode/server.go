package datanode

import (
	"bytes"
	"context"
	"encoding/hex"
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
func (s *DataNodeServer) PrepareChunkUpload(ctx context.Context, pb *proto.UploadChunkRequest) (*proto.NodeReady, error) {
	req := common.UploadChunkRequestFromProto(pb)

	logger := logging.OperationLogger(s.logger, "chunk_upload", slog.String("chunk_id", req.ChunkHeader.ID))
	logger.Info("Received PrepareChunkUpload request")

	// Validate request
	if req.ChunkHeader.ID == "" || req.ChunkHeader.Size <= 0 {
		logger.Error("Invalid chunk metadata")
		return common.NodeReady{
			Accept:  false,
			Message: "Invalid chunk metadata",
		}.ToProto(), nil
	}
	if !s.hasCapacity(req.ChunkHeader.Size) {
		logger.Error("Insufficient storage capacity")
		return common.NodeReady{
			Accept:  false,
			Message: "Insufficient storage capacity",
		}.ToProto(), nil
	}

	if existing, ok := s.sessionManager.LoadByChunk(req.ChunkHeader.ID); ok {
		switch existing.Status {
		case SessionCompleted, SessionFailed, SessionExpired:
			s.sessionManager.Delete(existing.SessionID) // purge zombie
		default: // SessionActive
			return common.NodeReady{
				Accept:  false,
				Message: "session still active",
			}.ToProto(), nil
		}
	}
	sessionID := uuid.NewString()
	if err := s.createStreamingSession(sessionID, req.ChunkHeader, req.Propagate, logger); err != nil {
		logger.Error("Failed to create streaming session", slog.String("error", err.Error()))
		return nil, status.Errorf(codes.AlreadyExists, "failed to create streaming session: %v", err)
	}

	return common.NodeReady{
		Accept:    true,
		Message:   "Ready to receive chunk data",
		SessionID: sessionID,
	}.ToProto(), nil
}

// TODO: add a different session for download, as it is not the same as the upload session
func (s *DataNodeServer) PrepareChunkDownload(ctx context.Context, pb *proto.DownloadChunkRequest) (*proto.DownloadReady, error) {
	req := common.DownloadChunkRequestFromProto(pb)

	logger := logging.OperationLogger(s.logger, "chunk_download", slog.String("chunk_id", req.ChunkID))
	logger.Info("Received PrepareChunkDownload request")

	if !s.store.Exists(req.ChunkID) {
		logger.Error("Chunk not found")
		return nil, status.Errorf(codes.NotFound, "chunk not found")
	}

	chunkHeader, err := s.store.GetChunkHeader(req.ChunkID)
	if err != nil {
		logger.Error("Failed to get chunk info", slog.String("error", err.Error()))
		return nil, status.Errorf(codes.Internal, "failed to get chunk info: %v", err)
	}

	sessionID := uuid.NewString()
	if err := s.createStreamingSession(sessionID, chunkHeader, false, logger); err != nil {
		logger.Error("Failed to create streaming session", slog.String("error", err.Error()))
		return nil, status.Errorf(codes.AlreadyExists, "failed to create streaming session: %v", err)
	}

	return common.DownloadReady{
		NodeReady: common.NodeReady{
			Accept:    true,
			Message:   "Ready to download chunk data",
			SessionID: sessionID,
		},
		ChunkHeader: chunkHeader,
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
func (s *DataNodeServer) UploadChunkStream(stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) error {
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
			session.Status = SessionFailed
			return status.Errorf(codes.Internal, "failed to receive chunk data: %v", err)
		}

		// Convert to internal struct
		chunk := common.ChunkDataStreamFromProto(chunkpb)

		// First chunk establishes session
		if session == nil {
			var exists bool
			session, exists = s.getStreamingSession(chunk.SessionID)
			if !exists {
				return status.Errorf(codes.NotFound, "invalid session")
			}
			defer func() {
				session.logger.Info("Deleting session", slog.String("session_id", session.SessionID))
				s.sessionManager.Delete(session.SessionID)
			}()

			// Create buffer with pre-defined capacity for performance
			buf := make([]byte, 0, session.ChunkHeader.Size)
			buffer = bytes.NewBuffer(buf)
		}

		// Verify data integrity and ordering
		if chunk.Offset != totalReceived {
			session.logger.Error("Data out of order", slog.Int("expected", chunk.Offset), slog.Int("got", totalReceived))
			session.Status = SessionFailed
			return status.Errorf(codes.Internal, "data out of order")
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
			session.Status = SessionFailed
			return status.Errorf(codes.Internal, "failed to send acknowledgment: %v", err)
		}

		// Handle final chunk
		if chunk.IsFinal {
			// Verify checksum immediately
			computedHash := session.RunningChecksum.Sum(nil)
			expectedHash, _ := hex.DecodeString(session.Checksum)

			if !bytes.Equal(computedHash, expectedHash) {
				session.logger.Error("Checksum mismatch", slog.String("expected", session.Checksum), slog.String("computed", hex.EncodeToString(computedHash)))
				session.Status = SessionFailed
				return status.Errorf(codes.Internal, "checksum mismatch")
			}

			// Store the chunk immediately
			err := s.store.Store(session.ChunkHeader, buffer.Bytes())
			if err != nil {
				session.logger.Error("Failed to store chunk", slog.String("error", err.Error()))
				session.Status = SessionFailed
				return status.Errorf(codes.Internal, "failed to store chunk: %v", err)
			}

			// Send final ack with verification result
			finalAck := common.ChunkDataAck{
				SessionID:     chunk.SessionID,
				Success:       true,
				Message:       "Chunk stored successfully",
				BytesReceived: totalReceived,
			}
			if err := stream.Send(finalAck.ToProto()); err != nil {
				session.logger.Error("Failed to send final acknowledgment", slog.String("error", err.Error()))
				session.Status = SessionFailed
				return status.Errorf(codes.Internal, "failed to send final acknowledgment: %v", err)
			}
		}
	}

	session.logger.Debug("Received chunk data stream successfully")

	// Propagate the chunk to other nodes if the flag is set
	if session.Propagate {
		replicaNodes, err := s.replicate(session.ChunkHeader, buffer.Bytes())
		if err != nil {
			session.logger.Error("Failed to replicate chunk", slog.String("error", err.Error()))
			session.Status = SessionFailed
			return status.Errorf(codes.Internal, "failed to replicate chunk: %v", err)
		}
		session.logger.Info("Chunk replicated successfully", slog.Any("replica_nodes", replicaNodes))

		if err := stream.Send(common.ChunkDataAck{
			SessionID: session.SessionID,
			Success:   true,
			Message:   "Chunk replicated successfully",
			Replicas:  replicaNodes,
		}.ToProto()); err != nil {
			session.logger.Error("Failed to send chunk data ack with replicas", slog.String("error", err.Error()))
			session.Status = SessionFailed
			return status.Errorf(codes.Internal, "failed to send chunk data ack with replicas: %v", err)
		}
	}
	session.Status = SessionCompleted

	return nil
}

func (s *DataNodeServer) DownloadChunkStream(pb *proto.DownloadStreamRequest, stream grpc.ServerStreamingServer[proto.ChunkDataStream]) error {
	req, err := common.DownloadStreamRequestFromProto(pb)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid download stream request: %v", err)
	}
	logger := logging.OperationLogger(s.logger, "download_stream", slog.String("session_id", req.SessionID))

	session, exists := s.getStreamingSession(req.SessionID)
	if !exists {
		logger.Error("Invalid download session")
		return status.Errorf(codes.NotFound, "invalid download session for ID: %s", req.SessionID)
	}
	defer s.sessionManager.Delete(session.SessionID) // Clean up

	// Retrieve the full chunk data
	// TODO: this should be a io.Reader of size configurable ChunkStreamSize
	chunkData, err := s.store.GetChunkData(session.ChunkHeader.ID)
	if err != nil {
		logger.Error("Could not retrieve chunk", slog.String("error", err.Error()))
		return status.Errorf(codes.Internal, "could not retrieve chunk %s: %v", session.ChunkHeader.ID, err)
	}
	reader := bytes.NewReader(chunkData)
	buffer := make([]byte, req.ChunkStreamSize)
	offset := int64(0)

	session.logger.Info("Starting to stream chunk to client")
	for {
		n, err := reader.Read(buffer)
		if err == io.EOF {
			break // We've sent the entire file.
		}
		if err != nil {
			session.logger.Error("Failed to read from chunk source", slog.String("error", err.Error()))
			return status.Errorf(codes.Internal, "failed to read from chunk source: %v", err)
		}

		if err := stream.Send(&proto.ChunkDataStream{
			SessionId: req.SessionID,
			ChunkId:   session.ChunkHeader.ID,
			Data:      buffer[:n],
			Offset:    offset,
			IsFinal:   offset+int64(n) >= int64(len(chunkData)),
		}); err != nil {
			session.logger.Error("Failed to send data frame to client", slog.String("error", err.Error()))
			return err
		}

		offset += int64(n)
	}

	session.logger.Info("Successfully completed streaming chunk to client")
	return nil
}

// Even though the general operation is based off of heartbeat requests from the datanode to the coordiantor
// It might be useful to still have a healthcheck endpoint for the datanode
func (s *DataNodeServer) HealthCheck(ctx context.Context, pb *proto.HealthCheckRequest) (*proto.HealthCheckResponse, error) {
	return nil, nil
}

func (s *DataNodeServer) RegisterWithCoordinator(ctx context.Context) error {
	coordinatorNode, ok := s.NodeManager.GetCoordinatorNode()
	if !ok {
		return fmt.Errorf("no coordinator node found")
	}

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
	s.NodeManager.InitializeNodes(resp.FullNodeList, resp.CurrentVersion)

	logger.Info("Datanode registered with coordinator successfully")
	return nil
}

// Actually replicates the chunk to the given nodes in parallel
func (s *DataNodeServer) replicate(chunkInfo common.ChunkHeader, data []byte) ([]*common.DataNodeInfo, error) {
	logger := logging.OperationLogger(s.logger, "replicate_chunk", slog.String("chunk_id", chunkInfo.ID))

	// Select N_NODES possible nodes to replicate to, excluding the current node
	nodes, err := s.NodeManager.SelectBestNodes(N_NODES, s.Config.Info.ID)
	if err != nil {
		logger.Error("Failed to select nodes", slog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to select nodes: %v", err)
	}

	// Create clients
	var clients []*DataNodeClient
	for _, node := range nodes {
		client, err := NewDataNodeClient(node)
		if err != nil {
			return nil, fmt.Errorf("failed to create client for %s - [%s]  %v", node.ID, node.Endpoint(), err)
		}
		if client == nil {
			return nil, fmt.Errorf("client for %v is nil", node)
		}
		clients = append(clients, client)
	}

	// Replicate to N_REPLICAS nodes
	replicaNodes, err := s.replicationManager.paralellReplicate(clients, chunkInfo, data, N_REPLICAS-1)
	if err != nil {
		logger.Error("Failed to replicate chunk", slog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to replicate chunk: %v", err)
	}
	replicaNodes = append(replicaNodes, &s.Config.Info) // Add self to the list of replica nodes

	return replicaNodes, nil
}
