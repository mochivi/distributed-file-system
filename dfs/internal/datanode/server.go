package datanode

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/mochivi/distributed-file-system/pkg/streaming"
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

	logger := logging.OperationLogger(s.logger, "prepare_chunk_upload", slog.String("chunk_id", req.ChunkHeader.ID))
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
		if !existing.Status.IsValid() {
			s.sessionManager.Delete(existing.SessionID)
		}

		// If is valid session, return early
		return common.NodeReady{
			Accept:    true,
			Message:   "session already active",
			SessionID: existing.SessionID,
		}.ToProto(), nil
	}
	session := s.sessionManager.NewSession(req.ChunkHeader, req.Propagate)
	if err := s.sessionManager.Store(session.SessionID, session); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to store streaming session: %v", err)
	}

	return common.NodeReady{
		Accept:    true,
		Message:   "Ready to receive chunk data",
		SessionID: session.SessionID,
	}.ToProto(), nil
}

// TODO: add a different session for download, as it is not the same as the upload session
func (s *DataNodeServer) PrepareChunkDownload(ctx context.Context, pb *proto.DownloadChunkRequest) (*proto.DownloadReady, error) {
	req := common.DownloadChunkRequestFromProto(pb)

	logger := logging.OperationLogger(s.logger, "prepare_chunk_download", slog.String("chunk_id", req.ChunkID))
	logger.Info("Received PrepareChunkDownload request")

	if !s.store.Exists(req.ChunkID) {
		logger.Error("Chunk not found", slog.String("chunk_id", req.ChunkID))
		return nil, status.Errorf(codes.NotFound, "chunk not found")
	}

	chunkHeader, err := s.store.GetHeader(req.ChunkID)
	if err != nil {
		logger.Error("Failed to get chunk info", slog.String("error", err.Error()))
		return nil, status.Errorf(codes.Internal, "failed to get chunk info: %v", err)
	}

	session := s.sessionManager.NewSession(chunkHeader, false)
	if err := s.sessionManager.Store(session.SessionID, session); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to store streaming session: %v", err)
	}
	logger.Info("Streaming session created", slog.String("session_id", session.SessionID))

	return common.DownloadReady{
		NodeReady: common.NodeReady{
			Accept:    true,
			Message:   "Ready to download chunk data",
			SessionID: session.SessionID,
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

func (s *DataNodeServer) BulkDeleteChunk(ctx context.Context, pb *proto.BulkDeleteChunkRequest) (*proto.BulkDeleteChunkResponse, error) {
	req := common.BulkDeleteChunkRequestFromProto(pb)

	logger := logging.OperationLogger(s.logger, "bulk_delete_chunk", slog.String("chunk_id", strings.Join(req.ChunkIDs, ",")))
	logger.Info("Received BulkDeleteChunk request")

	ctx, cancel := context.WithTimeout(ctx, s.config.BulkDelete.Timeout)
	defer cancel()

	type result struct {
		chunkID string
		success bool
		err     error
	}

	sem := make(chan struct{}, s.config.BulkDelete.MaxConcurrentDeletes)
	results := make(chan result, len(req.ChunkIDs))
	var wg sync.WaitGroup

	// Delete chunks in parallel
	for _, chunkID := range req.ChunkIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()

			// Acquire semaphore
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results <- result{chunkID: id, success: false, err: ctx.Err()}
				return
			}

			if !s.store.Exists(id) {
				results <- result{chunkID: id, success: false, err: fmt.Errorf("chunk not found")}
				return
			}

			if ctx.Err() != nil {
				results <- result{chunkID: id, success: false, err: ctx.Err()}
				return
			}

			if err := s.store.Delete(id); err != nil {
				results <- result{chunkID: id, success: false, err: err}
				return
			}

			results <- result{chunkID: id, success: true, err: nil}
		}(chunkID)
	}

	// Ensure the context is cancelled only after all work is done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var failures []string
	var successCount int

	for result := range results {
		if result.success {
			successCount++
			continue
		}
		logger.Error("Failed to delete chunk", slog.String("chunk_id", result.chunkID), slog.String("error", result.err.Error()))
		failures = append(failures, result.chunkID)
	}

	success := len(failures) == 0
	logger.Info("Bulk delete completed", slog.Int("success_count", successCount), slog.Int("failure_count", len(failures)), slog.Bool("success", success))

	return common.BulkDeleteChunkResponse{
		Success: success,
		Message: "Bulk delete chunk request completed",
		Failed:  failures,
	}.ToProto(), nil
}

// This is the side that is responsible for receiving the chunk data from some peer (client or another node)
// TODO: not taking into account the partialChecksum on each chunk, so it would be a good idea to ask for a retry in case that any stream frame fails.
// TODO: context awareness for cancelling upload request?
func (s *DataNodeServer) UploadChunkStream(stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) error {
	streamProcessor := s.serverStreamerFactory(s.sessionManager, s.config.Streamer)
	logger := logging.OperationLogger(s.logger, "upload_streaming_session")

	session, err := streamProcessor.HandleFirstChunk(stream)
	if err != nil {
		logger.Error("Failed to receive first chunk", slog.String("error", err.Error()))
		return status.Errorf(codes.Internal, "failed to receive first chunk: %v", err)
	}
	logger = logging.ExtendLogger(logger, slog.String("session_id", session.SessionID), slog.String("chunk_id", session.ChunkHeader.ID))
	defer s.sessionManager.Delete(session.SessionID)

	logger.Debug("starting to receive frames")
	chunkData, err := streamProcessor.ReceiveChunks(session, stream)
	if err != nil {
		logger.Error("Failed to receive chunks", slog.String("error", err.Error()))
		session.Fail()
		return status.Errorf(codes.Internal, "failed to receive chunks: %v", err)
	}

	// TODO: implement flushing at session level, not here
	// TODO: for now, storing entire chunk in buffer and writing all at once
	logger.Debug("Storing chunk", slog.String("chunk_id", session.ChunkHeader.ID), slog.Int("size", len(chunkData)))
	if err = s.store.Store(session.ChunkHeader, chunkData); err != nil {
		logger.Error("Failed to store chunk", slog.String("error", err.Error()))
		session.Fail()
		return status.Errorf(codes.Internal, "failed to store chunk: %v", err)
	}
	logger.Debug("Chunk stored successfully", slog.String("chunk_id", session.ChunkHeader.ID))

	if err := streamProcessor.SendFinalAck(session.SessionID, len(chunkData), stream); err != nil {
		logger.Error("Failed to send final ack", slog.String("error", err.Error()))
		return status.Errorf(codes.Internal, "failed to send final ack: %v", err)
	}
	logger.Debug("Final ack sent successfully")

	// If receiving node is primary node, must replicate to other nodes
	if session.Propagate {
		replicaNodes, err := s.replicate(session.ChunkHeader, chunkData)
		if err != nil {
			logger.Error("Failed to replicate chunk", slog.String("error", err.Error()))
			session.Fail()
			return status.Errorf(codes.Internal, "failed to replicate chunk: %v", err)
		}
		logger.Debug("Chunk replicated successfully", slog.Any("replica_nodes", replicaNodes))

		if err := streamProcessor.SendFinalReplicasAck(session, replicaNodes, stream); err != nil {
			logger.Error("Failed to send chunk data ack with replicas", slog.String("error", err.Error()))
			session.Fail()
			return status.Errorf(codes.Internal, "failed to send final acknowledgement with replica information: %v", err)
		}
		logger.Debug("Final replicas ack sent successfully")
	}

	session.Complete()

	return nil
}

func (s *DataNodeServer) DownloadChunkStream(pb *proto.DownloadStreamRequest, stream grpc.ServerStreamingServer[proto.ChunkDataStream]) error {
	req, err := common.DownloadStreamRequestFromProto(pb)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid download stream request: %v", err)
	}
	logger := logging.OperationLogger(s.logger, "download_streaming_session", slog.String("session_id", req.SessionID))

	session, exists := s.sessionManager.GetSession(req.SessionID)
	if !exists {
		logger.Error("Invalid download session", slog.String("received", req.SessionID))
		return status.Errorf(codes.NotFound, "invalid download session for ID: %s", req.SessionID)
	}
	defer s.sessionManager.Delete(session.SessionID) // Clean up

	// TODO: store should return an io.Reader directly
	chunkData, err := s.store.GetData(session.ChunkHeader.ID)
	if err != nil {
		logger.Error("Could not retrieve chunk", slog.String("error", err.Error()))
		return status.Errorf(codes.Internal, "could not retrieve chunk %s: %v", session.ChunkHeader.ID, err)
	}
	reader := bytes.NewReader(chunkData)

	if err := streaming.SendChunkFrames(reader, req.ChunkStreamSize, session.ChunkHeader.ID,
		session.SessionID, len(chunkData), stream); err != nil {
		return handleStreamingError(err, logger)
	}

	return nil
}

// Even though the general operation is based off of heartbeat requests from the datanode to the coordiantor
// It might be useful to still have a healthcheck endpoint for the datanode
func (s *DataNodeServer) HealthCheck(ctx context.Context, pb *proto.HealthCheckRequest) (*proto.HealthCheckResponse, error) {
	return nil, nil
}

// Actually replicates the chunk to the given nodes in parallel
func (s *DataNodeServer) replicate(chunkHeader common.ChunkHeader, data []byte) ([]*common.NodeInfo, error) {
	logger := logging.OperationLogger(s.logger, "replicate_chunk", slog.String("chunk_id", chunkHeader.ID))

	// Select N_NODES possible nodes to replicate to, excluding the current node
	nodes, ok := s.selector.SelectBestNodes(N_NODES, s.info)
	if !ok {
		logger.Error("Not enough nodes to replicate to")
		return nil, fmt.Errorf("not enough nodes to replicate to")
	}

	// Create clients
	clientPool, err := s.clientPoolFactory(nodes)
	if err != nil {
		return nil, fmt.Errorf("failed to create client pool: %v", err)
	}
	defer clientPool.Close()

	// Replicate to N_REPLICAS nodes
	replicaNodes, err := s.replicationManager.Replicate(clientPool, chunkHeader, data, N_REPLICAS-1)
	if err != nil {
		return nil, fmt.Errorf("failed to replicate chunk: %v", err)
	}
	replicaNodes = append(replicaNodes, s.info) // Add self to the list of replica nodes

	return replicaNodes, nil
}

// Error handling logic
// TODO: extract to somewhere else,
func handleStreamingError(err error, logger *slog.Logger) error {
	// Log error
	var streamErr *streaming.StreamingError
	if errors.As(err, &streamErr) {
		// Log with rich context from the structured error
		logger.Error("Streaming operation failed",
			slog.String("operation", streamErr.Op),
			slog.String("session_id", streamErr.SessionID),
			slog.String("chunk_id", streamErr.ChunkID),
			slog.Int64("offset", streamErr.Offset),
			slog.String("error", streamErr.Err.Error()))
	} else {
		// Fallback for unexpected error types
		logger.Error("Unexpected streaming error", slog.String("error", err.Error()))
	}

	// Translate error
	switch {
	case errors.Is(err, streaming.ErrFrameReadFailed):
		return status.Errorf(codes.Internal, "failed to read chunk data")
	case errors.Is(err, streaming.ErrStreamSendFailed):
		return status.Errorf(codes.Unavailable, "failed to send data to client")
	case errors.Is(err, streaming.ErrStreamClosed):
		return status.Errorf(codes.Aborted, "stream was closed unexpectedly")
	case errors.Is(err, streaming.ErrStreamTimeout):
		return status.Errorf(codes.DeadlineExceeded, "stream operation timed out")
	default:
		return status.Errorf(codes.Internal, "internal streaming error")
	}
}
