package datanode

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage/encoding"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/mochivi/distributed-file-system/pkg/streaming"
)

func (s *service) prepareChunkUpload(ctx context.Context, req common.UploadChunkRequest) (common.NodeReady, error) {
	logger := logging.FromContext(ctx)

	if req.ChunkHeader.ID == "" || req.ChunkHeader.Size <= 0 {
		return common.NodeReady{Accept: false, Message: "Invalid chunk metadata"}, nil
	}
	if !s.hasCapacity(req.ChunkHeader.Size) {
		return common.NodeReady{Accept: false, Message: "Insufficient storage capacity"}, nil
	}

	if existing, ok := s.sessionManager.LoadByChunk(req.ChunkHeader.ID); ok {
		if !existing.Status.IsValid() {
			s.sessionManager.Delete(existing.SessionID)
			return common.NodeReady{Accept: false, Message: "Invalid session"}, nil
		}
		// If is valid session, return early
		return common.NodeReady{Accept: true, Message: "Session already active", SessionID: existing.SessionID}, nil
	}

	session := s.sessionManager.NewSession(ctx, req.ChunkHeader, req.Propagate)
	if err := s.sessionManager.Store(session.SessionID, session); err != nil {
		return common.NodeReady{}, fmt.Errorf("failed to store streaming session: %w", err)
	}

	logger.Info("Prepared chunk upload", slog.String(common.LogStreamingSessionID, session.SessionID))
	return common.NodeReady{Accept: true, Message: "Ready to receive chunk data", SessionID: session.SessionID}, nil
}

func (s *service) prepareChunkDownload(ctx context.Context, req common.DownloadChunkRequest) (common.DownloadReady, error) {
	logger := logging.FromContext(ctx)

	if err := s.store.Exists(ctx, req.ChunkID); err != nil {
		return common.DownloadReady{}, fmt.Errorf("failed to check if chunk exists: %w", err)
	}

	chunkHeader, err := s.store.GetHeader(ctx, req.ChunkID)
	if err != nil {
		return common.DownloadReady{}, fmt.Errorf("failed to get chunk header: %w", err)
	}

	session := s.sessionManager.NewSession(ctx, chunkHeader, false)
	if err := s.sessionManager.Store(session.SessionID, session); err != nil {
		return common.DownloadReady{}, fmt.Errorf("failed to store streaming session: %w", err)
	}

	logger.Info("Prepared chunk download", slog.String(common.LogStreamingSessionID, session.SessionID))
	return common.DownloadReady{
		NodeReady:   common.NodeReady{Accept: true, Message: "Ready to download chunk data", SessionID: session.SessionID},
		ChunkHeader: chunkHeader,
	}, nil
}

func (s *service) deleteChunk(ctx context.Context, req common.DeleteChunkRequest) (common.DeleteChunkResponse, error) {
	logger := logging.FromContext(ctx)

	if err := s.store.Exists(ctx, req.ChunkID); err != nil {
		return common.DeleteChunkResponse{}, fmt.Errorf("failed to check if chunk exists: %w", err)
	}

	logger.Debug("Deleting chunk", slog.String(common.LogChunkID, req.ChunkID))
	if err := s.store.Delete(ctx, req.ChunkID); err != nil {
		return common.DeleteChunkResponse{}, fmt.Errorf("failed to delete chunk: %w", err)
	}

	logger.Info("Deleted chunk")
	return common.DeleteChunkResponse{Success: true, Message: "Chunk deleted"}, nil
}

func (s *service) bulkDeleteChunk(ctx context.Context, req common.BulkDeleteChunkRequest) (common.BulkDeleteChunkResponse, error) {
	logger := logging.FromContext(ctx)

	type result struct {
		chunkID string
		success bool
		err     error
	}

	ctx, cancel := context.WithTimeout(ctx, s.config.BulkDelete.Timeout)
	defer cancel()

	sem := make(chan struct{}, s.config.BulkDelete.MaxConcurrentDeletes)
	results := make(chan result, len(req.ChunkIDs))
	var wg sync.WaitGroup

	// Delete chunks in parallel
	for _, chunkID := range req.ChunkIDs {
		wg.Add(1)
		id := chunkID
		go func() {
			defer wg.Done()

			// Acquire semaphore
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results <- result{chunkID: id, success: false, err: ctx.Err()}
				return
			}

			if err := s.store.Exists(ctx, id); err != nil {
				results <- result{chunkID: id, success: false, err: fmt.Errorf("failed to check if chunk exists: %w", err)}
				return
			}
			if ctx.Err() != nil {
				results <- result{chunkID: id, success: false, err: ctx.Err()}
				return
			}
			if err := s.store.Delete(ctx, id); err != nil {
				results <- result{chunkID: id, success: false, err: err}
				return
			}
			results <- result{chunkID: id, success: true, err: nil}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var failures []string
	for res := range results {
		if !res.success {
			failures = append(failures, res.chunkID)
		}
	}

	success := len(failures) == 0

	logger.Info("Bulk delete chunk request completed", slog.Int(common.LogNumChunks, len(req.ChunkIDs)),
		slog.Int(common.LogNumFailed, len(failures)), slog.Bool(common.LogSuccess, success))
	return common.BulkDeleteChunkResponse{Success: success, Message: "Bulk delete chunk request completed", Failed: failures}, nil
}

func (s *service) uploadChunkStream(stream proto.DataNodeService_UploadChunkStreamServer) error {
	streamProcessor := s.serverStreamerFactory(s.sessionManager, s.config.Streamer)

	session, err := streamProcessor.HandleFirstChunk(stream)
	if err != nil {
		return fmt.Errorf("failed to receive first chunk frame: %w", err)
	}
	defer s.sessionManager.Delete(session.SessionID)
	logger := logging.FromContext(session.Context())
	logger.Debug("Received first chunk frame")

	// Receive frames
	logger.Debug("Receiving chunk frames")
	chunkData, err := streamProcessor.ReceiveChunks(session, stream)
	if err != nil {
		session.Fail()
		return fmt.Errorf("failed to receive chunk frames: %w", err)
	}
	logger.Debug("Received all chunk frames")

	// Store chunk
	if err = s.store.Store(session.Context(), session.ChunkHeader, chunkData); err != nil {
		session.Fail()
		return fmt.Errorf("failed to store chunk: %w", err)
	}

	if err := streamProcessor.SendFinalAck(session.SessionID, len(chunkData), stream); err != nil {
		return fmt.Errorf("failed to send final ack: %w", err)
	}

	// Replicate if needed
	if session.Propagate {
		replicaNodes, err := s.replicate(session.Context(), session.ChunkHeader, chunkData)
		if err != nil {
			session.Fail()
			return fmt.Errorf("failed to replicate chunk: %w", err)
		}
		if err := streamProcessor.SendFinalReplicasAck(session, replicaNodes, stream); err != nil {
			session.Fail()
			return fmt.Errorf("failed to send final acknowledgement with replica information: %w", err)
		}
	}

	session.Complete()
	return nil
}

func (s *service) downloadChunkStream(req common.DownloadStreamRequest, stream proto.DataNodeService_DownloadChunkStreamServer) error {
	session, exists := s.sessionManager.GetSession(req.SessionID)
	if !exists {
		return fmt.Errorf("invalid download session for ID: %s", req.SessionID)
	}
	defer s.sessionManager.Delete(session.SessionID)
	logger := logging.FromContext(session.Context())
	logger.Debug("Downloading chunk")

	// TODO: store should return an io.Reader directly
	logger.Debug("Getting chunk data from store")
	chunkData, err := s.store.GetData(session.Context(), session.ChunkHeader.ID)
	if err != nil {
		if errors.Is(err, encoding.ErrDeserializeHeader) {
			return fmt.Errorf("failed to deserialize header: %w", err)
		}
		return fmt.Errorf("failed to get chunk data: %w", err)
	}

	logger.Debug("Sending chunk frames to peer")
	reader := bytes.NewReader(chunkData)
	if err := streaming.SendChunkFrames(reader, req.ChunkStreamSize, session.ChunkHeader.ID, session.SessionID, len(chunkData), stream); err != nil {
		return err
	}
	return nil
}

func (s *service) healthCheck(ctx context.Context, req common.HealthCheckRequest) (common.HealthCheckResponse, error) {
	return common.HealthCheckResponse{}, nil
}

// replicate actually replicates the chunk to the given nodes in parallel
func (s *service) replicate(ctx context.Context, chunkHeader common.ChunkHeader, data []byte) ([]*common.NodeInfo, error) {
	nodes, err := s.selector.SelectBestNodes(N_NODES, s.info)
	if err != nil {
		return nil, err
	}

	clientPool, err := s.clientPoolFactory(nodes)
	if err != nil {
		return nil, fmt.Errorf("failed to create client pool: %v", err)
	}
	defer clientPool.Close()

	replicaNodes, err := s.replicationManager.Replicate(clientPool, chunkHeader, data, N_REPLICAS-1)
	if err != nil {
		return nil, fmt.Errorf("failed to replicate chunk: %v", err)
	}
	replicaNodes = append(replicaNodes, s.info)
	return replicaNodes, nil
}
