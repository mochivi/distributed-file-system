package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/logging"
)

func (s *service) uploadFile(ctx context.Context, req common.UploadRequest) (common.UploadResponse, error) {
	logger := logging.FromContext(ctx)

	// Calculate number of chunks needed
	numChunks := (req.Size + req.ChunkSize - 1) / req.ChunkSize
	logger = logging.ExtendLogger(logger, slog.Int(common.LogNumChunks, numChunks))

	// Select some nodes for the client to upload to
	nodes, err := s.selector.SelectBestNodes(numChunks, &common.NodeInfo{ID: "coordinator"}) // TODO: this has to be updated
	if err != nil {
		return common.UploadResponse{}, err
	}

	chunkPrefix := chunk.HashFilepath(req.Path)
	chunkIDs := chunk.FormatChunkIDs(chunkPrefix, numChunks)

	// Create metadata commit session for the upload
	sessionID := common.NewMetadataSessionID()
	go s.metadataManager.trackUpload(sessionID, req, chunkIDs)

	logger.Debug("Tracking file upload", slog.String(common.LogMetadataSessionID, sessionID.String()))
	return common.UploadResponse{
		ChunkIDs:  chunkIDs,
		Nodes:     nodes,
		SessionID: sessionID,
	}, nil
}

func (s *service) downloadFile(ctx context.Context, req common.DownloadRequest) (common.DownloadResponse, error) {
	logger := logging.FromContext(ctx)

	// Try to retrieve information about the file location
	fileInfo, err := s.metaStore.GetFile(ctx, req.Path)
	if err != nil {
		return common.DownloadResponse{}, fmt.Errorf("failed to get file info: %w", err)
	}

	// Build chunk sources
	chunkLocations := make([]common.ChunkLocation, len(fileInfo.Chunks))

	// Find available nodes to download from for this chunk
	for i, chunk := range fileInfo.Chunks {
		nodes, err := s.clusterStateHistoryManager.GetAvailableNodesForChunk(chunk.Replicas)
		if err != nil {
			logger.Error("Failed to get available node for chunk", slog.String(common.LogChunkID, chunk.Header.ID), slog.String(common.LogError, err.Error()))
			return common.DownloadResponse{}, fmt.Errorf("failed to get available nodes for chunk %s: %w", chunk.Header.ID, err)
		}

		chunkLocations[i] = common.ChunkLocation{
			ChunkID: chunk.Header.ID,
			Nodes:   nodes,
		}
	}

	logger.Info("Replying to client with chunk locations", slog.Int(common.LogNumChunks, len(chunkLocations)))
	return common.DownloadResponse{
		FileInfo:       *fileInfo,
		ChunkLocations: chunkLocations,
		SessionID:      uuid.NewString(), // TODO: this should be a session id that is used to track the download, not used for now
	}, nil
}

func (s *service) listFiles(ctx context.Context, req common.ListRequest) (common.ListResponse, error) {
	logger := logging.FromContext(ctx)

	files, err := s.metaStore.ListFiles(ctx, req.Directory, true)
	if err != nil {
		return common.ListResponse{}, fmt.Errorf("failed to list files: %w", err)
	}

	logger.Info("Replying to client with list of files", slog.Int(common.LogNumFiles, len(files)))
	return common.ListResponse{Files: files}, nil
}

func (s *service) deleteFile(ctx context.Context, req common.DeleteRequest) (common.DeleteResponse, error) {
	logger := logging.FromContext(ctx)

	if err := s.metaStore.DeleteFile(ctx, req.Path); err != nil {
		return common.DeleteResponse{}, fmt.Errorf("failed to delete file: %w", err)
	}

	logger.Info("File deleted")
	return common.DeleteResponse{Success: true}, nil
}

func (s *service) confirmUpload(ctx context.Context, req common.ConfirmUploadRequest) (common.ConfirmUploadResponse, error) {
	logger := logging.FromContext(ctx)

	if err := s.metadataManager.commit(ctx, req.SessionID, req.ChunkInfos, s.metaStore); err != nil {
		return common.ConfirmUploadResponse{}, fmt.Errorf("failed to commit metadata: %w", err)
	}

	logger.Info("Metadata committed")
	return common.ConfirmUploadResponse{Success: true}, nil
}

func (s *service) registerDataNode(ctx context.Context, req common.RegisterDataNodeRequest) (common.RegisterDataNodeResponse, error) {
	logger := logging.FromContext(ctx)

	nodeInfo := req.NodeInfo
	nodeInfo.LastSeen = time.Now()

	s.clusterStateHistoryManager.AddNode(nodeInfo)
	nodes, version := s.clusterStateHistoryManager.ListNodes()

	logger.Info("Registered new datanode", slog.Int(common.LogNumNodes, len(nodes)), slog.Int(common.LogClusterVersion, int(version)))
	return common.RegisterDataNodeResponse{
		Success:        true,
		Message:        "Node registered successfully",
		FullNodeList:   nodes,
		CurrentVersion: version,
	}, nil
}

func (s *service) heartbeat(ctx context.Context, req common.HeartbeatRequest) (common.HeartbeatResponse, error) {
	logger := logging.FromContext(ctx)

	node, err := s.clusterStateHistoryManager.GetNode(req.NodeID)
	if err != nil {
		return common.HeartbeatResponse{
			Success: false,
			Message: "node is not registered",
		}, nil
	}

	node.Status = req.Status.Status
	node.LastSeen = req.Status.LastSeen

	updates, currentVersion, err := s.clusterStateHistoryManager.GetUpdatesSince(req.LastSeenVersion)
	if err != nil {
		return common.HeartbeatResponse{
			Success:            true,
			Message:            "version too old",
			RequiresFullResync: true,
			FromVersion:        req.LastSeenVersion,
			ToVersion:          currentVersion,
		}, nil
	}

	logger.Debug("Replying to data node with updates", slog.Int(common.LogNumUpdates, len(updates)))
	return common.HeartbeatResponse{
		Success:            true,
		Message:            "ok",
		Updates:            updates,
		FromVersion:        req.LastSeenVersion,
		ToVersion:          currentVersion,
		RequiresFullResync: false,
	}, nil
}

func (s *service) listNodes(ctx context.Context, req common.ListNodesRequest) (common.ListNodesResponse, error) {
	logger := logging.FromContext(ctx)

	nodes, version := s.clusterStateHistoryManager.ListNodes()

	logger.Info("Replying to client with list of nodes", slog.Int(common.LogNumNodes, len(nodes)), slog.Int(common.LogClusterVersion, int(version)))
	return common.ListNodesResponse{
		Nodes:          nodes,
		CurrentVersion: version,
	}, nil
}
