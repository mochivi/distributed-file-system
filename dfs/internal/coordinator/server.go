package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/mochivi/distributed-file-system/internal/apperr"
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/internal/storage/metadata"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Upload
// 1. Validate if requested Path is available for upload.
// 2. Idenfity candidate data nodes for each data chunk.
// 3. Store metadata about the file. Will set metadata with a validation wrappper, expects confirmation from client
// 3. Reply with where the client should upload each chunk (primary + replicas).
func (c *Coordinator) UploadFile(ctx context.Context, pb *proto.UploadRequest) (*proto.UploadResponse, error) {
	// transform into internal representation
	req := common.UploadRequestFromProto(pb)

	_, logger := logging.FromContextWithOperation(ctx, common.OpUpload,
		slog.String(common.LogFilePath, req.Path),
		slog.Int(common.LogFileSize, req.Size),
		slog.Int(common.LogChunkSize, req.ChunkSize))

	// Calculate number of chunks needed
	chunkSize := req.ChunkSize
	if chunkSize == 0 {
		chunkSize = c.config.ChunkSize
	}
	numChunks := (req.Size + chunkSize - 1) / chunkSize
	logger = logging.ExtendLogger(logger, slog.Int(common.LogNumChunks, numChunks))

	// Select some nodes for the client to upload to
	nodes, err := c.selector.SelectBestNodes(numChunks, &common.NodeInfo{ID: "coordinator"}) // TODO: this has to be updated
	if err != nil {
		logger.Error("No available nodes to upload", slog.String(common.LogError, err.Error()))
		return nil, status.Error(codes.NotFound, err.Error())
	}

	chunkPrefix := chunk.HashFilepath(req.Path)
	chunkIDs := make([]string, numChunks)
	for i := range numChunks {
		chunkID := fmt.Sprintf("%s_%d", chunkPrefix, i)
		chunkIDs[i] = chunkID
	}

	// Create metadata commit session for the upload
	sessionID := uuid.NewString()
	go c.metadataManager.trackUpload(sessionID, req, numChunks)
	logger.Info("Tracking upload", slog.String(common.LogMetadataSessionID, sessionID))

	return common.UploadResponse{
		ChunkIDs:  chunkIDs,
		Nodes:     nodes,
		SessionID: sessionID,
	}.ToProto(), nil
}

// Client request for a file download
func (c *Coordinator) DownloadFile(ctx context.Context, pb *proto.DownloadRequest) (*proto.DownloadResponse, error) {
	req := common.DownloadRequestFromProto(pb)
	_, logger := logging.FromContextWithOperation(ctx, common.OpDownload,
		slog.String(common.LogFilePath, req.Path))

	// Try to retrieve information about the file location
	fileInfo, err := c.metaStore.GetFile(ctx, req.Path)
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			logger.Error("Failed to get file info", slog.String(common.LogError, err.Error()))
			return nil, apperr.Wrap(codes.NotFound, "file not found", err)
		}
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	// Build chunk sources
	chunkLocations := make([]common.ChunkLocation, len(fileInfo.Chunks))

	// Find available nodes to download from for this chunk
	for i, chunk := range fileInfo.Chunks {
		nodes, ok := c.clusterStateHistoryManager.GetAvailableNodesForChunk(chunk.Replicas)
		if !ok {
			logger.Error("Failed to get available node for chunk", slog.String(common.LogChunkID, chunk.Header.ID))
			return nil, status.Error(codes.NotFound, "no available nodes")
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
	}.ToProto(), nil
}

// Client request to list files from some directory
func (c *Coordinator) ListFiles(ctx context.Context, pb *proto.ListRequest) (*proto.ListResponse, error) {
	req := common.ListRequestFromProto(pb)
	_, logger := logging.FromContextWithOperation(ctx, common.OpList,
		slog.String(common.LogDirectory, req.Directory))

	files, err := c.metaStore.ListFiles(ctx, req.Directory, true) // hardcoded to always recursive for now
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	logger.Info("Replying to client with list of files", slog.Int(common.LogNumFiles, len(files)))
	return common.ListResponse{Files: files}.ToProto(), nil
}

// Client request to delete a file
// 1. Client sends delete request to coordinator
// 2. Coordinator deletes the file from metadata store
// 3. Coordinator replies with success or error
// 4. Background gc process deletes the files from storage
func (c *Coordinator) DeleteFile(ctx context.Context, pb *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	req := common.DeleteRequestFromProto(pb)
	_, logger := logging.FromContextWithOperation(ctx, common.OpDelete,
		slog.String(common.LogFilePath, req.Path))

	if err := c.metaStore.DeleteFile(ctx, req.Path); err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			return nil, apperr.Wrap(codes.NotFound, "file not found", err)
		}
		return nil, fmt.Errorf("failed to delete file: %w", err)
	}

	logger.Info("Deleted file")
	return common.DeleteResponse{Success: true}.ToProto(), nil
}

func (c *Coordinator) ConfirmUpload(ctx context.Context, pb *proto.ConfirmUploadRequest) (*proto.ConfirmUploadResponse, error) {
	req := common.ConfirmUploadRequestFromProto(pb)
	_, logger := logging.FromContextWithOperation(ctx, common.OpCommit,
		slog.String(common.LogMetadataSessionID, req.SessionID))

	if err := c.metadataManager.commit(ctx, req.SessionID, req.ChunkInfos, c.metaStore); err != nil {
		logger.Error("Failed to commit metadata", slog.String(common.LogError, err.Error()))
		return common.ConfirmUploadResponse{
			Success: false,
			Message: "Failed to commit metadata",
		}.ToProto(), nil
	}

	logger.Info("Commited metadata")
	return common.ConfirmUploadResponse{Success: true}.ToProto(), nil
}

// node -> coordinator requests below

// DataNode ingress mechanism
func (c *Coordinator) RegisterDataNode(ctx context.Context, pb *proto.RegisterDataNodeRequest) (*proto.RegisterDataNodeResponse, error) {
	nodeInfo := common.NodeInfoFromProto(pb.NodeInfo)
	_, logger := logging.FromContextWithOperation(ctx, common.OpRegister,
		slog.String(common.LogNodeID, nodeInfo.ID))

	nodeInfo.LastSeen = time.Now()

	c.clusterStateHistoryManager.AddNode(nodeInfo)
	nodes, version := c.clusterStateHistoryManager.ListNodes()

	logger.Info("Registered new datanode", slog.Int(common.LogNumNodes, len(nodes)), slog.Int(common.LogClusterVersion, int(version)))
	return common.RegisterDataNodeResponse{
		Success:        true,
		Message:        "Node registered successfully",
		FullNodeList:   nodes,
		CurrentVersion: version,
	}.ToProto(), nil
}

// DataNodes periodically communicate their status to the coordinator
func (c *Coordinator) DataNodeHeartbeat(ctx context.Context, pb *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	req := common.HeartbeatRequestFromProto(pb)

	_, logger := logging.FromContextWithOperation(ctx, common.OpHeartbeat,
		slog.String(common.LogNodeID, req.NodeID))

	node, err := c.clusterStateHistoryManager.GetNode(req.NodeID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			logger.Error("Data node not found", slog.String(common.LogError, err.Error()))
			return common.HeartbeatResponse{
				Success: false,
				Message: "node is not registered",
			}.ToProto(), nil
		}
		return nil, fmt.Errorf("failed to get data node: %w", err)
	}

	node.Status = req.Status.Status
	node.LastSeen = req.Status.LastSeen

	updates, currentVersion, err := c.clusterStateHistoryManager.GetUpdatesSince(req.LastSeenVersion)
	if err != nil {
		logger.Error("Failed to get updates since provided", slog.String(common.LogError, err.Error()))
		return common.HeartbeatResponse{
			Success:            true, // heartbeat was a success, as lastSeen & status were updated, but node requires resync
			Message:            "version too old",
			FromVersion:        req.LastSeenVersion,
			ToVersion:          currentVersion,
			RequiresFullResync: true,
		}.ToProto(), nil
	}

	logger.Info("Replying to data node with updates")
	return common.HeartbeatResponse{
		Success:            true,
		Message:            "ok",
		Updates:            updates,
		FromVersion:        req.LastSeenVersion,
		ToVersion:          currentVersion,
		RequiresFullResync: false,
	}.ToProto(), nil

}

// ListNodes returns a list of all registered data nodes
func (c *Coordinator) ListNodes(ctx context.Context, req *proto.ListNodesRequest) (*proto.ListNodesResponse, error) {
	nodes, version := c.clusterStateHistoryManager.ListNodes()
	return common.ListNodesResponse{
		Nodes:          nodes,
		CurrentVersion: version,
	}.ToProto(), nil
}

// Todo: this will be replaced by getting chunks for node from the distributed metadata store, not by asking the coordinator
func (c *Coordinator) GetChunksForNode(ctx context.Context, pb *proto.GetChunksForNodeRequest) (*proto.GetChunksForNodeResponse, error) {
	return nil, nil
}
