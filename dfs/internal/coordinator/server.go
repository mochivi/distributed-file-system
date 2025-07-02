package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
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
	c.logger.Info("Received UploadRequest")

	// Calculate number of chunks needed
	chunkSize := req.ChunkSize
	if chunkSize == 0 {
		chunkSize = c.config.ChunkSize * 1024 * 1024
	}
	numChunks := (req.Size + chunkSize - 1) / chunkSize
	c.logger.Debug(fmt.Sprintf("File will be split into %d chunks of %dMB", numChunks, chunkSize/(1024*1024)))

	// Get a list of the best nodes to upload to
	nodes, ok := c.selector.SelectBestNodes(numChunks)
	if !ok {
		c.logger.Error("Not enough nodes to upload to", slog.Int("num_nodes", len(nodes)), slog.Int("num_chunks", numChunks))
		return nil, status.Error(codes.NotFound, "no available nodes")
	}
	c.logger.Debug("Selected nodes for chunk distribution", slog.Int("num_nodes", len(nodes)))

	assignments := make([]common.ChunkLocation, numChunks)
	for i := 0; i < numChunks; i++ {
		chunkID := chunk.FormatChunkID(req.Path, i)

		// Randomly select a node from the available nodes
		selectedNodes := make([]*common.DataNodeInfo, 0, 3)
		count := 0
		for _, node := range nodes {
			selectedNodes = append(selectedNodes, node)
			count++
			if count >= 3 {
				break
			}
		}

		// Add chunk location to assignment
		assignments[i] = common.ChunkLocation{
			ChunkID: chunkID,
			Nodes:   selectedNodes,
		}
	}

	// Create metadata commit session for the upload
	sessionID := uuid.NewString()
	go c.metadataManager.trackUpload(sessionID, req, numChunks)
	c.logger.Debug("Created upload session", slog.String("session_id", sessionID), slog.String("file_path", req.Path))

	return common.UploadResponse{
		ChunkLocations: assignments,
		SessionID:      sessionID,
	}.ToProto(), nil
}

// Client request for a file download
func (c *Coordinator) DownloadFile(ctx context.Context, req *proto.DownloadRequest) (*proto.DownloadResponse, error) {
	// Try to retrieve information about the file location
	fileInfo, err := c.metaStore.GetFile(req.Path)
	if err != nil {
		c.logger.Error("Failed to get file info", slog.String("error", err.Error()))
		return nil, status.Error(codes.NotFound, "file not found")
	}

	// Build chunk sources
	chunkLocations := make([]common.ChunkLocation, len(fileInfo.Chunks))

	// Find available nodes to download from for this chunk
	for i, chunk := range fileInfo.Chunks {
		nodes, ok := c.clusterStateHistoryManager.GetAvailableNodesForChunk(chunk.Replicas)
		if !ok {
			c.logger.Error("Failed to get available node for chunk", slog.String("chunk_id", chunk.Header.ID), slog.String("file_path", req.Path))
			return nil, status.Error(codes.NotFound, "no available nodes")
		}

		chunkLocations[i] = common.ChunkLocation{
			ChunkID: chunk.Header.ID,
			Nodes:   nodes,
		}
	}

	c.logger.Debug("Replying to client with chunk locations", slog.Int("num_chunks", len(chunkLocations)))
	return common.DownloadResponse{
		FileInfo:       *fileInfo,
		ChunkLocations: chunkLocations,
		SessionID:      uuid.NewString(), // TODO: this should be a session id that is used to track the download
	}.ToProto(), nil
}

// // Client request to list files from some directory
// func (c *Coordinator) ListFiles(ctx context.Context, pb *proto.ListRequest) (*proto.ListResponse, error) {
// 	return nil, nil
// }

// // Client request to delete a file
// func (c *Coordinator) DeleteFile(ctx context.Context, pb *proto.DeleteRequest) (*proto.DeleteResponse, error) {
// 	return nil, nil
// }

func (c *Coordinator) ConfirmUpload(ctx context.Context, pb *proto.ConfirmUploadRequest) (*proto.ConfirmUploadResponse, error) {
	req := common.ConfirmUploadRequestFromProto(pb)
	c.logger.Debug("Received ConfirmUploadRequest from client", slog.String("session_id", req.SessionID))

	if err := c.metadataManager.commit(req.SessionID, req.ChunkInfos, c.metaStore); err != nil {
		c.logger.Error("Failed to commit metadata", slog.String("error", err.Error()))
		return common.ConfirmUploadResponse{
			Success: false,
			Message: "Failed to commit metadata",
		}.ToProto(), nil
	}

	return common.ConfirmUploadResponse{
		Success: true,
	}.ToProto(), nil
}

// node -> coordinator requests below

// DataNode ingress mechanism
func (c *Coordinator) RegisterDataNode(ctx context.Context, pb *proto.RegisterDataNodeRequest) (*proto.RegisterDataNodeResponse, error) {
	nodeInfo := common.DataNodeInfoFromProto(pb.NodeInfo)
	c.logger.Debug("Received RegisterDataNodeRequest from data node", slog.String("node_id", nodeInfo.ID))
	nodeInfo.LastSeen = time.Now()

	c.clusterStateHistoryManager.AddNode(&nodeInfo)
	nodes, version := c.clusterStateHistoryManager.ListNodes()

	c.logger.Debug("Registered datanode, replying with current node list and version", slog.Int("num_nodes", len(nodes)), slog.Int("version", int(version)))
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
	c.logger.Debug("Received HeartbeatRequest from data node", slog.String("node_id", req.NodeID))

	node, exists := c.clusterStateHistoryManager.GetNode(req.NodeID)
	if !exists {
		c.logger.Error("Data node not found", slog.String("node_id", req.NodeID))
		return common.HeartbeatResponse{
			Success: false,
			Message: "node is not registered",
		}.ToProto(), nil
	}

	node.Status = req.Status.Status
	node.LastSeen = req.Status.LastSeen

	updates, currentVersion, err := c.clusterStateHistoryManager.GetUpdatesSince(req.LastSeenVersion)
	if err != nil {
		c.logger.Error("Failed to get updates since provided", slog.String("error", err.Error()))
		return common.HeartbeatResponse{
			Success:            true, // heartbeat was a success, as lastSeen & status were updated, but node requires resync
			Message:            "version too old",
			FromVersion:        req.LastSeenVersion,
			ToVersion:          currentVersion,
			RequiresFullResync: true,
		}.ToProto(), nil
	}

	c.logger.Debug("Replying to data node with updates", slog.Int("num_updates", len(updates)))
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
