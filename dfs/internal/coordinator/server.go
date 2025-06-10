package coordinator

import (
	"context"
	"fmt"
	"log"
	"math/rand"

	"github.com/google/uuid"
	"github.com/mochivi/distributed-file-system/internal/common"
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
	req := newUploadRequestFromProto(pb)
	log.Printf("Received UploadRequest: %+v", req)

	// Calculate number of chunks needed
	chunkSize := req.ChunkSize
	if chunkSize == 0 {
		chunkSize = c.config.ChunkSize * 1024 * 1024
	}
	numChunks := (req.Size + chunkSize - 1) / chunkSize

	// Get a list of the best nodes to upload to
	nodes, err := c.nodeManager.SelectBestNodes(numChunks)
	if err != nil {
		return nil, status.Error(codes.NotFound, "no available nodes")
	}

	assignments := make([]ChunkLocation, numChunks)
	for i := 0; i < numChunks; i++ {
		chunkID := common.FormatChunkID(req.Path, i)

		// Randomly select a node from the available nodes
		nodeIndex := rand.Intn(len(nodes))
		node := nodes[nodeIndex]

		// Add chunk location to assignment
		assignments[i] = ChunkLocation{
			ChunkID:  chunkID,
			NodeID:   node.ID,
			Endpoint: fmt.Sprintf("%s:%d", node.IPAddress, node.Port),
		}
	}

	sessionID := uuid.NewString()
	go c.metadataManager.trackUpload(sessionID, req, numChunks) // todo: add response from client to commit metadata

	return UploadResponse{
		ChunkLocations: assignments,
	}.ToProto(), nil
}

// Client request for a file download
func (c *Coordinator) DownloadFile(ctx context.Context, req *proto.DownloadRequest) (*proto.DownloadResponse, error) {
	// Try to retrieve information about the file location
	fileInfo, err := c.metaStore.GetFile(req.Path)
	if err != nil {
		return nil, fmt.Errorf("file not found: %v", err)
	}

	// Build chunk sources
	chunkLocations := make([]ChunkLocation, 0, len(fileInfo.Chunks))

	// Find available nodes to download from for this chunk
	for i, chunk := range fileInfo.Chunks {
		node, ok := c.nodeManager.GetAvailableNodeForChunk(chunk.Replicas)
		if !ok {
			return nil, status.Error(codes.NotFound, "no available nodes")
		}

		chunkLocations[i] = ChunkLocation{
			ChunkID:  chunk.ID,
			NodeID:   node.ID,
			Endpoint: fmt.Sprintf("%s:%d", node.IPAddress, node.Port),
		}
	}

	return DownloadResponse{
		fileInfo:       *fileInfo,
		chunkLocations: chunkLocations,
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

// node -> coordinator requests below

// DataNode ingress mechanism
func (c *Coordinator) RegisterDataNode(ctx context.Context, pb *proto.RegisterDataNodeRequest) (*proto.RegisterDataNodeResponse, error) {
	nodeInfo := common.DataNodeInfoFromProto(pb.NodeInfo)

	c.nodeManager.AddNode(&nodeInfo)
	nodes, version := c.nodeManager.ListNodes()

	return RegisterDataNodeResponse{
		Success:        true,
		Message:        "Node registered successfully",
		FullNodeList:   nodes,
		CurrentVersion: version,
	}.ToProto(), nil
}

// DataNodes periodically communicate their status to the coordinator
func (c *Coordinator) DataNodeHeartbeat(ctx context.Context, pb *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	req := HeartbeatRequestFromProto(pb)

	node, exists := c.nodeManager.GetNode(req.NodeID)
	if !exists {
		return HeartbeatResponse{
			Success: false,
			Message: "node is not registered",
		}.ToProto(), nil
	}

	node.Status = req.Status.Status
	node.LastSeen = req.Status.LastSeen

	updates, currentVersion, err := c.nodeManager.GetUpdatesSince(req.LastSeenVersion)
	if err != nil {
		return HeartbeatResponse{
			Success:            true, // heartbeat was a success, as lastSeen & status were updated, but node requires resync
			Message:            "version too old",
			FromVersion:        req.LastSeenVersion,
			ToVersion:          currentVersion,
			RequiresFullResync: true,
		}.ToProto(), nil
	}

	return HeartbeatResponse{
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
	nodes, version := c.nodeManager.ListNodes()
	return ListNodesResponse{
		Nodes:          nodes,
		CurrentVersion: version,
	}.ToProto(), nil
}
