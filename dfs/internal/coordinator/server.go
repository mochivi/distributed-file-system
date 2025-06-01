package coordinator

import (
	"context"
	"fmt"

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

	// Calculate number of chunks needed
	chunkSize := req.ChunkSize
	if chunkSize == 0 {
		chunkSize = CHUNK_SIZE * 1024 * 1024
	}
	numChunks := (req.Size + chunkSize - 1) / chunkSize

	// type ChunkInfo struct {
	// 	ID       string
	// 	Size     int64
	// 	Replicas []string // DataNode IDs storing this chunk
	// 	Checksum string
	// }

	// Select nodes for each chunk, locking the nodes until complete
	assignments := make([]ChunkLocation, 0, numChunks)
	c.nodesMutex.RLock()
	for i := range numChunks {
		chunkID := common.FormatChunkID(req.Path, i)

		// Select nodes for this chunk (primary + replicas)
		node, ok := c.selectNodeForChunk()
		if !ok {
			return nil, status.Error(codes.NotFound, "no available nodes")
		}

		// Add chunk location to assignment
		assignments[i] = ChunkLocation{
			ChunkID:  chunkID,
			NodeID:   node.ID,
			Endpoint: fmt.Sprintf("%s:%d", node.IPAddress, node.Port),
		}
	}
	c.nodesMutex.RUnlock()

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
	c.nodesMutex.RLock()
	for i, chunk := range fileInfo.Chunks {
		node, ok := c.getAvailableNodeForChunk(chunk.Replicas)
		if !ok {
			return nil, status.Error(codes.NotFound, "no available nodes")
		}

		chunkLocations[i] = ChunkLocation{
			ChunkID:  chunk.ID,
			NodeID:   node.ID,
			Endpoint: fmt.Sprintf("%s:%d", node.IPAddress, node.Port),
		}
	}
	c.nodesMutex.RUnlock()

	return DownloadResponse{
		fileInfo:       *fileInfo,
		chunkLocations: chunkLocations,
	}.ToProto(), nil
}

// DataNode ingress mechanism
func (c *Coordinator) RegisterDataNode(ctx context.Context, req *proto.RegisterDataNodeRequest) (*proto.RegisterDataNodeResponse, error) {
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()

	// Convert request node info into internal representation
	nodeInfo := common.NewDataNodeInfoFromProto(req.NodeInfo)
	c.dataNodes[nodeInfo.ID] = &nodeInfo

	return RegisterDataNodeResponse{
		Success: true,
		Message: "Node registered successfully",
	}.ToProto(), nil
}

// Request from client to verify if some DataNode is healthly and able to process requests
func (c *Coordinator) DataNodeHeartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()

	heartbeatRequest := HeartbeatRequestFromProto(req)

	if node, exists := c.dataNodes[req.NodeId]; exists {
		node.Status = heartbeatRequest.Status.Status
		node.LastSeen = heartbeatRequest.Status.LastSeen

		return HeartbeatResponse{
			Success: true,
		}.ToProto(), nil
	}

	return HeartbeatResponse{
		Success: false,
	}.ToProto(), nil
}

// Helper functions
// TODO: implement selection algorithm, right now, just picking the first healthy nodes
// Selects nodes that could receive some chunk for storage
func (c *Coordinator) selectNodeForChunk() (*common.DataNodeInfo, bool) {
	for _, node := range c.dataNodes {
		if node.Status == common.NodeHealthy {
			return node, true
		}
	}
	return nil, false
}

// Retrieves which nodes have some chunk
func (c *Coordinator) getAvailableNodeForChunk(replicaIDs []string) (*common.DataNodeInfo, bool) {
	for _, replicaID := range replicaIDs {
		if node, exists := c.dataNodes[replicaID]; exists && node.Status == common.NodeHealthy {
			return node, true
		}
	}
	return nil, false
}
