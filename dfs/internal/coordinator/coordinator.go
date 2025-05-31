package coordinator

import (
	"context"
	"fmt"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/proto"
)

const CHUNK_SIZE = 8 // 8MB default chunk size

// Client request for a file upload
func (c *Coordinator) UploadFile(ctx context.Context, req *proto.UploadRequest) (*proto.UploadResponse, error) {
	// transform into internal representation
	uploadRequest := newUploadRequestFromProto(req)

	// Calculate number of chunks needed
	chunkSize := uploadRequest.ChunkSize
	if chunkSize == 0 {
		chunkSize = CHUNK_SIZE * 1024 * 1024 // Default 8MB chunks
	}
	numChunks := (uploadRequest.Size + chunkSize - 1) / chunkSize

	// Select nodes for each chunk, locking the nodes until complete
	assignments := make([]ChunkLocation, 0, numChunks)
	c.nodesMutex.RLock()
	for i := range numChunks {
		chunkID := common.FormatChunkID(uploadRequest.Path, i)

		// Select nodes for this chunk (primary + replicas)
		nodes := c.selectNodesForChunk(c.replication)
		if len(nodes) == 0 {
			return nil, fmt.Errorf("no available nodes for chunk storage")
		}

		nodeIDs := make([]string, len(nodes))
		endpoints := make([]string, len(nodes))

		for j, node := range nodes {
			nodeIDs[j] = node.ID
			endpoints[j] = fmt.Sprintf("%s:%d", node.IPAddress, node.Port)
		}

		// Add chunk location to assignment
		assignments[i] = ChunkLocation{
			ChunkID:   chunkID,
			NodeIDs:   nodeIDs,
			Endpoints: endpoints,
		}
	}
	c.nodesMutex.RUnlock()

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
		availableNodes := c.getAvailableNodesForChunk(chunk.Replicas)

		nodeIDs := make([]string, len(availableNodes))
		endpoints := make([]string, len(availableNodes))

		for i, node := range availableNodes {
			nodeIDs[i] = node.ID
			endpoints[i] = fmt.Sprintf("%s:%d", node.IPAddress, node.Port)
		}

		chunkLocations[i] = ChunkLocation{
			ChunkID:   chunk.ID,
			NodeIDs:   nodeIDs,
			Endpoints: endpoints,
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

// Helper methods
// TODO: implement selection algorithm, right now, just picking the first healthy nodes
// Selects nodes that could receive some chunk for storage
func (c *Coordinator) selectNodesForChunk(replicationFactor int) []*common.DataNodeInfo {
	var selected []*common.DataNodeInfo

	for _, node := range c.dataNodes {
		if node.Status == common.NodeHealthy {
			selected = append(selected, node)

			// Check if required amount of nodes have been selected
			if len(selected) >= replicationFactor {
				break
			}
		}
	}

	return selected
}

// Retrieves which nodes have some chunk
func (c *Coordinator) getAvailableNodesForChunk(replicaIDs []string) []*common.DataNodeInfo {
	var available []*common.DataNodeInfo

	for _, replicaID := range replicaIDs {
		if node, exists := c.dataNodes[replicaID]; exists && node.Status == common.NodeHealthy {
			available = append(available, node)
		}
	}

	return available
}
