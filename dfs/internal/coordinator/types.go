package coordinator

import (
	"context"
	"sync"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage"
	"github.com/mochivi/distributed-file-system/pkg/proto"
)

type CoordinatorService interface {
	// File operations
	UploadFile(ctx context.Context, req *UploadRequest) (*UploadResponse, error)
	DownloadFile(ctx context.Context, path string) (*DownloadResponse, error)
	DeleteFile(ctx context.Context, path string) error
	ListFiles(ctx context.Context, directory string) ([]*common.FileInfo, error)

	// Node management
	RegisterDataNode(ctx context.Context, nodeInfo *common.DataNodeInfo) error
	DataNodeHeartbeat(ctx context.Context, nodeID string, status *common.HealthStatus) error
}

// Implements CoordinatorService
type Coordinator struct {
	metaStore   storage.MetadataStore
	dataNodes   map[string]*common.DataNodeInfo
	nodesMutex  sync.RWMutex
	replication int
}

// Upload functionality
type UploadRequest struct {
	Path      string // Path to upload the file
	Size      int    // File size in bytes
	ChunkSize int    // What chunkSize to break it down into
}
type UploadResponse struct {
	ChunkLocations []ChunkLocation //
}

func (ur UploadResponse) ToProto() *proto.UploadResponse {
	protoChunkAssignments := make([]*proto.ChunkLocation, len(ur.ChunkLocations))
	for _, item := range ur.ChunkLocations {
		protoChunkAssignments = append(protoChunkAssignments, item.ToProto())
	}
	return &proto.UploadResponse{
		ChunkLocations: protoChunkAssignments,
	}
}

// Download functionality
type DownloadRequest struct {
	Path string
}
type DownloadResponse struct {
	fileInfo       common.FileInfo
	chunkLocations []ChunkLocation
}

func (dr DownloadResponse) ToProto() *proto.DownloadResponse {
	protoChunkLocations := make([]*proto.ChunkLocation, len(dr.chunkLocations))
	for _, item := range dr.chunkLocations {
		protoChunkLocations = append(protoChunkLocations, item.ToProto())
	}
	return &proto.DownloadResponse{
		FileInfo: dr.fileInfo.ToProto(),
	}
}

type ChunkLocation struct {
	ChunkID   string
	NodeIDs   []string // Available nodes for this chunk
	Endpoints []string // Corresponding endpoints to download from
}

func NewChunkLocationFromProto(pb *proto.ChunkLocation) ChunkLocation {
	return ChunkLocation{
		ChunkID:   pb.ChunkId,
		NodeIDs:   pb.NodeIds,
		Endpoints: pb.Endpoints,
	}
}

func (cs *ChunkLocation) ToProto() *proto.ChunkLocation {
	return &proto.ChunkLocation{
		ChunkId:   cs.ChunkID,
		NodeIds:   cs.NodeIDs,
		Endpoints: cs.Endpoints,
	}
}
