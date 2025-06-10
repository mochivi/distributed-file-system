package coordinator

import (
	"github.com/mochivi/distributed-file-system/internal/storage"
	"github.com/mochivi/distributed-file-system/pkg/proto"
)

// Implements proto.CoordinatorServiceServer interface
type Coordinator struct {
	proto.UnimplementedCoordinatorServiceServer // Embed

	// Coordinates data nodes access
	nodeManager *NodeManager

	// Coordinates metadata storage
	metaStore       storage.MetadataStore
	metadataManager *metadataManager // Coordinates when to actually commit metadata

	config CoordinatorConfig
}

func NewCoordinator(cfg CoordinatorConfig, metaStore storage.MetadataStore, metadataManager *metadataManager,
	nodeManager *NodeManager) *Coordinator {
	return &Coordinator{
		metaStore:       metaStore,
		nodeManager:     nodeManager,
		config:          cfg,
		metadataManager: metadataManager,
	}
}

// ChunkLocation represents where some chunk should be stored (primary node + endpoint)
type ChunkLocation struct {
	ChunkID  string
	NodeID   string
	Endpoint string
}

func ChunkLocationFromProto(pb *proto.ChunkLocation) ChunkLocation {
	return ChunkLocation{
		ChunkID:  pb.ChunkId,
		NodeID:   pb.NodeId,
		Endpoint: pb.Endpoint,
	}
}

func (cs *ChunkLocation) ToProto() *proto.ChunkLocation {
	return &proto.ChunkLocation{
		ChunkId:  cs.ChunkID,
		NodeId:   cs.NodeID,
		Endpoint: cs.Endpoint,
	}
}
