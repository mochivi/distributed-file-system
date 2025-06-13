package coordinator

import (
	"log/slog"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage"
	"github.com/mochivi/distributed-file-system/pkg/proto"
)

// Implements proto.CoordinatorServiceServer interface
type Coordinator struct {
	proto.UnimplementedCoordinatorServiceServer // Embed

	// Coordinates data nodes access
	nodeManager *common.NodeManager

	// Coordinates metadata storage
	metaStore       storage.MetadataStore
	metadataManager *metadataManager // Coordinates when to actually commit metadata

	config CoordinatorConfig
	logger *slog.Logger
}

func NewCoordinator(cfg CoordinatorConfig, metaStore storage.MetadataStore, metadataManager *metadataManager,
	nodeManager *common.NodeManager, logger *slog.Logger) *Coordinator {
	return &Coordinator{
		metaStore:       metaStore,
		nodeManager:     nodeManager,
		config:          cfg,
		metadataManager: metadataManager,
		logger:          logger,
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
