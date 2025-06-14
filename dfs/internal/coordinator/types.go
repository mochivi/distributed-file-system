package coordinator

import (
	"log/slog"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage"
	"github.com/mochivi/distributed-file-system/pkg/logging"
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
	coordinatorLogger := logging.ExtendLogger(logger, slog.String("component", "coordinator_server"))
	return &Coordinator{
		metaStore:       metaStore,
		nodeManager:     nodeManager,
		config:          cfg,
		metadataManager: metadataManager,
		logger:          coordinatorLogger,
	}
}

// ChunkLocation represents where some chunk should be stored (primary node + endpoint)
type ChunkLocation struct {
	ChunkID string
	Node    *common.DataNodeInfo
}

func ChunkLocationFromProto(pb *proto.ChunkLocation) ChunkLocation {
	node := common.DataNodeInfoFromProto(pb.Node)
	return ChunkLocation{
		ChunkID: pb.ChunkId,
		Node:    &node,
	}
}

func (cs *ChunkLocation) ToProto() *proto.ChunkLocation {
	return &proto.ChunkLocation{
		ChunkId: cs.ChunkID,
		Node:    cs.Node.ToProto(),
	}
}
