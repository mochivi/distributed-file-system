package coordinator

import (
	"log/slog"

	"github.com/mochivi/distributed-file-system/internal/cluster"
	"github.com/mochivi/distributed-file-system/internal/storage"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
)

// Implements proto.CoordinatorServiceServer interface
type Coordinator struct {
	proto.UnimplementedCoordinatorServiceServer // Embed

	// Coordinates data nodes access
	nodeManager cluster.INodeManager

	// Coordinates metadata storage
	metaStore       storage.MetadataStore
	metadataManager *metadataManager // Coordinates when to actually commit metadata

	config CoordinatorConfig
	logger *slog.Logger
}

func NewCoordinator(cfg CoordinatorConfig, metaStore storage.MetadataStore, metadataManager *metadataManager,
	nodeManager cluster.INodeManager, logger *slog.Logger) *Coordinator {
	coordinatorLogger := logging.ExtendLogger(logger, slog.String("component", "coordinator_server"))
	return &Coordinator{
		metaStore:       metaStore,
		nodeManager:     nodeManager,
		config:          cfg,
		metadataManager: metadataManager,
		logger:          coordinatorLogger,
	}
}
