package coordinator

import (
	"log/slog"

	"github.com/mochivi/distributed-file-system/internal/cluster"
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/storage"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
)

// Implements proto.CoordinatorServiceServer interface
type Coordinator struct {
	proto.UnimplementedCoordinatorServiceServer // Embed

	// Coordinates data nodes access
	clusterStateHistoryManager state.ClusterStateHistoryManager
	selector                   cluster.NodeSelector

	// Coordinates metadata storage
	metaStore       storage.MetadataStore
	metadataManager *metadataManager // Coordinates when to actually commit metadata

	config config.CoordinatorConfig
	logger *slog.Logger
}

func NewCoordinator(cfg config.CoordinatorConfig, metaStore storage.MetadataStore, metadataManager *metadataManager,
	clusterStateHistoryManager state.ClusterStateHistoryManager, selector cluster.NodeSelector, logger *slog.Logger) *Coordinator {
	coordinatorLogger := logging.ExtendLogger(logger, slog.String("component", "coordinator_server"))
	return &Coordinator{
		metaStore:                  metaStore,
		clusterStateHistoryManager: clusterStateHistoryManager,
		selector:                   selector,
		config:                     cfg,
		metadataManager:            metadataManager,
		logger:                     coordinatorLogger,
	}
}
