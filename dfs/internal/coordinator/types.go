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

// Contains all dependencies for the coordinator server
type container struct {
	// Coordinates data nodes access
	clusterStateHistoryManager state.ClusterStateHistoryManager
	selector                   cluster.NodeSelector

	// Coordinates metadata storage
	metaStore       storage.MetadataStore
	metadataManager MetadataSessionManager // Coordinates when to actually commit metadata
}

func NewContainer(metaStore storage.MetadataStore, metadataManager MetadataSessionManager,
	clusterStateHistoryManager state.ClusterStateHistoryManager, selector cluster.NodeSelector) *container {
	return &container{
		metaStore:                  metaStore,
		metadataManager:            metadataManager,
		clusterStateHistoryManager: clusterStateHistoryManager,
		selector:                   selector,
	}
}

// Implements proto.CoordinatorServiceServer interface
type Coordinator struct {
	proto.UnimplementedCoordinatorServiceServer // Embed
	*container                                  // Embed dependencies

	config *config.CoordinatorConfig
	logger *slog.Logger
}

func NewCoordinator(cfg *config.CoordinatorConfig, container *container, logger *slog.Logger) *Coordinator {
	// Extend logger
	coordinatorLogger := logging.ExtendLogger(logger, slog.String("component", "coordinator_server"))
	return &Coordinator{
		container: container,
		config:    cfg,
		logger:    coordinatorLogger,
	}
}
