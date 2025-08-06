package coordinator

import (
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/storage/metadata"
	"github.com/mochivi/distributed-file-system/pkg/proto"
)

// Contains all dependencies for the coordinator server
type container struct {
	// Coordinates data nodes access
	clusterStateHistoryManager state.ClusterStateHistoryManager
	selector                   state.NodeSelector

	// Coordinates metadata storage
	metaStore       metadata.MetadataStore
	metadataManager MetadataSessionManager // Coordinates when to actually commit metadata
}

func NewContainer(metaStore metadata.MetadataStore, metadataManager MetadataSessionManager,
	clusterStateHistoryManager state.ClusterStateHistoryManager, selector state.NodeSelector) *container {
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
	service                                     *service
}

func NewCoordinator(service *service) *Coordinator {
	return &Coordinator{service: service}
}

type service struct {
	config     *config.CoordinatorConfig
	*container // Embed dependencies
}

func NewService(cfg *config.CoordinatorConfig, container *container) *service {
	return &service{
		config:    cfg,
		container: container,
	}
}
