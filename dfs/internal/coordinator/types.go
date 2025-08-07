package coordinator

import (
	"context"

	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
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
	service                                     Service
}

func NewCoordinator(service Service) *Coordinator {
	return &Coordinator{service: service}
}

type Service interface {
	uploadFile(ctx context.Context, req common.UploadRequest) (common.UploadResponse, error)
	downloadFile(ctx context.Context, req common.DownloadRequest) (common.DownloadResponse, error)
	listFiles(ctx context.Context, req common.ListRequest) (common.ListResponse, error)
	deleteFile(ctx context.Context, req common.DeleteRequest) (common.DeleteResponse, error)
	confirmUpload(ctx context.Context, req common.ConfirmUploadRequest) (common.ConfirmUploadResponse, error)
	registerDataNode(ctx context.Context, req common.RegisterDataNodeRequest) (common.RegisterDataNodeResponse, error)
	heartbeat(ctx context.Context, req common.HeartbeatRequest) (common.HeartbeatResponse, error)
	listNodes(ctx context.Context, req common.ListNodesRequest) (common.ListNodesResponse, error)
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
