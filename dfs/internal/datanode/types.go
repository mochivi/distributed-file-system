package datanode

import (
	"context"

	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/client_pool"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/mochivi/distributed-file-system/pkg/streaming"
)

type container struct {
	store                 chunk.ChunkStorage
	replicationManager    ReplicationProvider
	sessionManager        streaming.SessionManager
	clusterViewer         state.ClusterStateViewer
	coordinatorFinder     state.CoordinatorFinder
	selector              state.NodeSelector
	serverStreamerFactory streaming.ServerStreamerFactory
	clientPoolFactory     client_pool.ClientPoolFactory
}

func NewContainer(store chunk.ChunkStorage, replicationManager ReplicationProvider, sessionManager streaming.SessionManager,
	clusterViewer state.ClusterStateViewer, coordinatorFinder state.CoordinatorFinder, selector state.NodeSelector,
	serverStreamerFactory streaming.ServerStreamerFactory, clientPoolFactory client_pool.ClientPoolFactory) *container {

	return &container{
		store:                 store,
		replicationManager:    replicationManager,
		sessionManager:        sessionManager,
		clusterViewer:         clusterViewer,
		coordinatorFinder:     coordinatorFinder,
		selector:              selector,
		serverStreamerFactory: serverStreamerFactory,
		clientPoolFactory:     clientPoolFactory,
	}
}

// Service defines business operations for the datanode
type Service interface {
	prepareChunkUpload(ctx context.Context, req common.UploadChunkRequest) (common.NodeReady, error)
	prepareChunkDownload(ctx context.Context, req common.DownloadChunkRequest) (common.DownloadReady, error)
	deleteChunk(ctx context.Context, req common.DeleteChunkRequest) (common.DeleteChunkResponse, error)
	bulkDeleteChunk(ctx context.Context, req common.BulkDeleteChunkRequest) (common.BulkDeleteChunkResponse, error)
	uploadChunkStream(stream proto.DataNodeService_UploadChunkStreamServer) error
	downloadChunkStream(req common.DownloadStreamRequest, stream proto.DataNodeService_DownloadChunkStreamServer) error
	healthCheck(ctx context.Context, req common.HealthCheckRequest) (common.HealthCheckResponse, error)
}

type service struct {
	config config.DataNodeConfig
	*container
	info *common.NodeInfo
}

func NewService(cfg config.DataNodeConfig, info *common.NodeInfo, container *container) *service {
	return &service{
		config:    cfg,
		container: container,
		info:      info,
	}
}

// Implements the proto.DataNodeServiceServer interface - transport layer
type DataNodeServer struct {
	proto.UnimplementedDataNodeServiceServer
	service Service
}

func NewDataNodeServer(service Service) *DataNodeServer {
	return &DataNodeServer{service: service}
}
