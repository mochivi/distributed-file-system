package datanode

import (
	"log/slog"

	"github.com/mochivi/distributed-file-system/internal/cluster"
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/client_pool"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/mochivi/distributed-file-system/pkg/streaming"
)

type container struct {
	store                 chunk.ChunkStorage
	replicationManager    ReplicationProvider
	sessionManager        streaming.SessionManager
	clusterViewer         state.ClusterStateViewer
	coordinatorFinder     state.CoordinatorFinder
	selector              cluster.NodeSelector
	serverStreamerFactory streaming.ServerStreamerFactory
	clientPoolFactory     client_pool.ClientPoolFactory
}

func NewContainer(store chunk.ChunkStorage, replicationManager ReplicationProvider, sessionManager streaming.SessionManager,
	clusterViewer state.ClusterStateViewer, coordinatorFinder state.CoordinatorFinder, selector cluster.NodeSelector,
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

// Implements the proto.DataNodeServiceServer interface
type DataNodeServer struct {
	proto.UnimplementedDataNodeServiceServer
	*container

	info   *common.NodeInfo
	config config.DataNodeConfig

	logger *slog.Logger
}

func NewDataNodeServer(info *common.NodeInfo, config config.DataNodeConfig, container *container, logger *slog.Logger) *DataNodeServer {

	datanodeLogger := logging.ExtendLogger(logger, slog.String("component", "datanode_server"))
	return &DataNodeServer{
		container: container,
		info:      info,
		config:    config,
		logger:    datanodeLogger,
	}
}
