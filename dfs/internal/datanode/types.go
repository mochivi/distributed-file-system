package datanode

import (
	"log/slog"

	"github.com/mochivi/distributed-file-system/internal/cluster"
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/storage"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
)

// Implements the proto.DataNodeServiceServer interface
type DataNodeServer struct {
	proto.UnimplementedDataNodeServiceServer

	store              storage.ChunkStorage
	replicationManager ReplicationProvider
	sessionManager     SessionManager
	clusterViewer      state.ClusterStateViewer
	coordinatorFinder  state.CoordinatorFinder
	selector           cluster.NodeSelector

	info   *common.DataNodeInfo
	config config.DataNodeConfig

	logger *slog.Logger
}

func NewDataNodeServer(info *common.DataNodeInfo, config config.DataNodeConfig, store storage.ChunkStorage, replicationManager ReplicationProvider,
	sessionManager SessionManager, clusterViewer state.ClusterStateViewer, coordinatorFinder state.CoordinatorFinder,
	selector cluster.NodeSelector, logger *slog.Logger) *DataNodeServer {

	datanodeLogger := logging.ExtendLogger(logger, slog.String("component", "datanode_server"))
	return &DataNodeServer{
		store:              store,
		replicationManager: replicationManager,
		sessionManager:     sessionManager,
		clusterViewer:      clusterViewer,
		coordinatorFinder:  coordinatorFinder,
		selector:           selector,
		info:               info,
		config:             config,
		logger:             datanodeLogger,
	}
}
