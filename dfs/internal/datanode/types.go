package datanode

import (
	"context"
	"log/slog"

	"github.com/mochivi/distributed-file-system/internal/clients"
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
	replicationManager IReplicationManager
	sessionManager     ISessionManager
	clusterViewer      state.ClusterStateViewer
	coordinatorFinder  state.CoordinatorFinder
	selector           cluster.NodeSelector

	info   *common.DataNodeInfo
	config config.DataNodeConfig

	logger *slog.Logger
}

type IReplicationManager interface {
	paralellReplicate(clients []*clients.DataNodeClient, chunkInfo common.ChunkHeader, data []byte, requiredReplicas int) ([]*common.DataNodeInfo, error)
	replicate(ctx context.Context, client *clients.DataNodeClient, req common.ChunkHeader, data []byte, clientLogger *slog.Logger) error
}

type ISessionManager interface {
	Store(sessionID string, session *StreamingSession) error
	Load(sessionID string) (*StreamingSession, bool)
	Delete(sessionID string)
	ExistsForChunk(chunkID string) bool
	LoadByChunk(chunkID string) (*StreamingSession, bool)
}

func NewDataNodeServer(store storage.ChunkStorage, replicationManager IReplicationManager, sessionManager ISessionManager,
	clusterViewer state.ClusterStateViewer, coordinatorFinder state.CoordinatorFinder, selector cluster.NodeSelector, info *common.DataNodeInfo, config config.DataNodeConfig, logger *slog.Logger) *DataNodeServer {
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
