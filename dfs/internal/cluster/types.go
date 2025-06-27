package cluster

import (
	"context"
	"log/slog"
	"sync"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
)

type NodeAgentServices struct {
	register    *RegisterService
	coordinator state.CoordinatorFinder
}

type NodeAgentControllers struct {
	heartbeat *HeartbeatController
}

type NodeAgent struct {
	// Internal state and lifecycle management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Dependencies
	info                *common.DataNodeInfo // TODO: think about this, maybe we should just use the config instead, but there are extra configuration steps first that need to be done
	config              *config.NodeAgentConfig
	clusterStateManager state.ClusterStateManager
	coordinatorClient   *clients.CoordinatorClient
	logger              *slog.Logger

	// services provide some functionality to the cluster node
	services NodeAgentServices

	// controllers implement some watch loop to manage the cluster node
	controllers NodeAgentControllers
}

func NewNodeAgent(config *config.NodeAgentConfig, info *common.DataNodeInfo, clusterStateManager state.ClusterStateManager,
	coordinatorFinder state.CoordinatorFinder, logger *slog.Logger) *NodeAgent {
	ctx, cancel := context.WithCancel(context.Background())

	return &NodeAgent{
		ctx:                 ctx,
		cancel:              cancel,
		config:              config,
		clusterStateManager: clusterStateManager,
		logger:              logger,
		info:                info,
		services: NodeAgentServices{
			register:    NewRegisterService(),
			coordinator: coordinatorFinder,
		},
		controllers: NodeAgentControllers{
			heartbeat: NewHeartbeatController(ctx, config.Heartbeat, info, clusterStateManager, coordinatorFinder, logger),
		},
	}
}

// type ClusterCoordinatorNode struct {
// 	ClusterNode // besides doing everything a ClusterNode does plus coordinator-specific functionality

// 	controllers struct {
// 		gc       *GarbageCollectionController
// 		register *RegisterController
// 	}
// }

// // Garbage collection controller
// type GarbageCollectionController struct {
// }

// // Register controller
// type RegisterController struct {
// }
