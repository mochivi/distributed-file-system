package cluster

import (
	"context"
	"log/slog"
	"sync"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/cluster/node_manager"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
)

type ClusterNode struct {
	// Internal state and lifecycle management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Dependencies
	info              *common.DataNodeInfo // TODO: think about this, maybe we should just use the config instead, but there are extra configuration steps first that need to be done
	config            *config.ClusterNodeConfig
	nodeManager       node_manager.INodeManager
	coordinatorClient *clients.CoordinatorClient
	logger            *slog.Logger

	// services provide some functionality to the cluster node
	services struct {
		register *RegisterService
	}

	// controllers implement some watch loop to manage the cluster node
	controllers struct {
		heartbeat *HeartbeatController
	}
}

func NewNode(config *config.ClusterNodeConfig, info *common.DataNodeInfo, nodeManager node_manager.INodeManager, logger *slog.Logger) *ClusterNode {
	return &ClusterNode{
		config:      config,
		nodeManager: nodeManager,
		logger:      logger,
		info:        info,
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
