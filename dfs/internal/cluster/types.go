package cluster

import (
	"context"
	"log/slog"
	"sync"

	coordinator_controllers "github.com/mochivi/distributed-file-system/internal/cluster/coordinator/controllers"
	datanode_controllers "github.com/mochivi/distributed-file-system/internal/cluster/datanode/controllers"
	datanode_services "github.com/mochivi/distributed-file-system/internal/cluster/datanode/services"
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
)

type NodeAgent struct {
	// Internal state and lifecycle management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Dependencies
	info                *common.NodeInfo // TODO: think about this, maybe we should just use the config instead, but there are extra configuration steps first that need to be done
	config              *config.DatanodeAgentConfig
	clusterStateManager state.ClusterStateManager
	logger              *slog.Logger

	// services provide some functionality to the cluster node
	services *datanode_services.NodeAgentServices

	// controllers implement some watch loop to manage the cluster node
	controllers *datanode_controllers.NodeAgentControllers
}

func NewNodeAgent(ctx context.Context, cancel context.CancelFunc, config *config.DatanodeAgentConfig, info *common.NodeInfo, clusterStateManager state.ClusterStateManager,
	services *datanode_services.NodeAgentServices, controllers *datanode_controllers.NodeAgentControllers, logger *slog.Logger) *NodeAgent {

	return &NodeAgent{
		ctx:                 ctx,
		cancel:              cancel,
		config:              config,
		clusterStateManager: clusterStateManager,
		logger:              logger,
		info:                info,
		services:            services,
		controllers:         controllers,
	}
}

type CoordinatorNodeAgent struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	config *config.CoordinatorAgentConfig
	// clusterStateManager state.ClusterStateManager
	logger *slog.Logger

	// services    CoordinatorNodeAgentServices
	controllers *coordinator_controllers.CoordinatorNodeAgentControllers
}

func NewCoordinatorNodeAgent(ctx context.Context, cancel context.CancelFunc, config *config.CoordinatorAgentConfig, info *common.NodeInfo,
	controllers *coordinator_controllers.CoordinatorNodeAgentControllers, logger *slog.Logger) *CoordinatorNodeAgent {

	return &CoordinatorNodeAgent{
		ctx:    ctx,
		cancel: cancel,
		config: config,
		// clusterStateManager: clusterStateManager,
		logger:      logger,
		controllers: controllers,
		// services:            CoordinatorNodeAgentServices{},
	}
}
