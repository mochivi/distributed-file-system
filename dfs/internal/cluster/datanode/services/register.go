package datanode_services

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/logging"
)

type RegisterService struct {
	logger *slog.Logger
}

func NewRegisterService(logger *slog.Logger) *RegisterService {
	logger = logging.ServiceLogger(logger, "register")
	return &RegisterService{
		logger: logger,
	}
}

func (s *RegisterService) RegisterWithCoordinator(ctx context.Context, nodeInfo *common.DataNodeInfo,
	clusterStateManager state.ClusterStateManager, coordinatorFinder state.CoordinatorFinder) error {
	coordinatorClient, ok := coordinatorFinder.GetLeaderCoordinator()
	if !ok {
		return fmt.Errorf("no leader coordinator node found")
	}

	logger := logging.OperationLogger(s.logger, "register", slog.String("coordinator_address", coordinatorClient.Node().Endpoint()))
	logger.Info("Registering with coordinator")

	req := common.RegisterDataNodeRequest{NodeInfo: *nodeInfo}
	resp, err := coordinatorClient.RegisterDataNode(ctx, req)
	if err != nil {
		logger.Error("Failed to register datanode with coordinator", slog.String("error", err.Error()))
		return fmt.Errorf("failed to register datanode with coordinator: %w", err)
	}

	if ctx.Err() != nil {
		logger.Error("Failed to register datanode with coordinator", slog.String("error", ctx.Err().Error()))
		return fmt.Errorf("failed to register datanode with coordinator: %w", ctx.Err())
	}

	if !resp.Success {
		logger.Error("Failed to register datanode with coordinator", slog.String("error", resp.Message))
		return fmt.Errorf("failed to register datanode with coordinator: %s", resp.Message)
	}

	// If the node list is empty, version must be 0
	if len(resp.FullNodeList) == 0 && resp.CurrentVersion != 0 {
		logger.Error("Failed to register datanode with coordinator", slog.String("error", "node list was empty but version was not 0"))
		return errors.New("failed to register datanode with coordinator: node list was empty but version was not 0")
	}

	// Save information about all nodes
	clusterStateManager.InitializeNodes(resp.FullNodeList, resp.CurrentVersion)

	logger.Info("Datanode registered with coordinator successfully")
	return nil
}
