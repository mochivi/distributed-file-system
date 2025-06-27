package cluster

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/cluster/node_manager"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/logging"
)

type RegisterService struct {
	logger *slog.Logger
}

func NewRegisterService() *RegisterService {
	return &RegisterService{}
}

func (s *RegisterService) RegisterWithCoordinator(ctx context.Context, nodeInfo *common.DataNodeInfo, nodeManager node_manager.INodeManager) error {
	coordinatorNode, ok := nodeManager.GetLeaderCoordinatorNode()
	if !ok {
		return fmt.Errorf("no coordinator node found")
	}

	logger := logging.OperationLogger(s.logger, "register", slog.String("coordinator_address", coordinatorNode.Endpoint()))
	logger.Info("Registering with coordinator")

	coordinatorClient, err := clients.NewCoordinatorClient(coordinatorNode)
	if err != nil {
		logger.Error("Failed to create coordinator client", slog.String("error", err.Error()))
		return fmt.Errorf("failed to create coordinator client: %v", err)
	}

	req := common.RegisterDataNodeRequest{NodeInfo: *nodeInfo}
	resp, err := coordinatorClient.RegisterDataNode(ctx, req)
	if err != nil {
		logger.Error("Failed to register datanode with coordinator", slog.String("error", err.Error()))
		return fmt.Errorf("failed to register datanode with coordinator: %v", err)
	}

	if !resp.Success {
		logger.Error("Failed to register datanode with coordinator", slog.String("error", resp.Message))
		return fmt.Errorf("failed to register datanode with coordinator: %s", resp.Message)
	}

	// Save information about all nodes
	nodeManager.InitializeNodes(resp.FullNodeList, resp.CurrentVersion)

	logger.Info("Datanode registered with coordinator successfully")
	return nil
}
