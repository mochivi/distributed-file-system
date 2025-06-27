package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/cluster/node_manager"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/logging"
)

var (
	ErrRequireResync = errors.New("require node resync")
)

type HeartbeatControllerConfig struct {
	Interval time.Duration // how often to send heartbeats to the coordinator
	Timeout  time.Duration // how long to wait for a heartbeat response from the coordinator
}

type HeartbeatController struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	config *HeartbeatControllerConfig
	logger *slog.Logger
}

func (h *HeartbeatController) Run(info *common.DataNodeInfo, nodeManager node_manager.INodeManager) error {
	ticker := time.NewTicker(h.config.Interval)
	defer ticker.Stop()

	errorCount := 0
	for {
		coordinatorNode, ok := nodeManager.GetLeaderCoordinatorNode()
		if !ok {
			return fmt.Errorf("no coordinator node found")
		}

		coordinatorClient, err := clients.NewCoordinatorClient(coordinatorNode)
		if err != nil {
			return fmt.Errorf("failed to create coordinator client: %w", err)
		}

		// TODO: figure out how to get the node info
		req := common.HeartbeatRequest{
			NodeID: info.ID,
			Status: common.HealthStatus{
				Status:   info.Status,
				LastSeen: time.Now(),
			},
			LastSeenVersion: nodeManager.GetCurrentVersion(),
		}

		updates, err := h.heartbeat(h.ctx, req, coordinatorClient)
		if err != nil {
			coordinatorClient.Close()
			errorCount++
			if errorCount > 3 {
				return fmt.Errorf("heartbeat failed: %w", err)
			}
			ticker.Reset(30 * time.Duration(errorCount) * time.Second)
			continue
		}
		nodeManager.ApplyHistory(updates)
		coordinatorClient.Close()

		select {
		case <-h.ctx.Done():
			return h.ctx.Err()
		case <-ticker.C:
		}
	}
}

func (h *HeartbeatController) heartbeat(ctx context.Context, req common.HeartbeatRequest, client clients.ICoordinatorClient) ([]common.NodeUpdate, error) {
	logger := logging.OperationLogger(h.logger, "heartbeat", slog.String("coordinator_address", client.Node().Endpoint()))

	resp, err := client.DataNodeHeartbeat(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("heartbeat failed: %w", err)
	}

	// If parent context is cancelled, we don't need to return an error but return early
	if ctx.Err() != nil {
		logger.Debug("Heartbeat loop cancelled")
		return nil, nil
	}

	if !resp.Success {
		return nil, fmt.Errorf("heartbeat failed with message '%s'", resp.Message)
	}

	if resp.RequiresFullResync {
		logger.Debug(fmt.Sprintf("Node requires resync: %s", resp.Message))
		return nil, ErrRequireResync
	}

	if resp.FromVersion == resp.ToVersion || len(resp.Updates) == 0 {
		return nil, nil
	}

	if len(resp.Updates) > 0 && resp.Updates != nil {
		logger.Debug("Updating nodes", slog.Int("from_version", int(resp.FromVersion)), slog.Int("to_version", int(resp.ToVersion)))
	}

	logger.Debug("Updating nodes", slog.Int("from_version", int(resp.FromVersion)), slog.Int("to_version", int(resp.ToVersion)))
	return resp.Updates, nil
}
