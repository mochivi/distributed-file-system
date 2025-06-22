package datanode

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/coordinator"
	"github.com/mochivi/distributed-file-system/pkg/logging"
)

var (
	ErrRequireResync = errors.New("require node resync")
)

// TODO: datanode should be aware of coordinator rotations
func (s *DataNodeServer) HeartbeatLoop(ctx context.Context, node *common.DataNodeInfo) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	errorCount := 0
	for {
		coordinatorClient, err := coordinator.NewCoordinatorClient(node)
		if err != nil {
			return fmt.Errorf("failed to create coordinator client: %w", err)
		}

		if err := s.heartbeat(ctx, coordinatorClient); err != nil {
			coordinatorClient.Close()
			errorCount++
			if errorCount > 3 {
				return fmt.Errorf("heartbeat failed: %w", err)
			}
			ticker.Reset(30 * time.Duration(errorCount) * time.Second)
			continue
		}
		coordinatorClient.Close()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *DataNodeServer) heartbeat(ctx context.Context, client *coordinator.CoordinatorClient) error {
	logger := logging.OperationLogger(s.logger, "heartbeat", slog.String("coordinator_address", client.Node.Endpoint()))

	req := coordinator.HeartbeatRequest{
		NodeID: s.Config.Info.ID,
		Status: common.HealthStatus{
			Status:   s.Config.Info.Status,
			LastSeen: time.Now(),
		},
		LastSeenVersion: s.nodeManager.GetCurrentVersion(),
	}

	resp, err := client.DataNodeHeartbeat(ctx, req)
	if err != nil {
		return fmt.Errorf("heartbeat failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("heartbeat failed with message '%s'", resp.Message)
	}

	if resp.RequiresFullResync {
		logger.Debug(fmt.Sprintf("Node requires resync: %s", resp.Message))
		return ErrRequireResync
	}

	if resp.FromVersion == resp.ToVersion || len(resp.Updates) == 0 {
		return nil
	}

	logger.Debug("Updating nodes", slog.Int("from_version", int(resp.FromVersion)), slog.Int("to_version", int(resp.ToVersion)))
	s.nodeManager.ApplyHistory(resp.Updates)

	return nil
}
