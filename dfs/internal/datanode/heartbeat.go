package datanode

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/coordinator"
)

var (
	ErrUnsucessfulHearbeat = errors.New("heartbeat error")
	ErrRequireResync       = errors.New("require node resync")
)

// TODO: datanode should be aware of coordinator rotations
func (s *DataNodeServer) HeartbeatLoop(ctx context.Context, coordinatorAddress string) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		coordinatorClient, err := coordinator.NewCoordinatorClient(coordinatorAddress)
		if err != nil {
			return fmt.Errorf("failed to create coordinator client: %w", err)
		}

		if err := s.heartbeat(ctx, coordinatorClient); err != nil {
			coordinatorClient.Close()
			return fmt.Errorf("heartbeat error: %w", err)
		}
		coordinatorClient.Close()

		select {
		case <-ctx.Done():
			log.Printf("HeartbeatLoop stopping due to context cancellation: %v", ctx.Err())
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *DataNodeServer) heartbeat(ctx context.Context, client *coordinator.CoordinatorClient) error {
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
		log.Printf("Heartbeat error: %v", err)
		return err
	}

	if !resp.Success {
		return fmt.Errorf("heartbeat failed with message '%s': %w", resp.Message, ErrUnsucessfulHearbeat)
	}

	if resp.RequiresFullResync {
		log.Printf("Node requires resync: %s", resp.Message)
		return ErrRequireResync
	}

	log.Printf("Updating nodes version from %d to %d", resp.FromVersion, resp.ToVersion)
	s.nodeManager.ApplyHistory(resp.Updates)

	return nil
}
