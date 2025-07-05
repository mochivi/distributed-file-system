package datanode_controllers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/pkg/logging"
)

var (
	ErrRequireResync = errors.New("require node resync")
	ErrNoUpdates     = errors.New("no updates to apply")
	ErrSameVersion   = errors.New("same version")
)

type HeartbeatProvider interface {
	Run(info *common.NodeInfo, csm state.ClusterStateManager, cf state.CoordinatorFinder) error
}

type heartbeatFunc func(ctx context.Context, req common.HeartbeatRequest, client clients.ICoordinatorClient) ([]common.NodeUpdate, error)

type HeartbeatController struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	config *config.HeartbeatControllerConfig
	logger *slog.Logger

	heartbeatFunc heartbeatFunc // for testing
}

func NewHeartbeatController(ctx context.Context, config *config.HeartbeatControllerConfig, logger *slog.Logger) *HeartbeatController {
	ctx, cancel := context.WithCancelCause(ctx)

	controller := &HeartbeatController{
		ctx:    ctx,
		cancel: cancel,
		config: config,
		logger: logger,
	}
	controller.heartbeatFunc = controller.heartbeat
	return controller
}

func (h *HeartbeatController) Run(info *common.NodeInfo, clusterStateManager state.ClusterStateManager, coordinatorFinder state.CoordinatorFinder) error {
	ticker := time.NewTicker(h.config.Interval)
	defer ticker.Stop()

	errorCount := 0
	for {
		if h.ctx.Err() != nil {
			return h.ctx.Err()
		}

		coordinatorClient, ok := coordinatorFinder.GetLeaderCoordinator()
		if !ok {
			return fmt.Errorf("no coordinator node found")
		}

		req := common.HeartbeatRequest{
			NodeID: info.ID,
			Status: common.HealthStatus{
				Status:   info.Status,
				LastSeen: time.Now(),
			},
			LastSeenVersion: clusterStateManager.GetCurrentVersion(),
		}

		updates, err := h.heartbeatFunc(h.ctx, req, coordinatorClient)
		if err != nil {
			coordinatorClient.Close()

			if errors.Is(err, ErrNoUpdates) || errors.Is(err, ErrSameVersion) {
				continue
			}

			errorCount++
			if errorCount > 3 {
				return fmt.Errorf("heartbeat failed: %w", err)
			}
			ticker.Reset(30 * time.Duration(errorCount) * time.Second)
			continue
		}
		clusterStateManager.ApplyUpdates(updates)
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

	if resp.FromVersion == resp.ToVersion {
		return nil, ErrSameVersion
	}
	if len(resp.Updates) == 0 {
		return nil, ErrNoUpdates
	}

	logger.Debug("Updating nodes", slog.Int("from_version", int(resp.FromVersion)), slog.Int("to_version", int(resp.ToVersion)))
	return resp.Updates, nil
}

// Cancel cancels the heartbeat context
func (h *HeartbeatController) Cancel(cause ...error) {
	if h.ctx.Err() != nil {
		return
	}

	if len(cause) == 0 {
		h.cancel(context.Canceled)
		return
	}

	h.cancel(errors.Join(cause...))
}
