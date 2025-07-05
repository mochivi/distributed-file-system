package coordinator_controllers

import (
	"context"
	"log/slog"
	"time"

	"github.com/mochivi/distributed-file-system/internal/cluster/shared"
	"github.com/mochivi/distributed-file-system/internal/config"
)

// DeletedFilesGCController is responsible for garbage collecting deleted files
// Follows expected operations, it does not clean up chunks left from system failures
// Only cleans up chunks for files that are marked for deletion
type DeletedFilesGCController struct {
	ctx     context.Context
	cancel  context.CancelCauseFunc
	scanner shared.MetadataScannerProvider
	running bool
	config  *config.DeletedFilesGCControllerConfig
	logger  *slog.Logger
}

func NewDeletedFilesGCController(ctx context.Context, scanner shared.MetadataScannerProvider, config *config.DeletedFilesGCControllerConfig, logger *slog.Logger) *DeletedFilesGCController {
	ctx, cancel := context.WithCancelCause(ctx)

	return &DeletedFilesGCController{
		ctx:     ctx,
		cancel:  cancel,
		scanner: scanner,
		running: false,
		config:  config,
		logger:  logger,
	}
}

func (c *DeletedFilesGCController) Run() error {
	ticker := time.NewTicker(c.config.Interval)
	defer ticker.Stop()

	// TODO: implement garbage collection

	for {
		select {
		case <-c.ctx.Done():
			return nil
		case <-ticker.C:
			c.logger.Info("Garbage collection controller running")
			if err := c.run(); err != nil {
				c.logger.Error("Garbage collection controller failed", "error", err)
			}
		}
	}
}

// 1. Retrieve the list of files that are marked for deletion using the metadata scanner
// 2. Aggregate the list of chunks for each node
// 3. Send bulk delete requests to the datanodes
func (c *DeletedFilesGCController) run() error {
	if c.running {
		return nil
	}

	c.running = true
	defer func() { c.running = false }()

	return nil
}
