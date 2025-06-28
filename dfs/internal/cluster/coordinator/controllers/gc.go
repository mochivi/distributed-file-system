package coordinator_controllers

import (
	"context"
	"log/slog"
	"time"

	"github.com/mochivi/distributed-file-system/internal/config"
)

// Garbage collection controller
type GarbageCollectionController struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	config *config.GarbageCollectionControllerConfig
	logger *slog.Logger
}

func NewGarbageCollectionController(ctx context.Context, config *config.GarbageCollectionControllerConfig, logger *slog.Logger) *GarbageCollectionController {
	ctx, cancel := context.WithCancelCause(ctx)

	return &GarbageCollectionController{
		ctx:    ctx,
		cancel: cancel,
		config: config,
		logger: logger,
	}
}

func (c *GarbageCollectionController) Run() error {
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

func (c *GarbageCollectionController) run() error {
	time.Sleep(1 * time.Second) // Pretend we're doing something
	return nil
}
