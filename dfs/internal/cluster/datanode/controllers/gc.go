package datanode_controllers

import (
	"context"
	"log/slog"

	"github.com/mochivi/distributed-file-system/internal/cluster/shared"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/storage"
)

type OrphanedChunksGCController struct {
	ctx     context.Context
	cancel  context.CancelCauseFunc
	scanner shared.MetadataScannerProvider
	store   storage.ChunkStorage // maybe abstract later
	config  *config.OrphanedChunksGCControllerConfig
	logger  *slog.Logger
}

func NewOrphanedChunksGCController(ctx context.Context, scanner shared.MetadataScannerProvider, store storage.ChunkStorage, config *config.OrphanedChunksGCControllerConfig, logger *slog.Logger) *OrphanedChunksGCController {
	ctx, cancel := context.WithCancelCause(ctx)
	return &OrphanedChunksGCController{
		ctx:     ctx,
		cancel:  cancel,
		scanner: scanner,
		store:   store,
		config:  config,
		logger:  logger,
	}
}

func (c *OrphanedChunksGCController) Run() error {
	return nil
}
