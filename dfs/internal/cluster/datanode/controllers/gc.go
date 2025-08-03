package datanode_controllers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/mochivi/distributed-file-system/internal/cluster/shared"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
)

type OrphanedChunksGCProvider interface {
	Run() error
}

type OrphanedChunksGCController struct {
	ctx     context.Context
	cancel  context.CancelCauseFunc
	scanner shared.MetadataScannerProvider
	store   chunk.ChunkStorage // maybe abstract later
	running bool               // No need for mutex as a single goroutine runs the GC
	config  *config.OrphanedChunksGCControllerConfig
	nodeID  string // Must be replaced with some reader of nodeInformation, which could be updated, nodeID should be immutable but it is a good pattern
	logger  *slog.Logger
}

func NewOrphanedChunksGCController(ctx context.Context, scanner shared.MetadataScannerProvider, store chunk.ChunkStorage,
	config *config.OrphanedChunksGCControllerConfig, nodeID string, logger *slog.Logger) *OrphanedChunksGCController {
	ctx, cancel := context.WithCancelCause(ctx)
	return &OrphanedChunksGCController{
		ctx:     ctx,
		cancel:  cancel,
		scanner: scanner,
		store:   store,
		config:  config,
		nodeID:  nodeID,
		logger:  logger,
	}
}

func (c *OrphanedChunksGCController) Run() error {
	ticker := time.NewTicker(c.config.InventoryScanInterval)

	for {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case <-ticker.C:
			c.logger.Info("OrphanedChunksGC cycle starting...")
			ctx, cycleCancel := context.WithTimeout(c.ctx, c.config.Timeout)
			failed, err := c.run(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					cycleCancel() // prevent lostcancel warning
					continue
				}
				c.logger.Error("Failed to delete orphaned chunks", slog.Any("error", err))
			}

			// TODO: Handle failed or leftover items: queue, log, report, decide
			handleFailed(failed)

			cycleCancel()
		}
	}
}

func handleFailed(failed []string) {}

func (c *OrphanedChunksGCController) run(ctx context.Context) ([]string, error) {
	if c.running {
		c.logger.Error("Tried to start GC cycle while already running")
		return nil, nil
	}
	c.running = true
	defer func() { c.running = false }()

	// Retrive the chunks from metadata and inventory
	expectedChunks, actualChunks, err := getChunks(ctx, c.nodeID, c.scanner, c.store)
	if err != nil {
		return nil, err
	}

	// Compare expected vs actual
	orphaned := findOrphanedChunks(expectedChunks, actualChunks)

	// Delegate deletion
	failed, err := c.store.BulkDelete(ctx, c.config.MaxConcurrentDeletes, orphaned)
	if err != nil {
		return nil, fmt.Errorf("failed to delete chunks: %w", err)
	}

	return failed, nil
}

func getChunks(ctx context.Context, nodeID string, scanner shared.MetadataScannerProvider,
	store chunk.ChunkStorage) (map[string]*common.ChunkHeader, map[string]common.ChunkHeader, error) {

	g, ctx := errgroup.WithContext(ctx)
	var actualChunks map[string]common.ChunkHeader
	var expectedChunks map[string]*common.ChunkHeader

	// Run metadata scanner to retrieve chunks for node from metadata store
	g.Go(func() error {
		chunks, err := scanner.GetChunksForNode(ctx, nodeID)
		if err != nil {
			return fmt.Errorf("failed to get chunks for node: %w", err)
		}
		expectedChunks = chunks // It's fine to update directly as there is only one goroutine writing to it
		return nil
	})

	// Run chunk inventory scan
	g.Go(func() error {
		chunks, err := store.GetHeaders(ctx)
		if err != nil {
			return fmt.Errorf("failed to get chunk headers: %w", err)
		}
		actualChunks = chunks // It's fine to update directly as there is only one goroutine writing to it
		return nil
	})

	// Wait for: any error, context cancellation or success
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	// Sanity check
	if actualChunks == nil || expectedChunks == nil {
		return nil, nil, errors.New("unexpected error")
	}

	return expectedChunks, actualChunks, nil
}

func findOrphanedChunks(expectedChunks map[string]*common.ChunkHeader,
	actualChunks map[string]common.ChunkHeader) []string {

	orphaned := make([]string, 0, len(actualChunks))
	for id := range actualChunks {
		if _, ok := expectedChunks[id]; !ok {
			orphaned = append(orphaned, id)
		}
	}
	return orphaned
}
