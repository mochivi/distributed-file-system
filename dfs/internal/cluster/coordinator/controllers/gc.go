package coordinator_controllers

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/cluster/shared"
	"github.com/mochivi/distributed-file-system/internal/common"
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

	for {
		select {
		case <-c.ctx.Done():
			return nil
		case <-ticker.C:
			c.logger.Info("Garbage collection controller running")
			cycleCtx, cycleCancel := context.WithTimeout(c.ctx, c.config.Timeout)
			if err := c.run(cycleCtx); err != nil {
				c.logger.Error("Garbage collection controller failed", "error", err)
			}
			cycleCancel()
		}
	}
}

// 1. Retrieve the list of files that are marked for deletion using the metadata scanner
// 2. Aggregate the list of chunks for each node
// 3. Send bulk delete requests to the datanodes
func (c *DeletedFilesGCController) run(ctx context.Context) error {
	if c.running {
		return nil
	}

	c.running = true
	defer func() { c.running = false }()

	files, err := c.scanner.GetDeletedFiles(time.Now().Add(-c.config.RecoveryTimeout))
	if err != nil {
		c.logger.Error("Failed to get deleted files", "error", err)
		return err
	}

	// Group chunks by node FIRST, then create work items
	nodeToChunks := make(map[string][]string)
	nodeToClient := make(map[string]*clients.DataNodeClient)

	for _, file := range files {
		if !file.Deleted {
			continue
		}

		for _, chunk := range file.Chunks {
			for _, node := range chunk.Replicas {
				// Create client once per node
				if nodeToClient[node.ID] == nil {
					client, err := clients.NewDataNodeClient(node)
					if err != nil {
						c.logger.Error("Failed to create client", "error", err)
						continue
					}
					nodeToClient[node.ID] = client
				}

				// Accumulate chunks for this node
				if _, ok := nodeToChunks[node.ID]; !ok {
					nodeToChunks[node.ID] = []string{}
				}
				nodeToChunks[node.ID] = append(nodeToChunks[node.ID], chunk.Header.ID)
			}
		}
	}

	// Create batched work items
	type Work struct {
		client   *clients.DataNodeClient
		chunkIDs []string
	}

	// Queue work
	works := make(chan *Work)
	go func() {
		defer close(works)

		for nodeID, allChunks := range nodeToChunks {
			client := nodeToClient[nodeID]

			// Create properly sized batches
			for start := 0; start < len(allChunks); start += c.config.BatchSize {
				end := start + c.config.BatchSize
				if end > len(allChunks) {
					end = len(allChunks)
				}

				works <- &Work{
					client:   client,
					chunkIDs: allChunks[start:end],
				}
			}
		}
	}()

	// Run worker pool
	sem := make(chan struct{}, c.config.ConcurrentRequests)
	wg := sync.WaitGroup{}

	for work := range works {
		wg.Add(1)
		go func(w *Work) {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			case sem <- struct{}{}:
				defer func() { <-sem }()
				break
			}

			// Simple single request per work item
			req := common.BulkDeleteChunkRequest{ChunkIDs: w.chunkIDs}
			_, err := w.client.BulkDeleteChunk(ctx, req)
			if err != nil {
				c.logger.Error("Failed to bulk delete chunks",
					"chunk_count", len(w.chunkIDs), "error", err)
			}
			w.client.Close()
		}(work)
	}

	wg.Wait()
	return nil
}
