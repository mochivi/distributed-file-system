package coordinator_controllers

import (
	"context"
	"fmt"
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

func NewDeletedFilesGCController(ctx context.Context, scanner shared.MetadataScannerProvider,
	config *config.DeletedFilesGCControllerConfig, logger *slog.Logger) *DeletedFilesGCController {

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
			return c.ctx.Err()
		case <-ticker.C:
			if c.running {
				c.logger.Error("Tried to start GC cycle while already running")
				continue
			}

			if err := c.tryRun(); err != nil {
				c.logger.Error("Error while trying to start deleted files GC cycle", slog.Any("error", err.Error()))
			}
		}
	}
}

// Create batched work items
type deleteWork struct {
	client   clients.IDataNodeClient
	chunkIDs []string
}

func (c *DeletedFilesGCController) tryRun() error {
	c.logger.Info("DeletedFilesGCController cycle starting")

	cycleCtx, cycleCancel := context.WithTimeout(c.ctx, c.config.Timeout)
	defer cycleCancel()

	files, err := c.scanner.GetDeletedFiles(cycleCtx, time.Now().Add(-c.config.RecoveryTimeout))
	if err != nil {
		return err
	}

	if len(files) == 0 {
		c.logger.Info("No deleted files to clean up")
		return nil
	}

	// At the moment, the cycle doesn't report or take actions on errors
	// It just logs them
	c.run(cycleCtx, files)
	return nil
}

// 1. Retrieve the list of files that are marked for deletion using the metadata scanner
// 2. Aggregate the list of chunks for each node
// 3. Send bulk delete requests to the datanodes
func (c *DeletedFilesGCController) run(ctx context.Context, files []*common.FileInfo) {
	c.running = true
	defer func() { c.running = false }()

	// From each file, retrieve what chunkIDs each datanode holds + a grpc connection to each datanode
	nodeToChunks, nodeToClient := prepareChunkMappings(files, c.logger)

	// Queue work using a buffered channel
	workCh := make(chan *deleteWork, c.config.ConcurrentRequests)
	go queueWork(workCh, nodeToClient, nodeToChunks, c.config.BatchSize)

	// Run worker pool
	wg := sync.WaitGroup{}

	for i := 0; i < c.config.ConcurrentRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := doWork(ctx, workCh); err != nil {
				c.logger.Error("Failed to bulk delete chunks", "error", err)
			}
		}()
	}

	wg.Wait()
}

func prepareChunkMappings(files []*common.FileInfo, logger *slog.Logger) (map[string][]string, map[string]clients.IDataNodeClient) {
	// Group chunks by node FIRST, then create work items
	nodeToChunks := make(map[string][]string)
	nodeToClient := make(map[string]clients.IDataNodeClient)

	for _, file := range files {
		if !file.Deleted {
			continue
		}

		for _, chunk := range file.Chunks {
			for _, node := range chunk.Replicas {
				if _, ok := nodeToClient[node.ID]; !ok {
					client, err := clients.NewDataNodeClient(node)
					if err != nil {
						logger.Error("Failed to create client", "error", err)
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

	return nodeToChunks, nodeToClient
}

func queueWork(workCh chan<- *deleteWork, nodeToClient map[string]clients.IDataNodeClient,
	nodeToChunks map[string][]string, batchSize int) {

	defer close(workCh)

	for nodeID, allChunks := range nodeToChunks {

		// Limit the amount of chunkIDs that can be requested in a single request to the same node
		// TODO: however, in the processWork section, requests to the same node still might be sent by different workers
		// TODO: without any time limit between them, making batching useless, somehow we should improve this
		for start := 0; start < len(allChunks); start += batchSize {
			end := min(start+batchSize, len(allChunks))

			// Blocks if the channel is full
			workCh <- &deleteWork{
				client:   nodeToClient[nodeID],
				chunkIDs: allChunks[start:end],
			}
		}
	}
}

func doWork(ctx context.Context, workCh <-chan *deleteWork) error {
	for {
		select {
		case work, ok := <-workCh:
			if !ok { // channel closed, all items processed, no more work, return
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := processWork(ctx, work); err != nil {
				return fmt.Errorf("failed to delete %d chunks: %w", len(work.chunkIDs), err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func processWork(ctx context.Context, work *deleteWork) error {
	defer work.client.Close()
	req := common.BulkDeleteChunkRequest{ChunkIDs: work.chunkIDs}
	if _, err := work.client.BulkDeleteChunk(ctx, req); err != nil {
		return err
	}
	return nil
}
