package datanode

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/logging"
)

type ReplicationManager struct {
	Config   ReplicateManagerConfig
	streamer *common.Streamer
	logger   *slog.Logger
}

func NewReplicationManager(config ReplicateManagerConfig, logger *slog.Logger) *ReplicationManager {
	logger = logging.ExtendLogger(logger, slog.String("component", "replication_manager"))
	return &ReplicationManager{
		Config:   config,
		streamer: common.NewStreamer(common.DefaultStreamerConfig()),
		logger:   logger,
	}
}

// paralellReplicate replicates the chunk to the given nodes in parallel
func (rm *ReplicationManager) paralellReplicate(nodes []*common.DataNodeInfo, chunkHeader common.ChunkHeader, data []byte, requiredReplicas int) error {
	logger := logging.OperationLogger(rm.logger, "send_replicate_chunk", slog.String("chunk_id", chunkHeader.ID))

	if len(nodes) == 0 {
		return fmt.Errorf("no node endpoints provided")
	}

	// Create clients
	var clients []*DataNodeClient
	for _, node := range nodes {
		client, err := NewDataNodeClient(node)
		if err != nil {
			return fmt.Errorf("failed to create client for %s - [%s]  %v", node.ID, node.Endpoint(), err)
		}
		if client == nil {
			return fmt.Errorf("client for %v is nil", node)
		}
		clients = append(clients, client)
	}
	logger.Debug(fmt.Sprintf("Created connection to %d clients", len(clients)))

	// Clean up all clients
	defer func() {
		for _, client := range clients {
			client.Close()
		}
	}()

	var (
		wg            sync.WaitGroup
		acceptedCount atomic.Int64
		semaphore     = make(chan struct{}, requiredReplicas)
	)
	errChan := make(chan error, len(clients))

	for i, client := range clients {
		clientLogger := logging.ExtendLogger(logger, slog.String("client_id", client.Node.ID), slog.String("client_address", client.Node.Endpoint()))
		// Stop starting new goroutines if we already have enough replicas
		if int(acceptedCount.Load()) >= requiredReplicas {
			break
		}

		semaphore <- struct{}{} // Acquire slot (blocks if channel full)
		wg.Add(1)
		acceptedCount.Add(1)

		clientLogger.Debug("Replicating to client")
		go func(clientIndex int, c *DataNodeClient) {
			defer func() {
				<-semaphore // Release slot
				wg.Done()
			}()

			ctx, cancel := context.WithTimeout(context.Background(), rm.Config.ReplicateTimeout)
			defer cancel()

			if err := rm.replicate(ctx, c, chunkHeader, data, clientLogger); err != nil {
				errChan <- fmt.Errorf("replication failed for client %d: %v", clientIndex, err)
				return
			}

			clientLogger.Debug("Replication succeeded")
		}(i, client)
	}

	wg.Wait()
	close(errChan)

	// Check if we achieved required replicas
	finalAccepted := acceptedCount.Load()
	if int(finalAccepted) < requiredReplicas {
		// Collect errors for debugging
		var errors []error
		for err := range errChan {
			errors = append(errors, err)
		}
		return fmt.Errorf("insufficient replicas: got %d, required %d. Errors: %v",
			finalAccepted, requiredReplicas, errors)
	}

	return nil
}

func (rm *ReplicationManager) replicate(ctx context.Context, client *DataNodeClient, chunkHeader common.ChunkHeader, data []byte, clientLogger *slog.Logger) error {
	// Request replication session
	resp, err := client.ReplicateChunk(ctx, chunkHeader)
	if err != nil {
		return fmt.Errorf("failed to request replication: %v", err)
	}

	if !resp.Accept {
		return fmt.Errorf("replication request rejected: %s", resp.Message)
	}
	clientLogger.Debug("Replication request accepted")

	// Create stream to send the chunk data
	stream, err := client.UploadChunkStream(ctx)
	if err != nil {
		return fmt.Errorf("failed to create stream for chunk %s: %v", chunkHeader.ID, err)
	}

	if err := rm.streamer.SendChunkStream(ctx, stream, clientLogger, common.UploadChunkStreamParams{
		SessionID:   resp.SessionID,
		ChunkHeader: chunkHeader,
		Data:        data,
	}); err != nil {
		return fmt.Errorf("failed to stream chunk data: %v", err)
	}

	return nil
}
