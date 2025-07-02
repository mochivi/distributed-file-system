package datanode

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/streamer"
)

type ReplicationProvider interface {
	Replicate(clients []*clients.DataNodeClient, chunkHeader common.ChunkHeader, data []byte, requiredReplicas int) ([]*common.DataNodeInfo, error)
}

type ReplicatedNodes struct {
	nodes []*common.DataNodeInfo
	mutex sync.Mutex
}

func (r *ReplicatedNodes) AddNode(node *common.DataNodeInfo) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.nodes = append(r.nodes, node)
}

func (r *ReplicatedNodes) GetNodes() []*common.DataNodeInfo {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.nodes
}

type ParalellReplicationService struct {
	Config   config.ReplicateManagerConfig
	streamer *streamer.Streamer
	logger   *slog.Logger
}

func NewParalellReplicationService(config config.ReplicateManagerConfig, streamer *streamer.Streamer, logger *slog.Logger) *ParalellReplicationService {
	logger = logging.ExtendLogger(logger, slog.String("component", "replication_manager"))
	return &ParalellReplicationService{
		Config:   config,
		streamer: streamer,
		logger:   logger,
	}
}

// paralellReplicate replicates the chunk to the given nodes in parallel
func (rm *ParalellReplicationService) Replicate(clients []*clients.DataNodeClient, chunkHeader common.ChunkHeader, data []byte, requiredReplicas int) ([]*common.DataNodeInfo, error) {
	logger := logging.OperationLogger(rm.logger, "send_replicate_chunk", slog.String("chunk_id", chunkHeader.ID))
	if len(clients) == 0 {
		return nil, fmt.Errorf("no clients provided")
	}
	if requiredReplicas > len(clients) {
		return nil, fmt.Errorf("required replicas (%d) is greater than the number of available clients (%d)", requiredReplicas, len(clients))
	}

	replicatedNodes := ReplicatedNodes{}

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

	for _, client := range clients {
		clientLogger := logging.ExtendLogger(logger, slog.String("client_id", client.Node.ID), slog.String("client_address", client.Node.Endpoint()))
		// Stop starting new goroutines if we already have enough replicas
		if int(acceptedCount.Load()) >= requiredReplicas {
			break
		}

		semaphore <- struct{}{} // Acquire slot (blocks if channel full)
		wg.Add(1)

		clientLogger.Debug("Replicating to client")
		go func() {
			defer func() {
				<-semaphore // Release slot
				wg.Done()
			}()

			if err := rm.replicate(context.Background(), client, chunkHeader, data, clientLogger); err != nil {
				errChan <- fmt.Errorf("replication failed for client %s: %v", client.Node.Endpoint(), err)
				return
			}
			replicatedNodes.AddNode(client.Node)
			acceptedCount.Add(1)
			clientLogger.Debug("Replication succeeded")
		}()
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
		return nil, fmt.Errorf("insufficient replicas: got %d, required %d. Errors: %v",
			finalAccepted, requiredReplicas, errors)
	}

	return replicatedNodes.GetNodes(), nil
}

func (rm *ParalellReplicationService) replicate(ctx context.Context, client *clients.DataNodeClient, chunkHeader common.ChunkHeader, data []byte, clientLogger *slog.Logger) error {
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

	_, err = rm.streamer.SendChunkStream(ctx, stream, clientLogger, streamer.UploadChunkStreamParams{
		SessionID:   resp.SessionID,
		ChunkHeader: chunkHeader,
		Data:        data,
	})
	if err != nil {
		return fmt.Errorf("failed to stream chunk data: %v", err)
	}

	return nil
}
