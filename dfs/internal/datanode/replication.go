package datanode

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/pkg/client_pool"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/streaming"
)

type ReplicationProvider interface {
	Replicate(clientPool client_pool.ClientPool, chunkHeader common.ChunkHeader, data []byte, requiredReplicas int) ([]*common.NodeInfo, error)
}

type ReplicatedNodes struct {
	nodes []*common.NodeInfo
	mutex sync.Mutex
}

func (r *ReplicatedNodes) AddNode(node *common.NodeInfo) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.nodes = append(r.nodes, node)
}

func (r *ReplicatedNodes) GetNodes() []*common.NodeInfo {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.nodes
}

type ParalellReplicationService struct {
	Config   config.ParallelReplicationServiceConfig
	streamer streaming.ClientStreamer
	logger   *slog.Logger
}

func NewParalellReplicationService(config config.ParallelReplicationServiceConfig, streamer streaming.ClientStreamer, logger *slog.Logger) *ParalellReplicationService {
	logger = logging.ExtendLogger(logger, slog.String("component", "replication_manager"))
	return &ParalellReplicationService{
		Config:   config,
		streamer: streamer,
		logger:   logger,
	}
}

// Replicate replicates the chunk to the given nodes in parallel
func (rm *ParalellReplicationService) Replicate(clientPool client_pool.ClientPool, chunkHeader common.ChunkHeader,
	data []byte, requiredReplicas int) ([]*common.NodeInfo, error) {

	logger := logging.OperationLogger(rm.logger, "send_replicate_chunk", slog.String("chunk_id", chunkHeader.ID))

	// Early validation
	if clientPool.Len() < requiredReplicas {
		return nil, fmt.Errorf("insufficient clients: have %d, need %d", clientPool.Len(), requiredReplicas)
	}

	ctx, cancel := context.WithTimeout(context.Background(), rm.Config.ReplicateTimeout)
	defer cancel()

	// Start replication
	replicatedNodes, err := rm.startReplication(ctx, clientPool, chunkHeader, data, requiredReplicas, logger)
	if err != nil {
		return nil, err
	}

	return replicatedNodes.GetNodes(), nil
}

// startReplication handles the core replication logic
func (rm *ParalellReplicationService) startReplication(ctx context.Context, clientPool client_pool.ClientPool,
	chunkHeader common.ChunkHeader, data []byte, requiredReplicas int, logger *slog.Logger) (*ReplicatedNodes, error) {

	// Setup concurrency control
	var (
		replicatedNodes    = &ReplicatedNodes{} // thread-safe slice of nodes
		wg                 sync.WaitGroup
		acceptedCount      atomic.Int64
		activeReplications = atomic.Int64{}
		semaphore          = make(chan struct{}, requiredReplicas)
		errChan            = make(chan error, clientPool.Len())
	)

	// Main replication loop
	rm.runReplicationLoop(ctx, clientPool, chunkHeader, data, requiredReplicas, logger,
		&wg, &acceptedCount, &activeReplications, semaphore, errChan, replicatedNodes)

	// Wait for completion and validate results
	return rm.waitAndValidateResults(&wg, errChan, &acceptedCount, requiredReplicas, replicatedNodes)
}

// runReplicationLoop manages the main loop for trying clients
// Doesn't return an error as we might run out of clients but still succesfully replicate to the amount of required clients
func (rm *ParalellReplicationService) runReplicationLoop(ctx context.Context, clientPool client_pool.ClientPool,
	chunkHeader common.ChunkHeader, data []byte, requiredReplicas int, logger *slog.Logger,
	wg *sync.WaitGroup, acceptedCount *atomic.Int64, activeReplications *atomic.Int64, semaphore chan struct{}, errChan chan error,
	replicatedNodes *ReplicatedNodes) {

loop:
	for {
		// Get semaphore slot or timeout
		select {
		case semaphore <- struct{}{}:
		case <-ctx.Done():
			break loop // we exit if the context is cancelled
		}

		if ctx.Err() != nil {
			break loop // double check context to avoid timing issues
		}

		// Check if we already have enough successful replicas
		if acceptedCount.Load()+activeReplications.Load() >= int64(requiredReplicas) {
			break loop // succesful exit
		}

		// Try to get a client and start replication
		if err := rm.tryStartReplication(ctx, clientPool, chunkHeader, data, logger, wg, semaphore, errChan, replicatedNodes, acceptedCount, activeReplications); err != nil {
			break loop // ran out of clients to try
		}
	}
}

// tryStartReplication attempts to get a client and start a replication goroutine
func (rm *ParalellReplicationService) tryStartReplication(ctx context.Context, clientPool client_pool.ClientPool,
	chunkHeader common.ChunkHeader, data []byte, logger *slog.Logger, wg *sync.WaitGroup, semaphore chan struct{},
	errChan chan error, replicatedNodes *ReplicatedNodes, acceptedCount *atomic.Int64, activeReplications *atomic.Int64) error {

	// Get a client from the pool
	client, response, err := clientPool.GetRemoveClientWithRetry(func(client clients.IDataNodeClient) (bool, any, error) {
		replicateChunkResponse, err := client.ReplicateChunk(ctx, chunkHeader)
		if err != nil {
			return false, common.StreamingSessionID(""), err
		}
		return replicateChunkResponse.Accept, replicateChunkResponse.SessionID, nil
	})

	if err != nil {
		<-semaphore // Release semaphore slot
		return err  // It only fails if there are no longer any available client to try
	}
	sessionID := response.(common.StreamingSessionID) // should panic if fails anyway

	// Start replication goroutine
	wg.Add(1)
	activeReplications.Add(1)
	go rm.replicateToClient(ctx, client, sessionID, chunkHeader, data, logger,
		wg, semaphore, errChan, replicatedNodes, acceptedCount, activeReplications)

	return nil
}

// replicateToClient handles replication to a single client
func (rm *ParalellReplicationService) replicateToClient(ctx context.Context, client clients.IDataNodeClient, sessionID common.StreamingSessionID,
	chunkHeader common.ChunkHeader, data []byte, logger *slog.Logger, wg *sync.WaitGroup, semaphore chan struct{},
	errChan chan error, replicatedNodes *ReplicatedNodes, acceptedCount *atomic.Int64, activeReplications *atomic.Int64) {

	defer func() {
		wg.Done()
		activeReplications.Add(-1)
		<-semaphore // Release slot only after reducing the active replications amount
	}()

	clientLogger := logging.ExtendLogger(logger, slog.String("client_id", client.Node().ID), slog.String("client_address", client.Node().Endpoint()))
	clientLogger.Debug("Replicating to client")

	if err := rm.replicate(ctx, client, chunkHeader, sessionID, data, clientLogger); err != nil {
		errChan <- fmt.Errorf("replication failed for client %s: %w", client.Node().Endpoint(), err)
		return
	}

	replicatedNodes.AddNode(client.Node())
	acceptedCount.Add(1)
	clientLogger.Debug("Replication succeeded")
}

// waitAndValidateResults waits for all goroutines and validates the final results
func (rm *ParalellReplicationService) waitAndValidateResults(wg *sync.WaitGroup, errChan chan error,
	acceptedCount *atomic.Int64, requiredReplicas int, replicatedNodes *ReplicatedNodes) (*ReplicatedNodes, error) {

	wg.Wait()
	close(errChan)

	// Check if we achieved required replicas
	finalAccepted := acceptedCount.Load()
	if int(finalAccepted) < requiredReplicas {
		// Collect errs for debugging
		var errs []error
		for err := range errChan {
			errs = append(errs, err)
		}
		finalErr := errors.Join(errs...)
		return nil, fmt.Errorf("insufficient replicas: got %d, required %d. Errors: %v",
			finalAccepted, requiredReplicas, finalErr)
	}

	return replicatedNodes, nil
}

func (rm *ParalellReplicationService) replicate(ctx context.Context, client clients.IDataNodeClient, chunkHeader common.ChunkHeader,
	sessionID common.StreamingSessionID, data []byte, clientLogger *slog.Logger) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Create stream to send the chunk data
	stream, err := client.UploadChunkStream(ctx)
	if err != nil {
		return fmt.Errorf("failed to create stream for chunk %s: %w", chunkHeader.ID, err)
	}

	uploadParams := streaming.UploadChunkStreamParams{
		SessionID:   sessionID,
		ChunkHeader: chunkHeader,
		Data:        data,
	}
	_, err = rm.streamer.SendChunkStream(ctx, stream, clientLogger, uploadParams)
	if err != nil {
		return fmt.Errorf("failed to stream chunk data: %w", err)
	}

	return nil
}
