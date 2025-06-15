package client

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/coordinator"
	"github.com/mochivi/distributed-file-system/internal/datanode"
	"github.com/mochivi/distributed-file-system/pkg/logging"
)

// Define the datatype that workers will receive
type Work struct {
	sessionID string
	chunkMeta common.ChunkMeta
	data      []byte
	client    *datanode.DataNodeClient
	logger    *slog.Logger
}

func (c *Client) UploadFile(ctx context.Context, file *os.File, path string, chunksize int) (map[string]*common.ChunkInfo, error) {
	logger := logging.ExtendLogger(c.logger, slog.String("operation", "upload_file"), slog.String("path", path))

	// Stat the file to get the file info
	fileInfo, err := file.Stat()
	if err != nil {
		logger.Error("Failed to stat file", slog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	checksum, err := common.CalculateFileChecksum(file)
	if err != nil {
		logger.Error("Failed to calculate checksum", slog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to calculate checksum")
	}

	uploadRequest := coordinator.UploadRequest{
		Path:      filepath.Join(path, fileInfo.Name()),
		Size:      int(fileInfo.Size()),
		ChunkSize: chunksize,
		Checksum:  checksum,
	}
	logger.Info("Prepared UploadRequest", slog.Any("upload_request", uploadRequest))

	uploadResponse, err := c.coordinatorClient.UploadFile(ctx, uploadRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to submit upload request: %w", err)
	}
	metadataSessionLogger := logging.ExtendLogger(logger, slog.String("metadata_session_id", uploadResponse.SessionID), slog.String("coordinator_id", c.coordinatorClient.Node.ID))
	metadataSessionLogger.Info("Received UploadResponse")

	// Control all the goroutines and requests we will make
	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel() // any early return will terminate the created goroutines

	// Worker pool
	const numWorkers = 10
	var wg sync.WaitGroup

	// Make one channel to receive the work, one to write any errors into
	workChan := make(chan Work, numWorkers) // At most numWorkers chunks will be processed at a time, to avoid too much memory usage
	errChan := make(chan error, len(uploadResponse.ChunkLocations))
	chunkInfos := make(map[string]*common.ChunkInfo, len(uploadResponse.ChunkLocations))
	chunkInfosMutex := sync.Mutex{}

	// Launch the worker pool
	metadataSessionLogger.Info("Initializing worker pool", slog.Int("num_workers", numWorkers))
	for i := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			if err := c.processChunkWorker(uploadCtx, workChan, &chunkInfos, &chunkInfosMutex); err != nil {
				metadataSessionLogger.Error(fmt.Sprintf("Worker %d failed", workerID), slog.String("error", err.Error()))
				errChan <- err
			}
		}(i)
	}

	// Read file and queue work
	metadataSessionLogger.Info("Reading file and queueing work", slog.Int("num_chunks", len(uploadResponse.ChunkLocations)))
	for i, chunkUploadLocation := range uploadResponse.ChunkLocations {
		chunkData := make([]byte, chunksize)
		n, err := file.Read(chunkData)
		if err != nil && err != io.EOF {
			close(workChan)
			logger.Error("Failed to read chunk", slog.Int("chunk_index", i), slog.String("error", err.Error()))
			return nil, fmt.Errorf("failed to read chunk %d: %w", i, err)
		}
		chunkData = chunkData[:n]

		checksum := common.CalculateChecksum(chunkData)
		chunkMeta := common.ChunkMeta{
			ChunkID:   chunkUploadLocation.ChunkID,
			ChunkSize: len(chunkData),
			Checksum:  checksum,
		}

		client, err := datanode.NewDataNodeClient(chunkUploadLocation.Node)
		if err != nil {
			metadataSessionLogger.Error("Failed to create connection to datanode client", slog.String("error", err.Error()))
			return nil, fmt.Errorf("failed to create connection to datanode client: %w", err)
		}

		// Check if client accepts to store the chunk
		storeChunkResponse, err := client.StoreChunk(ctx, chunkMeta)
		if err != nil {
			return nil, fmt.Errorf("failed to store chunk %s: %w", chunkMeta.ChunkID, err)
		}

		if !storeChunkResponse.Accept {
			return nil, fmt.Errorf("failed to accept chunk %s: %s", chunkMeta.ChunkID, storeChunkResponse.Message)
		}
		metadataSessionLogger.Info("Chunk accepted", slog.String("chunk_id", chunkMeta.ChunkID))

		streamingSessionLogger := logging.ExtendLogger(metadataSessionLogger, slog.String("streaming_session_id", storeChunkResponse.SessionID), slog.String("datanode_id", client.Node.ID))
		workChan <- Work{
			sessionID: storeChunkResponse.SessionID, // streaming sessionID, NOT metadata sessionID
			chunkMeta: chunkMeta,
			data:      chunkData,
			client:    client,
			logger:    streamingSessionLogger,
		}
	}

	// Finish writing work into the workChan
	close(workChan)

	// Close error channel when all goroutines complete
	// Looks a bit odd to wg.Wait inside a goroutine but the ErrChan blocks until its closed below
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Currently, returning on exactly the first error received
	for err := range errChan {
		if err != nil {
			metadataSessionLogger.Error("Error in worker", slog.String("error", err.Error()))
			return nil, err
		}
	}

	// Now, we need to confirm the upload to the coordinator, so that the metadata is updated
	// This is done by sending a ConfirmUploadRequest to the coordinator
	chunkInfosSlice := make([]common.ChunkInfo, 0, len(chunkInfos))
	for _, chunkInfo := range chunkInfos {
		chunkInfosSlice = append(chunkInfosSlice, *chunkInfo)
	}
	confirmUploadRequest := coordinator.ConfirmUploadRequest{
		SessionID:  uploadResponse.SessionID, // metadata sessionID, NOT streaming sessionID
		ChunkInfos: chunkInfosSlice,
	}
	confirmUploadResponse, err := c.coordinatorClient.ConfirmUpload(ctx, confirmUploadRequest)
	if err != nil {
		metadataSessionLogger.Error("Failed to confirm upload", slog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to confirm upload: %w", err)
	}

	if !confirmUploadResponse.Success {
		metadataSessionLogger.Error("Failed to confirm upload", slog.String("message", confirmUploadResponse.Message))
		return nil, fmt.Errorf("failed to confirm upload")
	}

	return chunkInfos, nil
}

// Processes work from workChan
func (c *Client) processChunkWorker(ctx context.Context, workChan chan Work, chunkInfos *map[string]*common.ChunkInfo, chunkInfosMutex *sync.Mutex) error {
	for work := range workChan {
		// Retry loop
		retryCount := 0
		for retryCount < 3 {
			if err := c.processChunk(ctx, work, work.logger); err != nil {
				retryCount++
				if retryCount >= 3 {
					return fmt.Errorf("failed to store chunk %s after %d retries", work.chunkMeta.ChunkID, retryCount)
				}
			} else {
				break // break out of the retry loop
			}
		}

		// Update chunk infos - this information is sent to the coordinator to update the metadata about the chunk
		chunkInfosMutex.Lock()
		entry, ok := (*chunkInfos)[work.chunkMeta.ChunkID]
		if !ok { // if chunk id has not been replicated yet, add the first replica
			newEntry := common.ChunkInfo{
				ID:       work.chunkMeta.ChunkID,
				Checksum: work.chunkMeta.Checksum,
				Size:     work.chunkMeta.ChunkSize,
				Replicas: []*common.DataNodeInfo{work.client.Node},
			}
			(*chunkInfos)[work.chunkMeta.ChunkID] = &newEntry
		} else { // Add replica to the chunk info
			entry.Replicas = append(entry.Replicas, work.client.Node)
		}
		chunkInfosMutex.Unlock()

		work.logger.Info("Chunk stored")
	}

	return nil
}

func (c *Client) processChunk(ctx context.Context, work Work, streamingLogger *slog.Logger) error {
	streamingLogger.Info("Streaming chunk")

	// Open stream to send chunk data to the datanode
	stream, err := work.client.StreamChunk(ctx)
	if err != nil {
		return fmt.Errorf("failed to create stream for chunk %s: %w", work.chunkMeta.ChunkID, err)
	}

	if err := c.streamer.StreamChunk(ctx, stream, streamingLogger, common.StreamChunkParams{
		SessionID: work.sessionID,
		ChunkMeta: work.chunkMeta,
		Data:      work.data,
	}); err != nil {
		return fmt.Errorf("failed to stream chunk %s: %w", work.chunkMeta.ChunkID, err)
	}

	streamingLogger.Info("Chunk streaming completed")

	return nil
}
