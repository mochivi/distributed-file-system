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

func (c *Client) UploadFile(ctx context.Context, file *os.File, path string, chunksize int) error {
	logger := logging.ExtendLogger(c.logger, slog.String("operation", "upload_file"), slog.String("path", path))

	// Stat the file to get the file info
	fileInfo, err := file.Stat()
	if err != nil {
		logger.Error("Failed to stat file", slog.String("error", err.Error()))
		return fmt.Errorf("failed to stat file: %w", err)
	}

	checksum, err := common.CalculateFileChecksum(file)
	if err != nil {
		logger.Error("Failed to calculate checksum", slog.String("error", err.Error()))
		return fmt.Errorf("failed to calculate checksum")
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
		return fmt.Errorf("failed to submit upload request: %w", err)
	}
	sessionLogger := logging.ExtendLogger(logger, slog.String("session_id", uploadResponse.SessionID), slog.String("coordinator_id", c.coordinatorClient.Node.ID))
	sessionLogger.Info("Received UploadResponse")

	// Control all the goroutines and requests we will make
	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel() // any early return will terminate the created goroutines

	// Prepare the chunk infos for the ConfirmUploadRequest
	// Must check where each chunk is stored and provide to coordinator for metadata update
	chunkInfos := make([]common.ChunkInfo, 0, len(uploadResponse.ChunkLocations))

	// Define the datatype that workers will receive
	type Work struct {
		req    common.StoreChunkRequest
		client *datanode.DataNodeClient
	}

	// Make one channel to receive the work, one to write any errors into
	workChan := make(chan Work, len(uploadResponse.ChunkLocations))
	errChan := make(chan error, len(uploadResponse.ChunkLocations))

	// Worker pool
	const numWorkers = 10
	var wg sync.WaitGroup

	sessionLogger.Info("Initializing worker pool", slog.Int("num_workers", numWorkers))
	for i := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for work := range workChan {
				sessionLogger.Info("Storing chunk", slog.String("chunk_id", work.req.ChunkID), slog.String("client_id", work.client.Node.ID))
				if err := work.client.StoreChunk(uploadCtx, work.req); err != nil {
					errChan <- fmt.Errorf("failed to store chunk %s: %w", work.req.ChunkID, err)
				}

				// Something like this
				for i, chunkInfo := range chunkInfos {
					if chunkInfo.ID == work.req.ChunkID {
						chunkInfos[i].Replicas = append(chunkInfos[i].Replicas, work.client.Node)
						break
					}
				}

				sessionLogger.Info("Chunk stored", slog.String("chunk_id", work.req.ChunkID))
			}
		}(i)
	}

	// Read file and queue work
	sessionLogger.Info("Reading file and queueing work", slog.Int("num_chunks", len(uploadResponse.ChunkLocations)))
	for i, chunkUploadLocation := range uploadResponse.ChunkLocations {
		chunkData := make([]byte, chunksize)
		n, err := file.Read(chunkData)
		if err != nil && err != io.EOF {
			close(workChan)
			logger.Error("Failed to read chunk", slog.Int("chunk_index", i), slog.String("error", err.Error()))
			return fmt.Errorf("failed to read chunk %d: %w", i, err)
		}
		chunkData = chunkData[:n]

		checksum := common.CalculateChecksum(chunkData)
		req := common.StoreChunkRequest{
			ChunkID:  chunkUploadLocation.ChunkID,
			Data:     chunkData,
			Checksum: checksum,
		}

		chunkInfos = append(chunkInfos, common.ChunkInfo{
			ID:       chunkUploadLocation.ChunkID,
			Checksum: checksum,
			Size:     len(chunkData),
			Replicas: make([]*common.DataNodeInfo, 0),
		})

		client, err := datanode.NewDataNodeClient(chunkUploadLocation.Node)
		if err != nil {
			sessionLogger.Error("Failed to create connection to datanode client", slog.String("error", err.Error()))
			return fmt.Errorf("failed to create connection to datanode client: %w", err)
		}

		workChan <- Work{
			req:    req,
			client: client,
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
			sessionLogger.Error("Error in worker", slog.String("error", err.Error()))
			return err
		}
	}

	// Now, we need to confirm the upload to the coordinator, so that the metadata is updated
	// This is done by sending a ConfirmUploadRequest to the coordinator
	confirmUploadRequest := coordinator.ConfirmUploadRequest{
		SessionID:  uploadResponse.SessionID,
		ChunkInfos: chunkInfos,
	}
	confirmUploadResponse, err := c.coordinatorClient.ConfirmUpload(ctx, confirmUploadRequest)
	if err != nil {
		sessionLogger.Error("Failed to confirm upload", slog.String("error", err.Error()))
		return fmt.Errorf("failed to confirm upload: %w", err)
	}

	if !confirmUploadResponse.Success {
		sessionLogger.Error("Failed to confirm upload", slog.String("message", confirmUploadResponse.Message))
		return fmt.Errorf("failed to confirm upload")
	}

	return nil
}
