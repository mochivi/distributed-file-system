package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/coordinator"
)

func (c *Client) UploadFile(ctx context.Context, file *os.File, path string, chunksize int) error {
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	// Prepare upload request to coordinator
	uploadRequest := coordinator.UploadRequest{
		Path:      path,
		Size:      int(fileInfo.Size()),
		ChunkSize: chunksize,
		Checksum:  common.CalculateChecksum([]byte{1, 2, 3, 4, 1, 2, 4, 5}), // TODO: implement actual file checksum
	}

	uploadResponse, err := c.coordinatorClient.UploadFile(ctx, uploadRequest)
	if err != nil {
		return fmt.Errorf("failed to submit upload request: %w", err)
	}

	// Control all the goroutines and requests we will make
	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel() // any early return will terminate the created goroutines

	// Make one channel to receive the work, one to write any errors into
	workChan := make(chan common.StoreChunkRequest, len(uploadResponse.ChunkLocations))
	errChan := make(chan error, len(uploadResponse.ChunkLocations))

	// Worker pool
	const numWorkers = 10
	var wg sync.WaitGroup

	for i := range numWorkers {
		wg.Add(1)
		log.Printf("initializing worker %d...\n", i)
		go func(workerID int) {
			defer wg.Done()
			for req := range workChan {
				log.Printf("worker %d received work: %+v", workerID, req)
				if err := c.datanodeClient.StoreChunk(uploadCtx, req); err != nil {
					errChan <- fmt.Errorf("failed to store chunk %s: %w", req.ChunkID, err)
				}
			}
		}(i)
	}

	// Read file and queue work
	for i, chunkUploadLocation := range uploadResponse.ChunkLocations {
		chunkData := make([]byte, chunksize)
		n, err := file.Read(chunkData)
		if err != nil && err != io.EOF {
			close(workChan)
			return fmt.Errorf("failed to read chunk %d: %w", i, err)
		}

		chunkData = chunkData[:n]
		workChan <- common.StoreChunkRequest{
			ChunkID:  chunkUploadLocation.ChunkID,
			Data:     chunkData,
			Checksum: common.CalculateChecksum(chunkData),
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
			return err
		}
	}

	return nil
}
