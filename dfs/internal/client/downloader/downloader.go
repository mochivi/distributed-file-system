package downloader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/client_pool"
	"github.com/mochivi/distributed-file-system/pkg/streaming"
)

// Downloader is responsible for downloading a file from the data nodes
type Downloader struct {
	config   DownloaderConfig
	streamer streaming.ClientStreamer
}

// DownloaderConfig is the configuration for the downloader
type DownloaderConfig struct {
	NumWorkers      int
	ChunkRetryCount int
	TempDir         string // directory to store temporary files
}

type downloadWork struct {
	chunkID    string
	clientPool client_pool.ClientPool
}

func NewDownloader(streamer streaming.ClientStreamer, config DownloaderConfig) *Downloader {
	return &Downloader{
		config:   config,
		streamer: streamer,
	}
}

func (d *Downloader) DownloadFile(ctx context.Context, fileInfo common.FileInfo, chunkLocations []common.ChunkLocation,
	metadataSessionID string, logger *slog.Logger) (string, error) {

	// Create temporary file
	tempFile, err := os.CreateTemp(d.config.TempDir, fmt.Sprintf("download_%s.tmp", metadataSessionID))
	if err != nil {
		return "", fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer tempFile.Close()

	// Pre-allocate file size (creates sparse file)
	if err := tempFile.Truncate(int64(fileInfo.Size)); err != nil {
		os.Remove(tempFile.Name())
		return "", fmt.Errorf("failed to truncate temp file: %w", err)
	}

	chunkHeaders := make([]common.ChunkHeader, 0, len(fileInfo.Chunks))
	for _, chunk := range fileInfo.Chunks {
		chunkHeaders = append(chunkHeaders, chunk.Header)
	}

	nWorkers := min(d.config.NumWorkers, len(chunkLocations))
	downloadCtx := NewDownloadContext(ctx, chunkLocations, chunkHeaders, tempFile, fileInfo, nWorkers, logger)

	d.startWorkers(downloadCtx, nWorkers)

	// Queue work
	if err := d.queueWork(downloadCtx); err != nil {
		logger.Error("Failed to queue work", slog.String("error", err.Error()))
		return "", fmt.Errorf("failed to queue work: %w", err)
	}

	if err := d.waitForCompletion(downloadCtx); err != nil {
		downloadCtx.file.Delete()
		return "", err
	}

	return downloadCtx.file.Name(), nil
}

func (d *Downloader) startWorkers(downloadCtx *downloadContext, nWorkers int) {
	for i := range nWorkers {
		downloadCtx.wg.Add(1)
		go func(workerID int) {
			defer downloadCtx.wg.Done()
			for work := range downloadCtx.workChan {
				if err := d.processWork(work, downloadCtx); err != nil {
					downloadCtx.logger.Error(fmt.Sprintf("Worker %d failed", workerID), slog.String("error", err.Error()))
					downloadCtx.errChan <- err
				}
			}
		}(i)
	}
}

func (d *Downloader) waitForCompletion(downloadCtx *downloadContext) error {
	// Close error channel when all goroutines complete
	go func() {
		downloadCtx.wg.Wait()
		close(downloadCtx.errChan)
	}()

	select {
	case err, ok := <-downloadCtx.errChan:
		if !ok { // Error channel is closed
			return nil
		}
		return err // fail fast on first error

	case <-downloadCtx.ctx.Done():
		return downloadCtx.ctx.Err()
	}
}

func (d *Downloader) processWork(work downloadWork, downloadCtx *downloadContext) error {
	// Try to download chunk from all clients in the pool
	// if any client fails, we remove it from the pool and try the next one until there's none left
	for work.clientPool.Len() > 0 {
		client, response, err := work.clientPool.GetRemoveClientWithRetry(func(client clients.IDataNodeClient) (bool, any, error) {
			downloadChunkResponse, err := client.PrepareChunkDownload(downloadCtx.ctx, common.DownloadChunkRequest{ChunkID: work.chunkID})
			if err != nil {
				return false, "", err
			}
			return downloadChunkResponse.Accept, downloadChunkResponse, nil
		})
		downloadChunkResponse := response.(common.DownloadReady) // should panic if fails anyway

		if err != nil {
			return fmt.Errorf("failed to get client: %w", err)
		}

		if err := d.downloadChunk(downloadCtx, client, downloadChunkResponse.ChunkHeader, downloadChunkResponse.SessionID); err != nil {
			downloadCtx.logger.Error("Failed to download chunk", slog.String("chunk_id", downloadChunkResponse.ChunkHeader.ID), slog.String("error", err.Error()))
			continue
		}
		return nil
	}

	return fmt.Errorf("failed to download chunk %s", work.chunkID)
}

func (d *Downloader) downloadChunk(downloadCtx *downloadContext, client clients.IDataNodeClient, chunkHeader common.ChunkHeader, sessionID string) error {
	downloadCtx.logger.Info("Downloading chunk")

	stream, err := client.DownloadChunkStream(downloadCtx.ctx, common.DownloadStreamRequest{
		SessionID:       sessionID,
		ChunkStreamSize: int32(d.streamer.Config().ChunkStreamSize),
	})
	if err != nil {
		return fmt.Errorf("failed to create download stream: %w", err)
	}

	// TODO: the buffer is as large as the chunk, but we would prefer to flush the buffer from time to time to avoid memory issues
	buffer := bytes.NewBuffer(make([]byte, 0, int(chunkHeader.Size)))

	if err := d.streamer.ReceiveChunkStream(downloadCtx.ctx, stream, buffer, downloadCtx.logger, streaming.DownloadChunkStreamParams{
		SessionID:   sessionID,
		ChunkHeader: chunkHeader,
	}); err != nil {
		return fmt.Errorf("failed to receive chunk stream: %w", err)
	}

	// Calculate offset to write to temp file
	offset := int64(0)
	for i := 0; i < chunkHeader.Index; i++ {
		offset += int64(downloadCtx.chunkHeaders[i].Size)
	}

	// Write buffer to temp file
	if _, err := downloadCtx.file.SeekWrite(offset, io.SeekStart, buffer.Bytes()); err != nil {
		return fmt.Errorf("failed to write to temp file at offset %d: %w", offset, err)
	}

	return nil
}

func (d *Downloader) queueWork(downloadCtx *downloadContext) error {
	defer close(downloadCtx.workChan) // done queuing work
	for _, location := range downloadCtx.chunkLocations {
		if err := downloadCtx.ctx.Err(); err != nil {
			return err
		}

		if len(location.Nodes) == 0 {
			downloadCtx.logger.Error("No nodes available for chunk", slog.String("chunk_id", location.ChunkID))
			return fmt.Errorf("no nodes available for chunk %s", location.ChunkID)
		}

		clientPool, err := client_pool.NewRotatingClientPool(location.Nodes)
		if err != nil {
			return fmt.Errorf("failed to create client pool: %w", err)
		}

		// downloadChunkResponse, err := client.PrepareChunkDownload(downloadCtx.ctx, common.DownloadChunkRequest{ChunkID: location.ChunkID})
		// if err != nil {
		// 	downloadCtx.logger.Error("Failed to prepare chunk %s for download: %v", location.ChunkID, err)
		// 	return fmt.Errorf("failed to prepare chunk %s for download: %v", location.ChunkID, err)
		// }

		// if !downloadChunkResponse.Accept {
		// 	downloadCtx.logger.Error("Node did not accept chunk download %s: %s", location.ChunkID, downloadChunkResponse.Message)
		// 	return fmt.Errorf("node did not accept chunk upload %s: %s", location.ChunkID, downloadChunkResponse.Message)
		// }
		// downloadCtx.logger.Info(fmt.Sprintf("Node accepted chunk %s download request", location.ChunkID))

		// if downloadChunkResponse.SessionID == "" {
		// 	downloadCtx.logger.Error("Node did not return a session ID", slog.String("chunk_id", location.ChunkID), slog.String("node_id", location.Nodes[0].ID))
		// 	return fmt.Errorf("node did not return a session ID")
		// }

		// Provide pool of clients that hold the desired chunk
		downloadCtx.workChan <- downloadWork{
			chunkID:    location.ChunkID,
			clientPool: clientPool,
		}
	}

	return nil
}
