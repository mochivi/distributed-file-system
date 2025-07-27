package downloader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/streaming"
)

type DownloaderConfig struct {
	NumWorkers      int
	ChunkRetryCount int
	TempDir         string // directory to store temporary files
}

type DownloadWork struct {
	chunkHeader common.ChunkHeader
	client      clients.IDataNodeClient
	sessionID   string
	logger      *slog.Logger
}

type Downloader struct {
	config   DownloaderConfig
	streamer streaming.ClientStreamer
}

type downloadSession struct {
	// Input params
	chunkLocations []common.ChunkLocation
	chunkHeaders   []common.ChunkHeader

	// Output params
	tempFile     *os.File
	fileMutex    sync.Mutex
	tempFileSize int64

	// Internal state
	ctx      context.Context
	workChan chan DownloadWork
	errChan  chan error
	wg       *sync.WaitGroup
	logger   *slog.Logger // scoped to the download session
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

	session := &downloadSession{
		ctx:            ctx,
		chunkLocations: chunkLocations,
		chunkHeaders:   chunkHeaders,
		workChan:       make(chan DownloadWork, d.config.NumWorkers),
		errChan:        make(chan error, len(chunkLocations)),
		wg:             &sync.WaitGroup{},
		logger:         logger,
		tempFile:       tempFile,
		tempFileSize:   int64(fileInfo.Size),
	}

	nWorkers := min(d.config.NumWorkers, len(chunkLocations))

	// Launch NumWorkers for session
	for i := range nWorkers {
		session.wg.Add(1)
		go func(workerID int) {
			defer session.wg.Done()
			for work := range session.workChan {
				if err := d.processWork(work, session); err != nil {
					session.logger.Error(fmt.Sprintf("Worker %d failed", workerID), slog.String("error", err.Error()))
					session.errChan <- err
				}
			}
		}(i)
	}

	// Queue work
	if err := d.queueWork(session); err != nil {
		tempFile.Close()
		return "", fmt.Errorf("failed to queue work: %w", err)
	}
	close(session.workChan)

	// Close error channel when all goroutines complete
	go func() {
		session.wg.Wait()

		close(session.errChan)
	}()

	select {
	case err, ok := <-session.errChan:
		if !ok { // Error channel is closed
			tempFile.Close()
			return tempFile.Name(), nil
		}

		// Return on first error
		tempFile.Close()
		os.Remove(tempFile.Name())
		return "", err

	case <-ctx.Done():
		tempFile.Close()
		os.Remove(tempFile.Name())
		return "", ctx.Err()
	}
}

func (d *Downloader) processWork(work DownloadWork, session *downloadSession) error {
	retryCount := 0
	for retryCount < d.config.ChunkRetryCount {
		if err := d.downloadChunk(work, session); err != nil {
			retryCount++
			if retryCount >= d.config.ChunkRetryCount {
				return fmt.Errorf("failed to download chunk %s after %d retries", work.chunkHeader.ID, retryCount)
			}
			session.logger.Error("Failed to download chunk", slog.String("chunk_id", work.chunkHeader.ID), slog.Int("retry_count", retryCount), slog.String("error", err.Error()))
			continue
		}
		break
	}
	return nil
}

func (d *Downloader) downloadChunk(work DownloadWork, session *downloadSession) error {
	session.logger.Info("Downloading chunk")

	stream, err := work.client.DownloadChunkStream(session.ctx, common.DownloadStreamRequest{
		SessionID:       work.sessionID,
		ChunkStreamSize: int32(d.streamer.Config().ChunkStreamSize),
	})
	if err != nil {
		return fmt.Errorf("failed to create download stream: %w", err)
	}

	// TODO: the buffer is as large as the chunk, but we would prefer to flush the buffer from time to time to avoid memory issues
	buffer := bytes.NewBuffer(make([]byte, 0, int(work.chunkHeader.Size)))

	if err := d.streamer.ReceiveChunkStream(session.ctx, stream, buffer, session.logger, streaming.DownloadChunkStreamParams{
		SessionID:   work.sessionID,
		ChunkHeader: work.chunkHeader,
	}); err != nil {
		return fmt.Errorf("failed to receive chunk stream: %w", err)
	}

	// Calculate offset to write to temp file
	offset := int64(0)
	for i := 0; i < work.chunkHeader.Index; i++ {
		offset += int64(session.chunkHeaders[i].Size)
	}

	// Write buffer to temp file
	session.fileMutex.Lock()
	if _, err := session.tempFile.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to offset %d: %w", offset, err)
	}
	if _, err := session.tempFile.Write(buffer.Bytes()); err != nil {
		return fmt.Errorf("failed to write buffer to temp file: %w", err)
	}
	// data := buffer.Bytes()
	// for written := 0; written < len(data); {
	//     n, err := session.tempFile.Write(data[written:])
	//     if err != nil {
	//         return fmt.Errorf("write failed: %w", err)
	//     }
	//     written += n
	// }
	session.fileMutex.Unlock()

	return nil
}

func (d *Downloader) queueWork(session *downloadSession) error {
	for _, location := range session.chunkLocations {
		if err := session.ctx.Err(); err != nil {
			return err
		}

		if len(location.Nodes) == 0 {
			session.logger.Error("No nodes available for chunk", slog.String("chunk_id", location.ChunkID))
			return fmt.Errorf("no nodes available for chunk %s", location.ChunkID)
		}

		client, err := clients.NewDataNodeClient(location.Nodes[0])
		if err != nil {
			return fmt.Errorf("failed to create datanode client: %w", err)
		}

		downloadChunkResponse, err := client.PrepareChunkDownload(session.ctx, common.DownloadChunkRequest{ChunkID: location.ChunkID})
		if err != nil {
			session.logger.Error("Failed to prepare chunk %s for download: %v", location.ChunkID, err)
			return fmt.Errorf("failed to prepare chunk %s for download: %v", location.ChunkID, err)
		}

		if !downloadChunkResponse.Accept {
			session.logger.Error("Node did not accept chunk download %s: %s", location.ChunkID, downloadChunkResponse.Message)
			return fmt.Errorf("node did not accept chunk upload %s: %s", location.ChunkID, downloadChunkResponse.Message)
		}
		session.logger.Info(fmt.Sprintf("Node accepted chunk %s download request", location.ChunkID))

		if downloadChunkResponse.SessionID == "" {
			session.logger.Error("Node did not return a session ID", slog.String("chunk_id", location.ChunkID), slog.String("node_id", location.Nodes[0].ID))
			return fmt.Errorf("node did not return a session ID")
		}

		session.workChan <- DownloadWork{
			chunkHeader: downloadChunkResponse.ChunkHeader,
			client:      client,
			sessionID:   downloadChunkResponse.SessionID,
			logger:      session.logger,
		}
	}

	return nil
}
