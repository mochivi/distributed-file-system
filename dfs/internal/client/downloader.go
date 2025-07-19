package client

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
	"github.com/mochivi/distributed-file-system/pkg/streamer"
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
	streamer *streamer.Streamer
}

type downloadSession struct {
	// Input params
	chunkLocations []common.ChunkLocation

	// Output params
	tempFile     *os.File
	fileMutex    sync.Mutex
	tempFileSize int64

	// Internal state
	ctx      context.Context
	workChan chan DownloadWork
	errChan  chan error
	wg       *sync.WaitGroup
	logger   *slog.Logger // scoped to the upload session
}

func NewDownloader(streamer *streamer.Streamer, config DownloaderConfig) *Downloader {
	return &Downloader{
		config:   config,
		streamer: streamer,
	}
}

func (d *Downloader) DownloadFile(ctx context.Context, chunkLocations []common.ChunkLocation, sessionID string,
	totalFileSize int, logger *slog.Logger) (string, error) {

	// Create temporary file
	tempFile, err := os.CreateTemp(d.config.TempDir, fmt.Sprintf("download_%s.tmp", sessionID))
	if err != nil {
		return "", fmt.Errorf("failed to create temporary file: %w", err)
	}

	// Pre-allocate file size (creates sparse file)
	if err := tempFile.Truncate(int64(totalFileSize)); err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return "", fmt.Errorf("failed to truncate temp file: %w", err)
	}

	session := &downloadSession{
		ctx:            ctx,
		chunkLocations: chunkLocations,
		workChan:       make(chan DownloadWork, d.config.NumWorkers),
		errChan:        make(chan error, len(chunkLocations)),
		wg:             &sync.WaitGroup{},
		logger:         logger,
		tempFile:       tempFile,
		tempFileSize:   int64(totalFileSize),
	}

	// Launch N workers for session
	for i := range d.config.NumWorkers {
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
	if err := d.queueWork(session, sessionID); err != nil {
		tempFile.Close()
		return "", fmt.Errorf("failed to queue work: %w", err)
	}

	// Close work channel
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
		}
	}

	return nil
}

func (d *Downloader) downloadChunk(work DownloadWork, session *downloadSession) error {
	session.logger.Info("Downloading chunk")

	stream, err := work.client.DownloadChunkStream(session.ctx, common.DownloadStreamRequest{
		SessionID:       work.sessionID,
		ChunkStreamSize: int32(d.streamer.Config.ChunkStreamSize),
	})
	if err != nil {
		return fmt.Errorf("failed to create download stream: %w", err)
	}

	// TODO: the buffer is as large as the chunk, but we would prefer to flush the buffer from time to time to avoid memory issues
	buffer := bytes.NewBuffer(make([]byte, work.chunkHeader.Size))

	if err := d.streamer.ReceiveChunkStream(session.ctx, stream, buffer, session.logger, streamer.DownloadChunkStreamParams{
		SessionID:   work.sessionID,
		ChunkHeader: work.chunkHeader,
	}); err != nil {
		return fmt.Errorf("failed to receive chunk stream: %w", err)
	}

	// Write buffer to temp file
	session.fileMutex.Lock()
	session.tempFile.Seek(int64(work.chunkHeader.Index)*work.chunkHeader.Size, io.SeekStart)
	session.tempFile.Write(buffer.Bytes())
	session.fileMutex.Unlock()

	return nil
}

func (d *Downloader) queueWork(session *downloadSession, sessionID string) error {
	for _, location := range session.chunkLocations {
		if err := session.ctx.Err(); err != nil {
			return err
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

		session.workChan <- DownloadWork{
			chunkHeader: common.ChunkHeader{
				ID: location.ChunkID,
			},
			client:    client,
			sessionID: sessionID,
			logger:    session.logger,
		}
	}

	return nil
}
