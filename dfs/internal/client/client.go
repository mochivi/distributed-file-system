package client

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/coordinator"
	"github.com/mochivi/distributed-file-system/pkg/logging"
)

func (c *Client) UploadFile(ctx context.Context, file *os.File, path string, chunksize int) ([]common.ChunkInfo, error) {
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

	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Uploader submits requests to all datanodes with each chunk
	uploader := NewUploader(c.streamer, metadataSessionLogger, UploaderConfig{
		NumWorkers:      10,
		ChunkRetryCount: 3,
	})
	chunkInfos, err := uploader.UploadFile(uploadCtx, file, uploadResponse.ChunkLocations, uploadResponse.SessionID, metadataSessionLogger, chunksize)
	if err != nil {
		return nil, fmt.Errorf("failed to upload file: %w", err)
	}

	// Confirm upload to coordinator with chunk location information
	confirmUploadRequest := coordinator.ConfirmUploadRequest{
		SessionID:  uploadResponse.SessionID, // metadata sessionID, NOT streaming sessionID
		ChunkInfos: chunkInfos,
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

func (c *Client) DownloadFile(ctx context.Context, path string) (string, error) {
	logger := logging.ExtendLogger(c.logger, slog.String("operation", "download_file"), slog.String("path", path))

	downloadResponse, err := c.coordinatorClient.DownloadFile(ctx, coordinator.DownloadRequest{Path: path})
	if err != nil {
		logger.Error("Failed to download file", slog.String("error", err.Error()))
		return "", fmt.Errorf("failed to download file %s: %w", path, err)
	}

	if downloadResponse.SessionID == "" {
		logger.Error("No session id returned for download file", slog.String("path", path))
		return "", fmt.Errorf("no session id returned for download file %s", path)
	}

	// Control all the goroutines and requests we will make
	downloadCtx, cancel := context.WithCancel(ctx)
	defer cancel() // any early return will terminate the created goroutines

	downloader := NewDownloader(DownloaderConfig{
		NumWorkers:      10,
		ChunkRetryCount: 3,
	}, c.streamer)

	tempFile, err := downloader.DownloadFile(downloadCtx, downloadResponse.ChunkLocations, downloadResponse.SessionID, int(downloadResponse.FileInfo.Size), logger)
	if err != nil {
		return "", fmt.Errorf("failed to download file: %w", err)
	}

	return tempFile, nil
}
