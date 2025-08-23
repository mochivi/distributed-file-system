package client

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/client_pool"
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

	checksum, err := chunk.CalculateFileChecksum(file)
	if err != nil {
		logger.Error("Failed to calculate checksum", slog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to calculate checksum")
	}

	uploadRequest := common.UploadRequest{
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
	metadataSessionLogger := logging.ExtendLogger(logger, slog.String("metadata_session_id", uploadResponse.SessionID.String()), slog.String("coordinator_id", c.coordinatorClient.Node().ID))
	metadataSessionLogger.Info("Received UploadResponse")

	clientPool, err := client_pool.NewRotatingClientPool(uploadResponse.Nodes)
	if err != nil {
		logger.Error("Failed to create client pool", slog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to create client pool: %w", err)
	}

	// TODO: add cancellation context to the uploader
	chunkInfos, err := c.uploader.UploadFile(ctx, file, clientPool, uploadResponse.ChunkIDs, metadataSessionLogger, chunksize)
	if err != nil {
		return nil, fmt.Errorf("failed to upload file: %w", err)
	}

	// Confirm upload to coordinator with chunk location information
	confirmUploadRequest := common.ConfirmUploadRequest{
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

	downloadResponse, err := c.coordinatorClient.DownloadFile(ctx, common.DownloadRequest{Path: path})
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
	tempFile, err := c.downloader.DownloadFile(downloadCtx, downloadResponse.FileInfo,
		downloadResponse.ChunkLocations, downloadResponse.SessionID, logger)
	if err != nil {
		return "", fmt.Errorf("failed to download file: %w", err)
	}

	return tempFile, nil
}

func (c *Client) DeleteFile(ctx context.Context, path string) error {
	logger := logging.ExtendLogger(c.logger, slog.String("operation", "delete_file"), slog.String("path", path))

	deleteResponse, err := c.coordinatorClient.DeleteFile(ctx, common.DeleteRequest{Path: path})
	if err != nil {
		logger.Error("Failed to delete file", slog.String("error", err.Error()))
		return fmt.Errorf("failed to delete file %s: %w", path, err)
	}

	if !deleteResponse.Success {
		logger.Error("Failed to delete file", slog.String("message", deleteResponse.Message))
		return fmt.Errorf("failed to delete file %s", path)
	}

	logger.Info("File deleted successfully", slog.String("path", path))

	return nil
}

func (c *Client) Close() error {
	return c.coordinatorClient.Close()
}
