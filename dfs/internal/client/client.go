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

func (c *Client) UploadFile(ctx context.Context, file *os.File, path string, cs int) ([]common.ChunkInfo, error) {
	logger := logging.ExtendLogger(c.logger, slog.String(common.LogOperation, common.OpUpload), slog.String(common.LogFilePath, path))

	// Stat the file to get the file info
	fileInfo, err := file.Stat()
	if err != nil {
		logger.Error("Failed to stat file", slog.String(common.LogError, err.Error()))
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}
	chunksize, err := common.NewChunkSize(int64(cs))
	if err != nil {
		logger.Error("Failed to create chunk size", slog.String(common.LogError, err.Error()))
		return nil, fmt.Errorf("failed to create chunk size: %w", err)
	}

	checksum, err := chunk.CalculateFileChecksum(file)
	if err != nil {
		logger.Error("Failed to calculate checksum", slog.String(common.LogError, err.Error()))
		return nil, fmt.Errorf("failed to calculate checksum")
	}

	uploadRequest := common.UploadRequest{
		Path:      common.FilePath(filepath.Join(path, fileInfo.Name())),
		Size:      common.FileSize(fileInfo.Size()),
		ChunkSize: chunksize,
		Checksum:  checksum,
	}
	logger.Info("Prepared UploadRequest")

	uploadResponse, err := c.coordinatorClient.UploadFile(ctx, uploadRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to submit upload request: %w", err)
	}
	metadataSessionLogger := logging.ExtendLogger(logger, slog.String(common.LogMetadataSessionID, uploadResponse.SessionID.String()),
		slog.String(common.ComponentCoordinator, c.coordinatorClient.Node().ID))
	metadataSessionLogger.Info("Received UploadResponse")

	clientPool, err := client_pool.NewRotatingClientPool(uploadResponse.Nodes)
	if err != nil {
		logger.Error("Failed to create client pool", slog.String(common.LogError, err.Error()))
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
		metadataSessionLogger.Error("Failed to confirm upload", slog.String(common.LogError, err.Error()))
		return nil, fmt.Errorf("failed to confirm upload: %w", err)
	}

	if !confirmUploadResponse.Success {
		metadataSessionLogger.Error("Failed to confirm upload", slog.String(common.LogMessage, confirmUploadResponse.Message))
		return nil, fmt.Errorf("failed to confirm upload")
	}

	return chunkInfos, nil
}

func (c *Client) DownloadFile(ctx context.Context, path string) (string, error) {
	logger := logging.ExtendLogger(c.logger, slog.String(common.LogOperation, common.OpDownload), slog.String(common.LogFilePath, path))

	filepath, err := common.NewFilePath(path)
	if err != nil {
		logger.Error("Failed to create path", slog.String(common.LogError, err.Error()))
		return "", fmt.Errorf("failed to create path: %w", err)
	}

	downloadResponse, err := c.coordinatorClient.DownloadFile(ctx, common.DownloadRequest{Path: filepath})
	if err != nil {
		logger.Error("Failed to download file", slog.String(common.LogError, err.Error()))
		return "", fmt.Errorf("failed to download file %s: %w", filepath.String(), err)
	}

	if downloadResponse.SessionID == "" {
		logger.Error("No session id returned for download file", slog.String(common.LogFilePath, filepath.String()))
		return "", fmt.Errorf("no session id returned for download file %s", filepath.String())
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
	logger := logging.ExtendLogger(c.logger, slog.String(common.LogOperation, common.OpDelete), slog.String(common.LogFilePath, path))

	filepath, err := common.NewFilePath(path)
	if err != nil {
		logger.Error("Failed to create path", slog.String(common.LogError, err.Error()))
		return fmt.Errorf("failed to create path: %w", err)
	}
	deleteResponse, err := c.coordinatorClient.DeleteFile(ctx, common.DeleteRequest{Path: filepath})
	if err != nil {
		logger.Error("Failed to delete file", slog.String(common.LogError, err.Error()))
		return fmt.Errorf("failed to delete file %s: %w", path, err)
	}

	if !deleteResponse.Success {
		logger.Error("Failed to delete file", slog.String(common.LogMessage, deleteResponse.Message))
		return fmt.Errorf("failed to delete file %s", path)
	}

	logger.Info("File deleted successfully", slog.String(common.LogFilePath, path))

	return nil
}

func (c *Client) Close() error {
	return c.coordinatorClient.Close()
}
