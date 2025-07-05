package e2e

import (
	"bytes"
	"context"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/internal/client"
	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/streamer"
	"github.com/mochivi/distributed-file-system/pkg/utils"
)

type TestFile struct {
	Path string
	Size int
}

func NewTestClient(t *testing.T, logger *slog.Logger) *client.Client {
	coordinatorHost := utils.GetEnvString("COORDINATOR_HOST", "coordinator")
	coordinatorPort := utils.GetEnvInt("COORDINATOR_PORT", 8080)
	coordinatorNode := &common.NodeInfo{
		ID:     "coordinator",
		Host:   coordinatorHost,
		Port:   coordinatorPort,
		Status: common.NodeHealthy,
	}
	coordinatorClient, err := clients.NewCoordinatorClient(coordinatorNode)
	if err != nil {
		t.Fatalf("failed to create coordinator client: %v", err)
	}

	streamer := streamer.NewStreamer(config.DefaultStreamerConfig(true))
	uploader := client.NewUploader(streamer, logger, client.UploaderConfig{
		NumWorkers:      10,
		ChunkRetryCount: 3,
	})
	downloader := client.NewDownloader(streamer, client.DownloaderConfig{
		NumWorkers:      10,
		ChunkRetryCount: 3,
		TempDir:         "/tmp",
	})
	return client.NewClient(coordinatorClient, uploader, downloader, streamer, logger)
}

func TestClientUpload(t *testing.T) {
	// Startup test client with all dependencies and config
	logger, err := logging.InitLogger()
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}
	client := NewTestClient(t, logger)

	// Test inputs
	testFilesDir := utils.GetEnvString("TEST_FILES_DIR", "/app/test-files")
	baseUploadAt := "/user/files"
	defaultChunkSize := 8 * 1024 * 1024 // Default chunk size 8MB

	// Wait for datanodes states to be known by all datanodes -- heartbeat every 30s
	// Temporary solution
	time.Sleep(35 * time.Second)

	// Test varying file sizes
	fileSizeTestCases := []struct {
		name     string
		filepath string
	}{
		{name: "file size tests - 1MB", filepath: "small_test.txt"},
		{name: "file size tests - 10MB", filepath: "medium_test.txt"},
		{name: "file size tests - 100MB", filepath: "large_test.txt"},
		{name: "file size tests - 1GB", filepath: "xlarge_test.txt"},
	}

	for _, tt := range fileSizeTestCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(testFilesDir, tt.filepath)
			file, err := os.Open(path)
			if err != nil {
				t.Fatalf("failed to open file: %v", err)
			}

			uploadAt := filepath.Join(baseUploadAt, tt.filepath, strconv.Itoa(rand.Intn(1000000)))
			chunkInfos, err := client.UploadFile(context.Background(), file, uploadAt, defaultChunkSize)
			if err != nil {
				t.Errorf("failed to upload file: %v", err)
			}
			file.Close()

			// Validate if the chunks were replicated to the provided nodes and their checksum matches the expectation
			streamerInstance := streamer.NewStreamer(config.StreamerConfig{
				ChunkStreamSize: defaultChunkSize,
			})
			streamerInstance.Config.WaitReplicas = true // Wait for the final stream with the replicas information

			for _, info := range chunkInfos {
				for _, replica := range info.Replicas {
					dnClient, _ := clients.NewDataNodeClient(replica)

					resp, err := dnClient.PrepareChunkDownload(context.Background(), common.DownloadChunkRequest{ChunkID: info.Header.ID})
					if err != nil {
						t.Errorf("retrieve %s: %v", info.Header.ID, err)
					}

					if !resp.Accept {
						t.Errorf("node %s did not accept chunk %s download", replica.ID, info.Header.ID)
					}

					stream, err := dnClient.DownloadChunkStream(context.Background(), common.DownloadStreamRequest{
						SessionID:       resp.SessionID,
						ChunkStreamSize: int32(config.DefaultStreamerConfig(true).ChunkStreamSize), // Follows the default chunk stream size -- 256KB
					})
					if err != nil {
						t.Fatalf("failed to create download stream: %v", err)
					}

					buffer := bytes.NewBuffer(make([]byte, 0, info.Header.Size))

					if err := streamerInstance.ReceiveChunkStream(context.Background(), stream, buffer, logger, streamer.DownloadChunkStreamParams{
						SessionID:   resp.SessionID,
						ChunkHeader: info.Header,
					}); err != nil {
						t.Errorf("failed to receive chunk stream: %v", err)
					}

					if checksum := chunk.CalculateChecksum(buffer.Bytes()); checksum != info.Header.Checksum {
						t.Errorf("checksum mismatch for %s", info.Header.ID)
					}
				}
			}
		})
	}

	// Test varying chunk sizes
	chunkSizeTestCases := []struct {
		name      string
		filepath  string
		chunkSize int
	}{
		{name: "chunk size tests - 1MB", filepath: "large_test.txt", chunkSize: 1 * 1024 * 1024},
		{name: "chunk size tests - 32MB", filepath: "large_test.txt", chunkSize: 32 * 1024 * 1024},
		{name: "chunk size tests - 64MB", filepath: "large_test.txt", chunkSize: 64 * 1024 * 1024},
	}

	for _, tt := range chunkSizeTestCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(testFilesDir, tt.filepath)
			file, err := os.Open(path)
			if err != nil {
				t.Fatalf("failed to open file: %v", err)
			}

			uploadAt := filepath.Join(baseUploadAt, tt.filepath, strconv.Itoa(rand.Intn(1000000)))
			chunkInfos, err := client.UploadFile(context.Background(), file, uploadAt, tt.chunkSize)
			if err != nil {
				t.Errorf("failed to upload file: %v", err)
			}

			file.Close()

			// Validate if the chunks were replicated to the provided nodes and their checksum matches the expectation
			streamerInstance := streamer.NewStreamer(config.StreamerConfig{
				ChunkStreamSize: tt.chunkSize,
			})
			streamerInstance.Config.WaitReplicas = true // Wait for the final stream with the replicas information

			for _, info := range chunkInfos {
				for _, replica := range info.Replicas {
					dnClient, _ := clients.NewDataNodeClient(replica)

					resp, err := dnClient.PrepareChunkDownload(context.Background(), common.DownloadChunkRequest{ChunkID: info.Header.ID})
					if err != nil {
						t.Errorf("retrieve %s: %v", info.Header.ID, err)
					}

					if !resp.Accept {
						t.Errorf("node %s did not accept chunk %s download", replica.ID, info.Header.ID)
					}

					stream, err := dnClient.DownloadChunkStream(context.Background(), common.DownloadStreamRequest{
						SessionID:       resp.SessionID,
						ChunkStreamSize: int32(config.DefaultStreamerConfig(true).ChunkStreamSize), // Follows the default chunk stream size -- 256KB
					})
					if err != nil {
						t.Fatalf("failed to create download stream: %v", err)
					}

					buffer := bytes.NewBuffer(make([]byte, 0, info.Header.Size))

					if err := streamerInstance.ReceiveChunkStream(context.Background(), stream, buffer, logger, streamer.DownloadChunkStreamParams{
						SessionID:   resp.SessionID,
						ChunkHeader: info.Header,
					}); err != nil {
						t.Errorf("failed to receive chunk stream: %v", err)
					}

					if checksum := chunk.CalculateChecksum(buffer.Bytes()); checksum != info.Header.Checksum {
						t.Errorf("checksum mismatch for %s", info.Header.ID)
					}
				}
			}
		})
	}
}
