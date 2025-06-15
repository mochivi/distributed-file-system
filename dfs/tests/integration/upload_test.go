package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/internal/client"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/coordinator"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/utils"
)

type TestFile struct {
	Path string
	Size int
}

func TestClientUpload(t *testing.T) {
	coordinatorHost := utils.GetEnvString("COORDINATOR_HOST", "coordinator")
	coordinatorPort := utils.GetEnvInt("COORDINATOR_PORT", 8080)
	coordinatorNode := &common.DataNodeInfo{
		ID:     "coordinator",
		Host:   coordinatorHost,
		Port:   coordinatorPort,
		Status: common.NodeHealthy,
	}
	coordinatorClient, err := coordinator.NewCoordinatorClient(coordinatorNode)
	if err != nil {
		t.Fatalf("failed to create coordinator client: %v", err)
	}

	logger, err := logging.InitLogger()
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}
	client := client.NewClient(coordinatorClient, logger)

	// Test inputs
	testFilesDir := utils.GetEnvString("TEST_FILES_DIR", "/app/test-files")
	uploadAt := "/user/files"
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
	}

	for _, tt := range fileSizeTestCases {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(testFilesDir, tt.filepath)
			file, err := os.Open(path)
			if err != nil {
				t.Fatalf("failed to open file: %v", err)
			}

			if err := client.UploadFile(context.Background(), file, uploadAt, defaultChunkSize); err != nil {
				t.Errorf("failed to upload file: %v", err)
			}

			file.Close()
		})
	}

	// Test varying chunk sizes
	chunkSizeTestCases := []struct {
		name      string
		filepath  string
		chunkSize int
	}{
		{name: "chunk size tests - 16MB", filepath: "large_test.txt", chunkSize: 16 * 1024 * 1024},
		{name: "chunk size tests - 32MB", filepath: "large_test.txt", chunkSize: 32 * 1024 * 1024},
		{name: "chunk size tests - 64MB", filepath: "large_test.txt", chunkSize: 64 * 1024 * 1024},
		{name: "chunk size tests - 128MB", filepath: "large_test.txt", chunkSize: 128 * 1024 * 1024},
		{name: "chunk size tests - 256MB", filepath: "large_test.txt", chunkSize: 256 * 1024 * 1024},
		{name: "chunk size tests - 512MB", filepath: "large_test.txt", chunkSize: 512 * 1024 * 1024},
		{name: "chunk size tests - 1024MB", filepath: "large_test.txt", chunkSize: 1024 * 1024 * 1024},
	}

	for _, tt := range chunkSizeTestCases {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(testFilesDir, tt.filepath)
			file, err := os.Open(path)
			if err != nil {
				t.Fatalf("failed to open file: %v", err)
			}

			if err := client.UploadFile(context.Background(), file, filepath.Join(uploadAt, tt.filepath), tt.chunkSize); err != nil {
				t.Errorf("failed to upload file: %v", err)
			}

			file.Close()
		})
	}
}
