package e2e

import (
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/utils"
)

func TestEndToEnd(t *testing.T) {
	// Startup test client with all dependencies and config
	logger, err := logging.InitLogger()
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}
	client := NewTestClient(t, logger)
	// defer client.Close()

	// Test inputs
	testFilesDir := utils.GetEnvString("TEST_FILES_DIR", "/app/test-files")
	baseUploadAt := "/user/files"
	defaultChunkSize := config.DefaultStreamerConfig(true).ChunkStreamSize

	// Wait for datanodes states to be known by all datanodes -- heartbeat every 30s
	// Temporary solution
	time.Sleep(35 * time.Second)

	// Test varying file sizes
	fileSizeTestCases := []struct {
		name      string
		filename  string
		chunkSize int
	}{
		{name: "file size tests - 1MB", filename: "small_test.txt", chunkSize: defaultChunkSize},
		{name: "file size tests - 10MB", filename: "medium_test.txt", chunkSize: defaultChunkSize},
		{name: "file size tests - 100MB", filename: "large_test.txt", chunkSize: defaultChunkSize},
		// {name: "file size tests - 1GB", filename: "xlarge_test.txt", chunkSize: defaultChunkSize},
		{name: "chunk size tests - 1MB", filename: "small_test.txt", chunkSize: 1 * 1024 * 1024},
		{name: "chunk size tests - 4MB", filename: "medium_test.txt", chunkSize: 4 * 1024 * 1024},
		{name: "chunk size tests - 8MB", filename: "large_test.txt", chunkSize: 8 * 1024 * 1024},
		{name: "chunk size tests - 32MB", filename: "large_test.txt", chunkSize: 32 * 1024 * 1024},
		{name: "chunk size tests - 64MB", filename: "large_test.txt", chunkSize: 64 * 1024 * 1024},
	}

	for _, tt := range fileSizeTestCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			t.Logf("Running test case: %s", tt.name)

			path := filepath.Join(testFilesDir, tt.filename)
			file, err := os.Open(path)
			if err != nil {
				t.Fatalf("failed to open file: %v", err)
			}
			defer file.Close()

			uploadAt := filepath.Join(baseUploadAt, strconv.Itoa(rand.Intn(1000000)))
			t.Logf("Uploading file: %s", tt.filename)
			chunkInfos, err := client.UploadFile(file, uploadAt, tt.chunkSize)
			if err != nil {
				t.Errorf("failed to upload file: %v", err)
			}
			t.Logf("Uploaded file: %s", tt.filename)
			t.Logf("Chunk infos for file: %+v", chunkInfos)

			// Download all chunks from all replicas for this file and validate the checksum
			// bypasses the coordinator and downloads the file directly from the datanodes
			// so that we can check if all replicas have all chunks they report to have
			t.Logf("Downloading all chunks from all replicas for file: %s", tt.filename)
			if err := client.DownloadAllChunks(t, chunkInfos, logger); err != nil {
				t.Errorf("failed to download file: %v", err)
			}

			// Delete the file and try to retrieve it again, we expect to receive an error
			fullFilepath := filepath.Join(uploadAt, tt.filename)
			t.Logf("Deleting file: %s", fullFilepath)
			if err := client.DeleteFile(fullFilepath); err != nil {
				t.Errorf("failed to delete file: %v", err)
			}

			// Try to download the file again, we expect to receive an error
			// this method calls the coordinator to download the file.
			t.Logf("Trying to download deleted file: %s", fullFilepath)
			if _, err := client.DownloadFile(fullFilepath); err == nil {
				t.Errorf("expected error when downloading deleted file")
			}
		})
	}

	// // Test varying chunk sizes
	// chunkSizeTestCases := []struct {
	// 	name      string
	// 	filename  string
	// 	chunkSize int
	// }{}

	// for _, tt := range chunkSizeTestCases {
	// 	t.Run(tt.name, func(t *testing.T) {
	// 		t.Parallel()

	// 		t.Logf("Running test case: %s", tt.name)

	// 		path := filepath.Join(testFilesDir, tt.filename)
	// 		file, err := os.Open(path)
	// 		if err != nil {
	// 			t.Fatalf("failed to open file: %v", err)
	// 		}
	// 		defer file.Close()

	// 		uploadAt := filepath.Join(baseUploadAt, strconv.Itoa(rand.Intn(1000000)))
	// 		t.Logf("Uploading file: %s", tt.filename)
	// 		chunkInfos, err := client.UploadFile(file, uploadAt, tt.chunkSize)
	// 		if err != nil {
	// 			t.Errorf("failed to upload file: %v", err)
	// 		}

	// 		t.Logf("Downloading all chunks from all replicas for file: %s", tt.filename)
	// 		if err := client.DownloadAllChunks(t, chunkInfos, logger); err != nil {
	// 			t.Errorf("failed to download file: %v", err)
	// 		}

	// 		t.Logf("Deleting file: %s", uploadAt)
	// 		if err := client.DeleteFile(uploadAt); err != nil {
	// 			t.Errorf("failed to delete file: %v", err)
	// 		}

	// 		t.Logf("Trying to download deleted file: %s", uploadAt)
	// 		if _, err := client.DownloadFile(uploadAt); err == nil {
	// 			t.Errorf("expected error when downloading deleted file")
	// 		}
	// 	})
	// }
}
