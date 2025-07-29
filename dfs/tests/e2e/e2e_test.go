package e2e

import (
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/utils"
)

func compareChecksums(t *testing.T, originalFilePath string, downloadedFilePath string, testName string) {
	originalFile, err := os.Open(originalFilePath)
	if err != nil {
		t.Fatalf("%s: failed to open file: %v", testName, err)
	}
	defer originalFile.Close()
	originalFileData, err := io.ReadAll(originalFile)
	if err != nil {
		t.Fatalf("%s: failed to read file: %v", testName, err)
	}
	originalFileChecksum := chunk.CalculateChecksum(originalFileData)
	t.Logf("%s: Original file checksum: %s", testName, originalFileChecksum)

	downloadedFile, err := os.Open(downloadedFilePath)
	if err != nil {
		t.Fatalf("%s: failed to open downloaded file: %v", testName, err)
	}
	defer downloadedFile.Close()
	downloadedFileData, err := io.ReadAll(downloadedFile)
	if err != nil {
		t.Fatalf("%s: failed to read file: %v", testName, err)
	}
	downloadedFileChecksum := chunk.CalculateChecksum(downloadedFileData)
	t.Logf("%s: Downloaded file checksum: %s", testName, downloadedFileChecksum)

	if originalFileChecksum != downloadedFileChecksum {
		t.Errorf("%s: Checksums do not match", testName)
	}
}

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

	// Wait for datanodes states to be known by all datanodes -- heartbeat every 2s
	// Temporary solution until service discovery is implemented
	time.Sleep(4 * time.Second)

	// Test varying file sizes
	fileSizeTestCases := []struct {
		name      string
		filename  string
		chunkSize int
	}{
		{name: "structured file test - 1000 lines", filename: "small_structured_test.txt", chunkSize: defaultChunkSize},
		{name: "structured file test - 50000 lines", filename: "structured_test.txt", chunkSize: defaultChunkSize},
		{name: "file size tests - 1MB", filename: "small_test.txt", chunkSize: defaultChunkSize},
		{name: "file size tests - 10MB", filename: "medium_test.txt", chunkSize: defaultChunkSize},
		{name: "file size tests - 100MB", filename: "large_test.txt", chunkSize: defaultChunkSize},
		{name: "chunk size tests - 1MB", filename: "small_test.txt", chunkSize: 1 * 1024 * 1024},
		{name: "chunk size tests - 4MB", filename: "medium_test.txt", chunkSize: 4 * 1024 * 1024},
		{name: "chunk size tests - 8MB", filename: "large_test.txt", chunkSize: 8 * 1024 * 1024},
		{name: "chunk size tests - 32MB", filename: "large_test.txt", chunkSize: 32 * 1024 * 1024},
		{name: "chunk size tests - 64MB", filename: "large_test.txt", chunkSize: 64 * 1024 * 1024},
	}

	for _, tt := range fileSizeTestCases {
		t.Run(tt.name, func(t *testing.T) {
			// t.Parallel()

			t.Logf("Running test case: %s", tt.name)

			testFilePath := filepath.Join(testFilesDir, tt.filename)
			file, err := os.Open(testFilePath)
			if err != nil {
				t.Fatalf("%s: failed to open file: %v", tt.name, err)
			}
			defer file.Close()

			uploadAt := filepath.Join(baseUploadAt, strconv.Itoa(rand.Intn(1000000)))
			t.Logf("%s: Uploading file: %s", tt.name, tt.filename)
			chunkInfos, err := client.UploadFile(file, uploadAt, tt.chunkSize)
			if err != nil {
				t.Errorf("%s: failed to upload file: %v", tt.name, err)
			}
			t.Logf("%s: Uploaded file: %s", tt.name, tt.filename)

			// First, try downloading the file from the coordinator
			fullFilepath := filepath.Join(uploadAt, tt.filename)
			downloadFilePath, err := client.DownloadFile(fullFilepath)
			if err != nil {
				t.Fatalf("%s: failed to download file: %v", tt.name, err)
			}
			t.Logf("%s: Downloaded file: %s", tt.name, downloadFilePath)

			// Check the integrity of the downloaded file
			compareChecksums(t, testFilePath, downloadFilePath, tt.name)

			// Download all chunks from all replicas for this file and validate the checksum
			// bypasses the coordinator and downloads the file directly from the datanodes
			// so that we can check if all replicas have all chunks they report to have
			t.Logf("Downloading all chunks from all replicas for file: %s", tt.filename)
			if err := client.DownloadAllChunks(t, chunkInfos, logger); err != nil {
				t.Errorf("%s: failed to download file: %v", tt.name, err)
			}

			// // Delete the file
			// t.Logf("Deleting file: %s", fullFilepath)
			// if err := client.DeleteFile(fullFilepath); err != nil {
			// 	t.Errorf("failed to delete file: %v", err)
			// }

			// // Try to download the file again, we expect to receive an error
			// // this method calls the coordinator to download the file.
			// t.Logf("Trying to download deleted file: %s", fullFilepath)
			// if _, err := client.DownloadFile(fullFilepath); err == nil {
			// 	t.Errorf("expected error when downloading deleted file")
			// }
		})
	}
}
