package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/mochivi/distributed-file-system/internal/client"
	"github.com/mochivi/distributed-file-system/internal/coordinator"
	"github.com/mochivi/distributed-file-system/pkg/utils"
)

func TestClientUpload(t *testing.T) {
	coordinatorHost := utils.GetEnvString("COORDINATOR_HOST", "coordinator")
	coordinatorPort := utils.GetEnvInt("COORDINATOR_PORT", 8080)
	coordinatorClient, err := coordinator.NewCoordinatorClient(fmt.Sprintf("%s:%d", coordinatorHost, coordinatorPort))
	if err != nil {
		t.Fatalf("failed to create coordinator client: %v", err)
	}

	client := client.NewClient(coordinatorClient)

	testFilesDir := utils.GetEnvString("TEST_FILES_DIR", "/app/test-files")
	file, err := os.Open(filepath.Join(testFilesDir, "test.txt"))
	if err != nil {
		t.Fatalf("missing required test file")
	}
	defer file.Close()

	uploadAt := "/user/files"
	chunkSize := 8 * 1024 * 1024 // 8MB
	if err := client.UploadFile(context.Background(), file, uploadAt, chunkSize); err != nil {
		t.Errorf("failed to upload file: %v", err)
	}
}
