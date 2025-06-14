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

	testFilesDir := utils.GetEnvString("TEST_FILES_DIR", "/app/test-files")
	file, err := os.Open(filepath.Join(testFilesDir, "test.txt"))
	if err != nil {
		t.Fatalf("missing required test file")
	}
	defer file.Close()

	time.Sleep(35 * time.Second)

	uploadAt := "/user/files"
	chunkSize := 8 * 1024 * 1024 // 8MB
	if err := client.UploadFile(context.Background(), file, uploadAt, chunkSize); err != nil {
		t.Errorf("failed to upload file: %v", err)
	}
}
