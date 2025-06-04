package integration

import (
	"context"
	"os"
	"testing"

	"github.com/mochivi/distributed-file-system/internal/client"
	"github.com/mochivi/distributed-file-system/internal/coordinator"
)

func TestClientUpload(t *testing.T) {
	// The coordinator IP address is just a environment variable for internal tests
	coordinatorAddr := os.Getenv("DFS_COORDINATOR_ADDR")
	if coordinatorAddr == "" {
		t.Fatalf("missing required DFS_COORDINATOR_ADDR environment variable")
	}

	coordinatorClient, err := coordinator.NewCoordinatorClient(coordinatorAddr)
	if err != nil {
		t.Fatalf("failed to create coordinator client: %v", err)
	}

	client := client.NewClient(coordinatorClient)

	file, err := os.Open("./test-files/test.txt")
	if err != nil {
		t.Fatalf("missing required test file")
	}
	defer file.Close()

	uploadAt := "/user/files/test.txt"
	if err := client.UploadFile(context.Background(), file, uploadAt, 0); err != nil {
		t.Errorf("failed to upload file: %v", err)
	}
}
