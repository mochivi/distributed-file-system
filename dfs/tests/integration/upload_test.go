package integration

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/mochivi/distributed-file-system/internal/client"
	"github.com/mochivi/distributed-file-system/internal/coordinator"
	"github.com/mochivi/distributed-file-system/internal/datanode"
)

func TestClientUpload(t *testing.T) {
	// Setup coordinator default config
	nodeConfig := datanode.DefaultDatanodeConfig()
	nodeConfig.Coordinator.Host = "coordinator" // Overwriting manually for now so docker can find the coordinator

	coordinatorClient, err := coordinator.NewCoordinatorClient(fmt.Sprintf("%s:%d", nodeConfig.Coordinator.Host, nodeConfig.Coordinator.Port))
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
