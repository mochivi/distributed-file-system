package e2e

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/mochivi/distributed-file-system/internal/client"
	"github.com/mochivi/distributed-file-system/internal/client/downloader"
	"github.com/mochivi/distributed-file-system/internal/client/uploader"
	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/streaming"
	"github.com/mochivi/distributed-file-system/pkg/utils"
)

type datanodeClientPool struct {
	clients map[string]*clients.DataNodeClient
	mu      sync.Mutex
}

func (p *datanodeClientPool) getOrAddClient(replica *common.NodeInfo) (*clients.DataNodeClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	dnClient, ok := p.clients[replica.ID]
	if !ok {
		var err error
		dnClient, err = clients.NewDataNodeClient(replica)
		if err != nil {
			return nil, fmt.Errorf("failed to create datanode client: %w", err)
		}
		p.clients[replica.ID] = dnClient
	}

	return dnClient, nil
}

func (p *datanodeClientPool) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, client := range p.clients {
		client.Close()
	}
}

// Global datanode client pool
var dnClientPool = &datanodeClientPool{
	mu:      sync.Mutex{},
	clients: make(map[string]*clients.DataNodeClient),
}

// Wrapper around the client to provide some test utilities
type TestClient struct {
	client *client.Client
}

func NewTestClient(t *testing.T, logger *slog.Logger) *TestClient {
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

	streamer := streaming.NewClientStreamer(config.DefaultStreamerConfig(true))
	uploader := uploader.NewUploader(streamer, logger, uploader.UploaderConfig{
		NumWorkers:      10,
		ChunkRetryCount: 3,
	})
	downloader := downloader.NewDownloader(streamer, downloader.DownloaderConfig{
		NumWorkers:      10,
		ChunkRetryCount: 3,
		TempDir:         "/tmp",
	})

	return &TestClient{
		client: client.NewClient(coordinatorClient, uploader, downloader, streamer, logger),
	}
}

func (c *TestClient) UploadFile(file *os.File, uploadAt string, chunkSize int) ([]common.ChunkInfo, error) {
	chunkInfos, err := c.client.UploadFile(context.Background(), file, uploadAt, chunkSize)
	if err != nil {
		return nil, err
	}
	return chunkInfos, nil
}

func (c *TestClient) DownloadFile(path string) (string, error) {
	return c.client.DownloadFile(context.Background(), path)
}

func (c *TestClient) DownloadAllChunks(t *testing.T, chunkInfos []common.ChunkInfo, logger *slog.Logger) error {
	// Validate if the chunks were replicated to the provided nodes and their checksum matches the expectation
	streamerInstance := streaming.NewClientStreamer(config.StreamerConfig{
		ChunkStreamSize: config.DefaultStreamerConfig(true).ChunkStreamSize,
	})
	streamerInstance.Config().WaitReplicas = true // Wait for the final stream with the replicas information

	for _, info := range chunkInfos {
		for _, replica := range info.Replicas {
			if err := downloadChunk(t, info, replica, streamerInstance, logger); err != nil {
				return err
			}
		}
	}

	// dnClientPool.closeAll()

	return nil
}

func downloadChunk(t *testing.T, info common.ChunkInfo, replica *common.NodeInfo, streamerInstance streaming.ClientStreamer, logger *slog.Logger) error {
	dnClient, err := dnClientPool.getOrAddClient(replica)
	if err != nil {
		t.Errorf("failed to get datanode client: %v", err)
	}

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

	if err := streamerInstance.ReceiveChunkStream(context.Background(), stream, buffer, logger, streaming.DownloadChunkStreamParams{
		SessionID:   resp.SessionID,
		ChunkHeader: info.Header,
	}); err != nil {
		t.Errorf("failed to receive chunk stream: %v", err)
	}

	if checksum := chunk.CalculateChecksum(buffer.Bytes()); checksum != info.Header.Checksum {
		t.Errorf("checksum mismatch for %s", info.Header.ID)
	}

	return nil
}

func (c *TestClient) DeleteFile(path string) error {
	return c.client.DeleteFile(context.Background(), path)
}

func (c *TestClient) Close() error {
	return c.client.Close()
}
