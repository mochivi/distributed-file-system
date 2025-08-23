package downloader

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	ic "github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/pkg/client_pool"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/streaming"
	"github.com/mochivi/distributed-file-system/pkg/testutils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// downloaderMocks bundles frequently used mocks for the downloader tests
// keeping the setup concise and readable.
type downloaderMocks struct {
	streamer   *streaming.MockClientStreamer
	clientPool *client_pool.MockClientPool
}

func (m *downloaderMocks) assertExpectations(t *testing.T) {
	m.streamer.AssertExpectations(t)
	m.clientPool.AssertExpectations(t)
}

func TestDownloader_downloadChunk(t *testing.T) {
	logger := logging.NewTestLogger(slog.LevelError, true)

	testCases := []struct {
		name        string
		setupMocks  func(*downloaderMocks, *clients.MockDataNodeClient)
		chunkHeader common.ChunkHeader
		sessionID   common.StreamingSessionID
		expectErr   bool
	}{
		{
			name: "success: chunk downloaded successfully",
			setupMocks: func(mocks *downloaderMocks, mockClient *clients.MockDataNodeClient) {
				streamClient := &testutils.MockStreamClient{}
				mockClient.On("DownloadChunkStream", mock.Anything, mock.Anything).Return(streamClient, nil).Once()

				// streamer config
				mocks.streamer.On("Config").Return(&ic.StreamerConfig{ChunkStreamSize: 1024}).Once()
				mocks.streamer.On("ReceiveChunkStream", mock.Anything, streamClient, mock.AnythingOfType("*bytes.Buffer"), mock.Anything, mock.Anything).Return(nil).Once()
			},
			chunkHeader: common.ChunkHeader{
				ID:       "test_0",
				Index:    0,
				Size:     12,
				Checksum: "test-checksum",
			},
			sessionID: common.NewStreamingSessionID(),
			expectErr: false,
		},
		{
			name: "error: receive stream fails",
			setupMocks: func(mocks *downloaderMocks, mockClient *clients.MockDataNodeClient) {
				streamClient := &testutils.MockStreamClient{}
				mockClient.On("DownloadChunkStream", mock.Anything, mock.Anything).Return(streamClient, nil).Once()

				mocks.streamer.On("Config").Return(&ic.StreamerConfig{ChunkStreamSize: 1024}).Once()
				mocks.streamer.On("ReceiveChunkStream", mock.Anything, streamClient, mock.AnythingOfType("*bytes.Buffer"), mock.Anything, mock.Anything).Return(fmt.Errorf("streaming failed")).Once()
			},
			chunkHeader: common.ChunkHeader{
				ID:       "test_0",
				Index:    0,
				Size:     12,
				Checksum: "test-checksum",
			},
			sessionID: common.NewStreamingSessionID(),
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &downloaderMocks{
				streamer:   new(streaming.MockClientStreamer),
				clientPool: client_pool.NewMockClientPool(),
			}

			mockClient := clients.NewMockDataNodeClient(&common.NodeInfo{ID: "node1"})
			tc.setupMocks(mocks, mockClient)

			downloader := NewDownloader(mocks.streamer, DownloaderConfig{})

			// Build download context
			tempDir := t.TempDir()
			tmpFile, err := os.CreateTemp(tempDir, "download_test_*.tmp")
			assert.NoError(t, err)

			fileInfo := common.FileInfo{Size: 12}
			downloadCtx := NewDownloadContext(context.Background(), nil, []common.ChunkHeader{tc.chunkHeader}, tmpFile, fileInfo, 1, logger)

			err = downloader.downloadChunk(downloadCtx, mockClient, tc.chunkHeader, tc.sessionID)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestDownloader_queueWork(t *testing.T) {
	logger := logging.NewTestLogger(slog.LevelError, true)

	node := &common.NodeInfo{ID: "node1"}

	testCases := []struct {
		name            string
		chunkLocations  []common.ChunkLocation
		nWorkers        int
		expectErr       bool
		expectedWorkCnt int
	}{
		{
			name: "success: single chunk",
			chunkLocations: []common.ChunkLocation{
				{
					ChunkID: "chunk_0",
					Nodes:   []*common.NodeInfo{node},
				},
			},
			nWorkers:        10,
			expectErr:       false,
			expectedWorkCnt: 1,
		},
		{
			name: "success: multiple chunks",
			chunkLocations: []common.ChunkLocation{
				{ChunkID: "chunk_0", Nodes: []*common.NodeInfo{node}},
				{ChunkID: "chunk_1", Nodes: []*common.NodeInfo{node}},
			},
			nWorkers:        10,
			expectErr:       false,
			expectedWorkCnt: 2,
		},
		{
			name: "error: no nodes for chunk",
			chunkLocations: []common.ChunkLocation{
				{ChunkID: "chunk_0", Nodes: []*common.NodeInfo{}},
			},
			nWorkers:        10,
			expectErr:       true,
			expectedWorkCnt: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			downloader := NewDownloader(nil, DownloaderConfig{})

			tempDir := t.TempDir()
			tmpFile, err := os.CreateTemp(tempDir, "download_test_queue_*.tmp")
			assert.NoError(t, err)

			fileInfo := common.FileInfo{Size: 0}

			// Just be careful to not add more chunks what workers during tests
			// As the queue work goroutine will block, as there's nothing consuming it
			// That won't happen outside of tests
			downloadCtx := NewDownloadContext(context.Background(), tc.chunkLocations, nil, tmpFile, fileInfo, tc.nWorkers, logger)

			err = downloader.queueWork(downloadCtx)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				workCnt := 0
				for range downloadCtx.workChan {
					workCnt++
				}
				assert.Equal(t, tc.expectedWorkCnt, workCnt)
			}
		})
	}
}

func TestDownloader_processWork(t *testing.T) {
	logger := logging.NewTestLogger(slog.LevelError, true)

	baseHeader := common.ChunkHeader{
		ID:       "chunk_0",
		Index:    0,
		Size:     12,
		Checksum: "checksum",
	}

	testCases := []struct {
		name       string
		setupMocks func(*downloaderMocks, *clients.MockDataNodeClient)
		expectErr  bool
	}{
		{
			name: "success: first attempt",
			setupMocks: func(mocks *downloaderMocks, mockClient *clients.MockDataNodeClient) {
				// client pool
				mocks.clientPool.On("Len").Return(1).Once()
				downloadReady := common.DownloadReady{NodeReady: common.NodeReady{Accept: true, SessionID: common.NewStreamingSessionID()}, ChunkHeader: baseHeader}
				mocks.clientPool.On("GetRemoveClientWithRetry", mock.Anything).Return(mockClient, downloadReady, nil).Once()

				// client
				streamClient := &testutils.MockStreamClient{}
				mockClient.On("DownloadChunkStream", mock.Anything, mock.Anything).Return(streamClient, nil).Once()

				// streamer
				mocks.streamer.On("Config").Return(&ic.StreamerConfig{ChunkStreamSize: 1024}).Once()
				mocks.streamer.On("ReceiveChunkStream", mock.Anything, streamClient, mock.AnythingOfType("*bytes.Buffer"), mock.Anything, mock.Anything).Return(nil).Once()
			},
			expectErr: false,
		},
		{
			name: "error: all retries fail",
			setupMocks: func(mocks *downloaderMocks, mockClient *clients.MockDataNodeClient) {
				// simulate empty pool (Len returns 0)
				mocks.clientPool.On("Len").Return(0).Once()
			},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &downloaderMocks{
				streamer:   new(streaming.MockClientStreamer),
				clientPool: client_pool.NewMockClientPool(),
			}

			mockClient := clients.NewMockDataNodeClient(&common.NodeInfo{ID: "node1"})
			tc.setupMocks(mocks, mockClient)

			downloader := NewDownloader(mocks.streamer, DownloaderConfig{})

			tempDir := t.TempDir()
			tmpFile, err := os.CreateTemp(tempDir, "download_proc_*.tmp")
			assert.NoError(t, err)

			fileInfo := common.FileInfo{Size: 12}
			downloadCtx := NewDownloadContext(context.Background(), nil, []common.ChunkHeader{baseHeader}, tmpFile, fileInfo, 1, logger)

			work := downloadWork{
				chunkID:    baseHeader.ID,
				clientPool: mocks.clientPool,
			}

			err = downloader.processWork(work, downloadCtx)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mocks.assertExpectations(t)
		})
	}
}
