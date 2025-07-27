package uploader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/client_pool"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/streaming"
	"github.com/mochivi/distributed-file-system/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type uploaderMocks struct {
	streamer   *streaming.MockClientStreamer
	clientPool *client_pool.MockClientPool
}

func (m *uploaderMocks) assertExpectations(t *testing.T) {
	m.streamer.AssertExpectations(t)
	m.clientPool.AssertExpectations(t)
}

func TestUploader_UploadFile(t *testing.T) {
	logger := logging.NewTestLogger(slog.LevelError, true)

	testCases := []struct {
		name         string
		setupMocks   func(*uploaderMocks)
		config       UploaderConfig
		fileContent  string
		chunkIDs     []string
		chunkSize    int
		expectedResp []common.ChunkInfo
		expectErr    bool
	}{
		{
			name: "success: single chunk",
			setupMocks: func(mocks *uploaderMocks) {
				// Mock successful chunk upload
				mockClient := clients.NewMockDataNodeClient(&common.NodeInfo{ID: "node1"})
				mocks.clientPool.On("Close", mock.Anything).Return()
				mocks.streamer.On("SendChunkStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
					[]*common.NodeInfo{{ID: "node1", Status: common.NodeHealthy}}, nil).Once()
				mocks.clientPool.On("GetClientWithRetry", mock.Anything).Return(
					mockClient, "test-session", nil).Once()
				mockClient.On("UploadChunkStream", mock.Anything).Return(&testutils.MockBidiStreamClient{}, nil).Once()
			},
			config:      UploaderConfig{NumWorkers: 2, ChunkRetryCount: 3},
			fileContent: "test content",
			chunkIDs:    []string{"test_0"},
			chunkSize:   1024,
			expectedResp: []common.ChunkInfo{
				{
					Header: common.ChunkHeader{
						ID:       "test_0",
						Index:    0,
						Size:     12, // "test content" length in bytes, all of these characters are represented by 1 byte in UTF-8
						Checksum: chunk.CalculateChecksum([]byte("test content")),
					},
					Replicas: []*common.NodeInfo{{ID: "node1", Status: common.NodeHealthy}},
				},
			},
			expectErr: false,
		},
		{
			name: "success: multiple chunks",
			setupMocks: func(mocks *uploaderMocks) {
				// Mock successful chunk uploads for both chunks
				mocks.clientPool.On("Close", mock.Anything).Return()
				mocks.streamer.On("SendChunkStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
					[]*common.NodeInfo{{ID: "node1", Status: common.NodeHealthy}}, nil).Times(3)
				mockClient := clients.NewMockDataNodeClient(&common.NodeInfo{ID: "node1"})
				mocks.clientPool.On("GetClientWithRetry", mock.Anything).Return(
					mockClient, "test-session", nil).Times(3)
				mockClient.On("UploadChunkStream", mock.Anything).Return(&testutils.MockBidiStreamClient{}, nil).Times(3)
			},
			config: UploaderConfig{
				NumWorkers:      2,
				ChunkRetryCount: 3,
			},
			fileContent: "aaaabbbbcccc",
			chunkIDs:    []string{"test_0", "test_1", "test_2"},
			chunkSize:   4,
			expectedResp: []common.ChunkInfo{
				{
					Header: common.ChunkHeader{
						ID:       "test_0",
						Index:    0,
						Size:     4,
						Checksum: chunk.CalculateChecksum([]byte("aaaa")),
					},
					Replicas: []*common.NodeInfo{{ID: "node1", Status: common.NodeHealthy}},
				},
				{
					Header: common.ChunkHeader{
						ID:       "test_1",
						Index:    1,
						Size:     4,
						Checksum: chunk.CalculateChecksum([]byte("bbbb")),
					},
					Replicas: []*common.NodeInfo{{ID: "node1", Status: common.NodeHealthy}},
				},
				{
					Header: common.ChunkHeader{
						ID:       "test_2",
						Index:    2,
						Size:     4,
						Checksum: chunk.CalculateChecksum([]byte("cccc")),
					},
					Replicas: []*common.NodeInfo{{ID: "node1", Status: common.NodeHealthy}},
				},
			},
			expectErr: false,
		},

		{
			name: "error: streaming fails",
			setupMocks: func(mocks *uploaderMocks) {
				// Mock streaming failure
				mockClient := clients.NewMockDataNodeClient(&common.NodeInfo{ID: "node1"})
				mocks.clientPool.On("Close", mock.Anything).Return()
				mocks.streamer.On("SendChunkStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
					nil, errors.New("streaming failed")).Times(3)
				mocks.clientPool.On("GetClientWithRetry", mock.Anything).Return(
					mockClient, "test-session", nil).Times(3)
				mockClient.On("UploadChunkStream", mock.Anything).Return(&testutils.MockBidiStreamClient{}, nil).Times(3)
			},
			config: UploaderConfig{
				NumWorkers:      2,
				ChunkRetryCount: 3,
			},
			fileContent:  "test content",
			chunkIDs:     []string{"test_0"},
			chunkSize:    1024,
			expectedResp: nil,
			expectErr:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &uploaderMocks{
				streamer:   new(streaming.MockClientStreamer),
				clientPool: client_pool.NewMockClientPool(),
			}
			tc.setupMocks(mocks)

			uploader := NewUploader(mocks.streamer, logger, tc.config)

			// Create a reader from the file content
			reader := io.NopCloser(strings.NewReader(tc.fileContent))

			resp, err := uploader.UploadFile(context.Background(), reader, mocks.clientPool, tc.chunkIDs, logger, tc.chunkSize)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, resp, len(tc.expectedResp))

				// Sort responses by chunk ID for consistent comparison
				for _, expected := range tc.expectedResp {
					found := false
					for _, actual := range resp {
						if actual.Header.ID == expected.Header.ID {
							assert.Equal(t, expected.Header.ID, actual.Header.ID)
							assert.Equal(t, expected.Header.Index, actual.Header.Index)
							assert.Equal(t, expected.Header.Size, actual.Header.Size)
							assert.Equal(t, expected.Header.Checksum, actual.Header.Checksum)
							assert.Len(t, actual.Replicas, len(expected.Replicas))
							found = true
							break
						}
					}
					assert.True(t, found, "Expected chunk %s not found in response", expected.Header.ID)
				}
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestUploader_QueueWork(t *testing.T) {
	logger := logging.NewTestLogger(slog.LevelError, true)

	testCases := []struct {
		name        string
		fileContent string
		chunkIDs    []string
		chunkSize   int
		expectErr   bool
	}{
		{
			name:        "success: single chunk",
			fileContent: "test content",
			chunkIDs:    []string{"test_0"},
			chunkSize:   1024,
			expectErr:   false,
		},
		{
			name:        "success: multiple chunks",
			fileContent: "chunk1 content chunk2 content",
			chunkIDs:    []string{"test_0", "test_1"},
			chunkSize:   10,
			expectErr:   false,
		},
		{
			name:        "success: empty file",
			fileContent: "",
			chunkIDs:    []string{"test_0"},
			chunkSize:   1024,
			expectErr:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			uploader := NewUploader(nil, logger, UploaderConfig{})

			// Create a mock client pool
			mockClientPool := client_pool.NewMockClientPool()

			// Create upload context
			uploadCtx := NewUploadContext(context.Background(), tc.chunkIDs, tc.chunkSize, mockClientPool, logger)

			// Create a reader from the file content
			reader := io.NopCloser(strings.NewReader(tc.fileContent))

			err := uploader.QueueWork(uploadCtx, reader, tc.chunkSize)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				// Verify that work was queued
				workCount := 0
				for work := range uploadCtx.workChan {
					assert.NotEmpty(t, work.chunkHeader.ID)
					assert.NotNil(t, work.data)
					workCount++
				}

				expectedWorkCount := len(tc.chunkIDs)
				assert.Equal(t, expectedWorkCount, workCount)
			}
		})
	}
}

func TestUploader_processWork(t *testing.T) {
	logger := logging.NewTestLogger(slog.LevelError, true)

	testCases := []struct {
		name         string
		setupMocks   func(*uploaderMocks)
		config       UploaderConfig
		work         UploaderWork
		expectErr    bool
		expectedInfo *common.ChunkInfo
	}{
		{
			name: "success: first attempt succeeds",
			setupMocks: func(mocks *uploaderMocks) {
				mockClient := clients.NewMockDataNodeClient(&common.NodeInfo{ID: "node1"})
				mocks.clientPool.On("GetClientWithRetry", mock.Anything).Return(
					mockClient, "test-session", nil).Once()
				mockClient.On("UploadChunkStream", mock.Anything).Return(&testutils.MockBidiStreamClient{}, nil).Once()
				mocks.streamer.On("SendChunkStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
					[]*common.NodeInfo{{ID: "node1", Status: common.NodeHealthy}}, nil).Once()
			},
			config: UploaderConfig{
				NumWorkers:      2,
				ChunkRetryCount: 3,
			},
			work: UploaderWork{
				chunkHeader: common.ChunkHeader{
					ID:       "test_0",
					Index:    0,
					Size:     12,
					Checksum: "test-checksum",
				},
				data: []byte("test content"),
			},
			expectErr: false,
			expectedInfo: &common.ChunkInfo{
				Header: common.ChunkHeader{
					ID:       "test_0",
					Index:    0,
					Size:     12,
					Checksum: "test-checksum",
				},
				Replicas: []*common.NodeInfo{{ID: "node1", Status: common.NodeHealthy}},
			},
		},
		{
			name: "success: retry succeeds",
			setupMocks: func(mocks *uploaderMocks) {
				// First attempt fails, second succeeds
				mockClient := clients.NewMockDataNodeClient(&common.NodeInfo{ID: "node1"})
				mocks.clientPool.On("GetClientWithRetry", mock.Anything).Return(mockClient, "test-session", nil).Twice()
				mockClient.On("UploadChunkStream", mock.Anything).Return(&testutils.MockBidiStreamClient{}, nil).Twice()

				// Attempt 1 fails
				mocks.streamer.On("SendChunkStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
					nil, errors.New("first attempt failed")).Once()

				// Attempt 2 succeeds
				mocks.streamer.On("SendChunkStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
					[]*common.NodeInfo{{ID: "node1", Status: common.NodeHealthy}}, nil).Once()
			},
			config: UploaderConfig{
				NumWorkers:      2,
				ChunkRetryCount: 3,
			},
			work: UploaderWork{
				chunkHeader: common.ChunkHeader{
					ID:       "test_0",
					Index:    0,
					Size:     12,
					Checksum: "test-checksum",
				},
				data: []byte("test content"),
			},
			expectErr: false,
			expectedInfo: &common.ChunkInfo{
				Header: common.ChunkHeader{
					ID:       "test_0",
					Index:    0,
					Size:     12,
					Checksum: "test-checksum",
				},
				Replicas: []*common.NodeInfo{{ID: "node1", Status: common.NodeHealthy}},
			},
		},
		{
			name: "error: all retries fail",
			setupMocks: func(mocks *uploaderMocks) {
				// All attempts fail
				mockClient := clients.NewMockDataNodeClient(&common.NodeInfo{ID: "node1"})
				mocks.clientPool.On("GetClientWithRetry", mock.Anything).Return(
					mockClient, "test-session", nil).Times(3)
				mockClient.On("UploadChunkStream", mock.Anything).Return(&testutils.MockBidiStreamClient{}, nil).Times(3)
				mocks.streamer.On("SendChunkStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
					nil, errors.New("streaming failed")).Times(3)
			},
			config: UploaderConfig{
				NumWorkers:      2,
				ChunkRetryCount: 3,
			},
			work: UploaderWork{
				chunkHeader: common.ChunkHeader{
					ID:       "test_0",
					Index:    0,
					Size:     12,
					Checksum: "test-checksum",
				},
				data: []byte("test content"),
			},
			expectErr:    true,
			expectedInfo: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &uploaderMocks{
				streamer:   new(streaming.MockClientStreamer),
				clientPool: client_pool.NewMockClientPool(),
			}
			tc.setupMocks(mocks)

			uploader := NewUploader(mocks.streamer, logger, tc.config)

			// Create upload context
			uploadCtx := NewUploadContext(context.Background(), []string{"test_0"}, 1024, mocks.clientPool, logger)

			err := uploader.processWork(tc.work, mocks.clientPool, uploadCtx)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				// Verify chunk info was added
				chunkInfos := uploadCtx.chunkInfos.getChunkInfos()
				assert.Len(t, chunkInfos, 1)

				actual := chunkInfos[0]
				assert.Equal(t, tc.expectedInfo.Header.ID, actual.Header.ID)
				assert.Equal(t, tc.expectedInfo.Header.Index, actual.Header.Index)
				assert.Equal(t, tc.expectedInfo.Header.Size, actual.Header.Size)
				assert.Equal(t, tc.expectedInfo.Header.Checksum, actual.Header.Checksum)
				assert.Len(t, actual.Replicas, len(tc.expectedInfo.Replicas))
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestUploader_uploadChunk(t *testing.T) {
	logger := logging.NewTestLogger(slog.LevelError, true)

	testCases := []struct {
		name               string
		setupMocks         func(*uploaderMocks, *clients.MockDataNodeClient)
		work               UploaderWork
		streamingSessionID string
		expectErr          bool
		expectedNodes      []*common.NodeInfo
	}{
		{
			name: "success: chunk uploaded successfully",
			setupMocks: func(mocks *uploaderMocks, mockClient *clients.MockDataNodeClient) {
				mockClient.On("UploadChunkStream", mock.Anything).Return(&testutils.MockBidiStreamClient{}, nil).Once()
				mocks.streamer.On("SendChunkStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
					[]*common.NodeInfo{{ID: "node1", Status: common.NodeHealthy}}, nil).Once()
			},
			work: UploaderWork{
				chunkHeader: common.ChunkHeader{
					ID:       "test_0",
					Index:    0,
					Size:     12,
					Checksum: "test-checksum",
				},
				data: []byte("test content"),
			},
			streamingSessionID: "test-session",
			expectErr:          false,
			expectedNodes:      []*common.NodeInfo{{ID: "node1", Status: common.NodeHealthy}},
		},
		{
			name: "error: streaming fails",
			setupMocks: func(mocks *uploaderMocks, mockClient *clients.MockDataNodeClient) {
				mockClient.On("UploadChunkStream", mock.Anything).Return(&testutils.MockBidiStreamClient{}, nil).Once()
				mocks.streamer.On("SendChunkStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
					nil, errors.New("streaming failed")).Once()
			},
			work: UploaderWork{
				chunkHeader: common.ChunkHeader{
					ID:       "test_0",
					Index:    0,
					Size:     12,
					Checksum: "test-checksum",
				},
				data: []byte("test content"),
			},
			streamingSessionID: "test-session",
			expectErr:          true,
			expectedNodes:      nil,
		},
		{
			name: "error: no replicated nodes returned",
			setupMocks: func(mocks *uploaderMocks, mockClient *clients.MockDataNodeClient) {
				mockClient.On("UploadChunkStream", mock.Anything).Return(&testutils.MockBidiStreamClient{}, nil).Once()
				mocks.streamer.On("SendChunkStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
					nil, nil).Once()
			},
			work: UploaderWork{
				chunkHeader: common.ChunkHeader{
					ID:       "test_0",
					Index:    0,
					Size:     12,
					Checksum: "test-checksum",
				},
				data: []byte("test content"),
			},
			streamingSessionID: "test-session",
			expectErr:          true,
			expectedNodes:      nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &uploaderMocks{
				streamer:   new(streaming.MockClientStreamer),
				clientPool: client_pool.NewMockClientPool(),
			}
			mockClient := clients.NewMockDataNodeClient(&common.NodeInfo{ID: "node1"})
			tc.setupMocks(mocks, mockClient)

			uploader := NewUploader(mocks.streamer, logger, UploaderConfig{})

			// Create upload context
			uploadCtx := NewUploadContext(context.Background(), []string{"test_0"}, 1024, mocks.clientPool, logger)

			nodes, err := uploader.uploadChunk(tc.work, mockClient, "test-session", uploadCtx)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedNodes, nodes)
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestUploader_makeChunk(t *testing.T) {
	testCases := []struct {
		name        string
		fileContent string
		chunkID     string
		index       int
		chunkSize   int
		expected    struct {
			header common.ChunkHeader
			data   []byte
			err    bool
		}
	}{
		{
			name:        "success: normal chunk",
			fileContent: "test content",
			chunkID:     "test_0",
			index:       0,
			chunkSize:   1024,
			expected: struct {
				header common.ChunkHeader
				data   []byte
				err    bool
			}{
				header: common.ChunkHeader{
					ID:       "test_0",
					Index:    0,
					Size:     12,
					Checksum: chunk.CalculateChecksum([]byte("test content")),
				},
				data: []byte("test content"),
				err:  false,
			},
		},
		{
			name:        "success: chunk smaller than buffer",
			fileContent: "short",
			chunkID:     "test_0",
			index:       0,
			chunkSize:   1024,
			expected: struct {
				header common.ChunkHeader
				data   []byte
				err    bool
			}{
				header: common.ChunkHeader{
					ID:       "test_0",
					Index:    0,
					Size:     5,
					Checksum: chunk.CalculateChecksum([]byte("short")),
				},
				data: []byte("short"),
				err:  false,
			},
		},
		{
			name:        "success: empty content",
			fileContent: "",
			chunkID:     "test_0",
			index:       0,
			chunkSize:   1024,
			expected: struct {
				header common.ChunkHeader
				data   []byte
				err    bool
			}{
				header: common.ChunkHeader{
					ID:       "test_0",
					Index:    0,
					Size:     0,
					Checksum: chunk.CalculateChecksum([]byte{}),
				},
				data: []byte{},
				err:  false,
			},
		},
		{
			name:        "success: second chunk",
			fileContent: "bbbb",
			chunkID:     "test_1",
			index:       1,
			chunkSize:   4,
			expected: struct {
				header common.ChunkHeader
				data   []byte
				err    bool
			}{
				header: common.ChunkHeader{
					ID:       "test_1",
					Index:    1,
					Size:     4,
					Checksum: chunk.CalculateChecksum([]byte("bbbb")),
				},
				data: []byte("bbbb"),
				err:  false,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader := strings.NewReader(tc.fileContent)

			header, data, err := makeChunk(reader, tc.chunkID, tc.index, tc.chunkSize)

			if tc.expected.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected.header.ID, header.ID)
				assert.Equal(t, tc.expected.header.Index, header.Index)
				assert.Equal(t, tc.expected.header.Size, header.Size)
				assert.Equal(t, tc.expected.header.Checksum, header.Checksum)
				assert.Equal(t, tc.expected.data, data)
			}
		})
	}
}

func TestUploader_startWorkers(t *testing.T) {
	logger := logging.NewTestLogger(slog.LevelError, true)

	testCases := []struct {
		name       string
		setupMocks func(*uploaderMocks)
		config     UploaderConfig
		workItems  []UploaderWork
		expectErr  bool
	}{
		{
			name: "success: workers process all work",
			setupMocks: func(mocks *uploaderMocks) {
				mockClient := clients.NewMockDataNodeClient(&common.NodeInfo{ID: "node1"})
				mocks.clientPool.On("GetClientWithRetry", mock.Anything).Return(
					mockClient, "test-session", nil).Times(2)
				mockClient.On("UploadChunkStream", mock.Anything).Return(&testutils.MockBidiStreamClient{}, nil).Times(2)
				mocks.streamer.On("SendChunkStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
					[]*common.NodeInfo{{ID: "node1", Status: common.NodeHealthy}}, nil).Times(2)
			},
			config: UploaderConfig{
				NumWorkers:      2,
				ChunkRetryCount: 1,
			},
			workItems: []UploaderWork{
				{
					chunkHeader: common.ChunkHeader{ID: "test_0", Index: 0, Size: 5, Checksum: "checksum1"},
					data:        []byte("chunk1"),
				},
				{
					chunkHeader: common.ChunkHeader{ID: "test_1", Index: 1, Size: 5, Checksum: "checksum2"},
					data:        []byte("chunk2"),
				},
			},
			expectErr: false,
		},
		{
			name: "error: worker encounters error",
			setupMocks: func(mocks *uploaderMocks) {
				mockClient := clients.NewMockDataNodeClient(&common.NodeInfo{ID: "node1"})
				mocks.clientPool.On("GetClientWithRetry", mock.Anything).Return(
					mockClient, "test-session", nil).Once()
				mockClient.On("UploadChunkStream", mock.Anything).Return(&testutils.MockBidiStreamClient{}, nil).Once()
				mocks.streamer.On("SendChunkStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
					nil, errors.New("streaming failed")).Once()
			},
			config: UploaderConfig{
				NumWorkers:      1,
				ChunkRetryCount: 1,
			},
			workItems: []UploaderWork{
				{
					chunkHeader: common.ChunkHeader{ID: "test_0", Index: 0, Size: 5, Checksum: "checksum1"},
					data:        []byte("chunk1"),
				},
			},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &uploaderMocks{
				streamer:   new(streaming.MockClientStreamer),
				clientPool: client_pool.NewMockClientPool(),
			}
			tc.setupMocks(mocks)

			uploader := NewUploader(mocks.streamer, logger, tc.config)

			// Create upload context
			uploadCtx := NewUploadContext(context.Background(), []string{"test_0", "test_1"}, 1024, mocks.clientPool, logger)

			// Queue work items
			for _, work := range tc.workItems {
				uploadCtx.workChan <- work
			}
			close(uploadCtx.workChan)

			// Start workers
			uploader.startWorkers(uploadCtx, mocks.clientPool)

			// Wait for completion
			err := uploader.waitForCompletion(uploadCtx)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestUploader_ConcurrentWorkers(t *testing.T) {
	logger := logging.NewTestLogger(slog.LevelError, true)

	testCases := []struct {
		name       string
		setupMocks func(*uploaderMocks)
		numWorkers int
		numChunks  int
		expectErr  bool
	}{
		{
			name: "success: multiple workers process chunks concurrently",
			setupMocks: func(mocks *uploaderMocks) {
				mockClient := clients.NewMockDataNodeClient(&common.NodeInfo{ID: "node1"})
				// Each chunk needs a client and stream
				mocks.clientPool.On("GetClientWithRetry", mock.Anything).Return(
					mockClient, "test-session", nil).Times(5)
				mockClient.On("UploadChunkStream", mock.Anything).Return(&testutils.MockBidiStreamClient{}, nil).Times(5)
				mocks.streamer.On("SendChunkStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
					[]*common.NodeInfo{{ID: "node1", Status: common.NodeHealthy}}, nil).Times(5)
			},
			numWorkers: 3,
			numChunks:  5,
			expectErr:  false,
		},
		{
			name: "success: more workers than chunks",
			setupMocks: func(mocks *uploaderMocks) {
				mockClient := clients.NewMockDataNodeClient(&common.NodeInfo{ID: "node1"})
				mocks.clientPool.On("GetClientWithRetry", mock.Anything).Return(
					mockClient, "test-session", nil).Times(2)
				mockClient.On("UploadChunkStream", mock.Anything).Return(&testutils.MockBidiStreamClient{}, nil).Times(2)
				mocks.streamer.On("SendChunkStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
					[]*common.NodeInfo{{ID: "node1", Status: common.NodeHealthy}}, nil).Times(2)
			},
			numWorkers: 5,
			numChunks:  2,
			expectErr:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &uploaderMocks{
				streamer:   new(streaming.MockClientStreamer),
				clientPool: client_pool.NewMockClientPool(),
			}
			tc.setupMocks(mocks)

			uploader := NewUploader(mocks.streamer, logger, UploaderConfig{
				NumWorkers:      tc.numWorkers,
				ChunkRetryCount: 1,
			})

			// Create upload context
			chunkIDs := make([]string, tc.numChunks)
			for i := 0; i < tc.numChunks; i++ {
				chunkIDs[i] = fmt.Sprintf("test_%d", i)
			}
			uploadCtx := NewUploadContext(context.Background(), chunkIDs, 1024, mocks.clientPool, logger)

			// Queue work items
			for i := 0; i < tc.numChunks; i++ {
				work := UploaderWork{
					chunkHeader: common.ChunkHeader{
						ID:       fmt.Sprintf("test_%d", i),
						Index:    i,
						Size:     5,
						Checksum: fmt.Sprintf("checksum_%d", i),
					},
					data: []byte(fmt.Sprintf("chunk%d", i)),
				}
				uploadCtx.workChan <- work
			}
			close(uploadCtx.workChan)

			// Start workers
			uploader.startWorkers(uploadCtx, mocks.clientPool)

			// Wait for completion
			err := uploader.waitForCompletion(uploadCtx)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				// Verify all chunks were processed
				chunkInfos := uploadCtx.chunkInfos.getChunkInfos()
				assert.Len(t, chunkInfos, tc.numChunks)
			}

			mocks.assertExpectations(t)
		})
	}
}
