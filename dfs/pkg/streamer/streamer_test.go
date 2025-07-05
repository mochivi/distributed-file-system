package streamer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"testing"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/mochivi/distributed-file-system/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestStreamer_SendChunkStream(t *testing.T) {
	data := []byte("this is some test data")
	checksum := fmt.Sprintf("%x", sha256.Sum256(data))
	defaultParams := UploadChunkStreamParams{
		SessionID: "test_session",
		ChunkHeader: common.ChunkHeader{
			ID:       "test_chunk",
			Checksum: checksum,
		},
		Data: data,
	}

	testCases := []struct {
		name             string
		config           config.StreamerConfig
		params           UploadChunkStreamParams
		setupMocks       func(*testutils.MockBidiStreamClient)
		expectErr        bool
		expectedReplicas []*common.NodeInfo
	}{
		{
			name:   "success",
			config: config.DefaultStreamerConfig(true),
			params: defaultParams,
			setupMocks: func(s *testutils.MockBidiStreamClient) {
				s.On("Send", mock.AnythingOfType("*proto.ChunkDataStream")).Return(nil).Once()
				s.On("Recv").Return(&proto.ChunkDataAck{
					Success:       true,
					BytesReceived: int64(len(data)),
					ReadyForNext:  true,
				}, nil).Once()
				s.On("CloseSend").Return(nil).Once()
				s.On("Recv").Return(&proto.ChunkDataAck{Success: true}, nil).Once()

				replicas := []*common.NodeInfo{{ID: "node1", Host: "localhost", Port: 9001}}
				var protoReplicas []*proto.NodeInfo
				for _, r := range replicas {
					protoReplicas = append(protoReplicas, r.ToProto())
				}
				s.On("Recv").Return(&proto.ChunkDataAck{
					Success:  true,
					Replicas: protoReplicas,
				}, nil).Once()
			},
			expectErr: false,
			expectedReplicas: []*common.NodeInfo{
				{ID: "node1", Host: "localhost", Port: 9001},
			},
		},
		{
			name:   "success: no replicas expected",
			config: config.DefaultStreamerConfig(false), // WaitReplicas is false
			params: defaultParams,
			setupMocks: func(s *testutils.MockBidiStreamClient) {
				s.On("Send", mock.AnythingOfType("*proto.ChunkDataStream")).Return(nil).Once()
				s.On("Recv").Return(&proto.ChunkDataAck{
					Success:       true,
					BytesReceived: int64(len(data)),
				}, nil).Once()
				s.On("CloseSend").Return(nil).Once()
				s.On("Recv").Return(&proto.ChunkDataAck{Success: true}, nil).Once()
			},
			expectErr:        false,
			expectedReplicas: nil,
		},
		{
			name:   "error: send error",
			config: config.DefaultStreamerConfig(true),
			params: defaultParams,
			setupMocks: func(s *testutils.MockBidiStreamClient) {
				s.On("Send", mock.AnythingOfType("*proto.ChunkDataStream")).Return(fmt.Errorf("network error")).Once()
			},
			expectErr: true,
		},
		{
			name: "error: unsuccessful response",
			config: config.StreamerConfig{ // Override retries to prevent more mock calls
				MaxChunkRetries: 1,
				ChunkStreamSize: 256 * 1024,
				WaitReplicas:    false,
			},
			params: defaultParams,
			setupMocks: func(s *testutils.MockBidiStreamClient) {
				s.On("Send", mock.AnythingOfType("*proto.ChunkDataStream")).Return(nil).Once()
				s.On("Recv").Return(&proto.ChunkDataAck{
					Success: false,
					Message: "unsuccessful",
				}, nil).Once()
			},
			expectErr:        true,
			expectedReplicas: nil,
		},
		{
			name:   "error: checksum mismatch",
			config: config.DefaultStreamerConfig(true),
			params: UploadChunkStreamParams{
				SessionID: "test_session",
				ChunkHeader: common.ChunkHeader{
					ID:       "test_chunk",
					Checksum: "invalid_checksum",
				},
				Data: data,
			},
			setupMocks: func(s *testutils.MockBidiStreamClient) {
				s.On("Send", mock.AnythingOfType("*proto.ChunkDataStream")).Return(nil).Once()
				s.On("Recv").Return(&proto.ChunkDataAck{
					Success:       true,
					BytesReceived: int64(len(data)),
					ReadyForNext:  true,
				}, nil).Once()
				s.On("CloseSend").Return(nil).Once()
				s.On("Recv").Return(&proto.ChunkDataAck{Success: true}, nil).Once()
			},
			expectErr: true,
		},
		{
			name: "error: incorrect amount of bytes received",
			config: config.StreamerConfig{ // Override retries to prevent more mock calls
				MaxChunkRetries: 1,
				ChunkStreamSize: 256 * 1024,
				WaitReplicas:    false,
			},
			params: defaultParams,
			setupMocks: func(s *testutils.MockBidiStreamClient) {
				s.On("Send", mock.AnythingOfType("*proto.ChunkDataStream")).Return(nil).Once()
				s.On("Recv").Return(&proto.ChunkDataAck{
					Success:       true,
					BytesReceived: int64(len(data)) + 1,
					ReadyForNext:  true,
				}, nil).Once()
			},
			expectErr:        true,
			expectedReplicas: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup
			streamer := NewStreamer(tc.config)
			logger := logging.NewTestLogger(slog.LevelError)
			mockStream := &testutils.MockBidiStreamClient{}
			tc.setupMocks(mockStream)

			// Execute
			returnedReplicas, err := streamer.SendChunkStream(context.Background(), mockStream, logger, tc.params)

			// Assert
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.expectedReplicas, returnedReplicas)
			mockStream.AssertExpectations(t)
		})
	}
}

func TestStreamer_ReceiveChunkStream(t *testing.T) {
	testCases := []struct {
		name         string
		config       config.StreamerConfig
		params       DownloadChunkStreamParams
		setupMocks   func(*testutils.MockStreamClient)
		expectErr    bool
		expectedData []byte
	}{
		{
			name:   "success",
			config: config.DefaultStreamerConfig(false),
			params: DownloadChunkStreamParams{
				SessionID: "test_session",
				ChunkHeader: common.ChunkHeader{
					ID:       "test_chunk",
					Checksum: "test_checksum",
				},
			},
			setupMocks: func(s *testutils.MockStreamClient) {
				s.On("Recv").Return(&proto.ChunkDataStream{
					Data:    []byte("first stream frame for chunk"),
					IsFinal: true,
					ChunkId: "test_chunk",
				}, nil).Once()
			},
			expectErr: false,
		},
		{
			name:   "success, multiple stream frames",
			config: config.DefaultStreamerConfig(true),
			params: DownloadChunkStreamParams{
				SessionID: "test_session",
			},
			setupMocks: func(s *testutils.MockStreamClient) {
				s.On("Recv").Return(&proto.ChunkDataStream{
					Data:    []byte("first stream frame for chunk"),
					IsFinal: false,
					ChunkId: "test_chunk",
				}, nil).Once()
				s.On("Recv").Return(&proto.ChunkDataStream{
					Data:    []byte("second stream frame for chunk"),
					IsFinal: false,
					ChunkId: "test_chunk",
				}, nil).Once()
				s.On("Recv").Return(&proto.ChunkDataStream{
					Data:    []byte("third and final stream frame for chunk"),
					IsFinal: true,
					ChunkId: "test_chunk",
				}, nil).Once()
			},
			expectErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup
			streamer := NewStreamer(tc.config)
			logger := logging.NewTestLogger(slog.LevelError)
			mockStream := &testutils.MockStreamClient{}
			tc.setupMocks(mockStream)

			buffer := bytes.NewBuffer(nil)

			// Execute
			err := streamer.ReceiveChunkStream(context.Background(), mockStream, buffer, logger, tc.params)

			// Assert
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockStream.AssertExpectations(t)
		})
	}
}
