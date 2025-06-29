package common_test

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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

// MockBidiStream is a mock for the grpc.BidiStreamingClient interface.
type MockBidiStream struct {
	grpc.ClientStream
	mock.Mock
}

func (s *MockBidiStream) Send(m *proto.ChunkDataStream) error {
	args := s.Called(m)
	return args.Error(0)
}

func (s *MockBidiStream) Recv() (*proto.ChunkDataAck, error) {
	args := s.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*proto.ChunkDataAck), args.Error(1)
}

func (s *MockBidiStream) CloseSend() error {
	args := s.Called()
	return args.Error(0)
}

type MockServerStream struct {
	grpc.ServerStreamingClient[proto.ChunkDataStream]
	mock.Mock
}

func (s *MockServerStream) Recv() (*proto.ChunkDataStream, error) {
	args := s.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*proto.ChunkDataStream), args.Error(1)
}

func TestStreamer_SendChunkStream(t *testing.T) {
	data := []byte("this is some test data")
	checksum := fmt.Sprintf("%x", sha256.Sum256(data))
	defaultParams := common.UploadChunkStreamParams{
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
		params           common.UploadChunkStreamParams
		setupMocks       func(*MockBidiStream)
		expectErr        bool
		expectedReplicas []*common.DataNodeInfo
	}{
		{
			name:   "success",
			config: config.DefaultStreamerConfig(true),
			params: defaultParams,
			setupMocks: func(s *MockBidiStream) {
				s.On("Send", mock.AnythingOfType("*proto.ChunkDataStream")).Return(nil).Once()
				s.On("Recv").Return(&proto.ChunkDataAck{
					Success:       true,
					BytesReceived: int64(len(data)),
					ReadyForNext:  true,
				}, nil).Once()
				s.On("CloseSend").Return(nil).Once()
				s.On("Recv").Return(&proto.ChunkDataAck{Success: true}, nil).Once()

				replicas := []*common.DataNodeInfo{{ID: "node1", Host: "localhost", Port: 9001}}
				var protoReplicas []*proto.DataNodeInfo
				for _, r := range replicas {
					protoReplicas = append(protoReplicas, r.ToProto())
				}
				s.On("Recv").Return(&proto.ChunkDataAck{
					Success:  true,
					Replicas: protoReplicas,
				}, nil).Once()
			},
			expectErr: false,
			expectedReplicas: []*common.DataNodeInfo{
				{ID: "node1", Host: "localhost", Port: 9001},
			},
		},
		{
			name:   "success: no replicas expected",
			config: config.DefaultStreamerConfig(false), // WaitReplicas is false
			params: defaultParams,
			setupMocks: func(s *MockBidiStream) {
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
			setupMocks: func(s *MockBidiStream) {
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
			setupMocks: func(s *MockBidiStream) {
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
			params: common.UploadChunkStreamParams{
				SessionID: "test_session",
				ChunkHeader: common.ChunkHeader{
					ID:       "test_chunk",
					Checksum: "invalid_checksum",
				},
				Data: data,
			},
			setupMocks: func(s *MockBidiStream) {
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
			setupMocks: func(s *MockBidiStream) {
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
			streamer := common.NewStreamer(tc.config)
			logger := logging.NewTestLogger(slog.LevelError)
			mockStream := &MockBidiStream{}
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
		params       common.DownloadChunkStreamParams
		setupMocks   func(*MockServerStream)
		expectErr    bool
		expectedData []byte
	}{
		{
			name:   "success",
			config: config.DefaultStreamerConfig(false),
			params: common.DownloadChunkStreamParams{
				SessionID: "test_session",
				ChunkHeader: common.ChunkHeader{
					ID:       "test_chunk",
					Checksum: "test_checksum",
				},
			},
			setupMocks: func(s *MockServerStream) {
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
			params: common.DownloadChunkStreamParams{
				SessionID: "test_session",
			},
			setupMocks: func(s *MockServerStream) {
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
			streamer := common.NewStreamer(tc.config)
			logger := logging.NewTestLogger(slog.LevelError)
			mockStream := &MockServerStream{}
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
