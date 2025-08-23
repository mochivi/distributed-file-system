package streaming

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"io"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/mochivi/distributed-file-system/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/codes"
)

type serverStreamerMocks struct {
	sessionManager *MockStreamingSessionManager
	stream         *testutils.MockBidiStreamServer
}

func (m *serverStreamerMocks) assertExpectations(t *testing.T) {
	m.sessionManager.AssertExpectations(t)
	m.stream.AssertExpectations(t)
}

func TestServerStreamer_HandleFirstChunk(t *testing.T) {
	testCases := []struct {
		name                string
		setupMocks          func(*serverStreamerMocks)
		chunkData           *proto.ChunkDataStream
		expectErr           bool
		expectedSentinelErr error
		expectedCustomErr   error
		expectedStatus      codes.Code
	}{
		{
			name: "success: valid first chunk",
			setupMocks: func(mocks *serverStreamerMocks) {
				chunk := &proto.ChunkDataStream{
					SessionId: "test-session",
					ChunkId:   "test-chunk",
					Data:      []byte("test data"),
					Offset:    0,
					IsFinal:   false,
				}
				mocks.stream.On("Recv").Return(chunk, nil).Once()

				session := &streamingSession{
					SessionID: common.StreamingSessionID("test-session"),
					ChunkHeader: common.ChunkHeader{
						ID:       "test-chunk",
						Index:    0,
						Size:     1024,
						Checksum: "test-checksum",
					},
					Status:          SessionActive,
					runningChecksum: sha256.New(),
				}
				mocks.sessionManager.On("GetSession", common.StreamingSessionID("test-session")).Return(session, nil).Once()

				successfulAck := &common.ChunkDataAck{
					SessionID:     common.StreamingSessionID(chunk.SessionId),
					Success:       true,
					BytesReceived: 9,
					ReadyForNext:  true, // TODO: Requires flushing to be setup to work
				}
				mocks.stream.On("Send", successfulAck.ToProto()).Return(nil).Once()
			},
			chunkData: &proto.ChunkDataStream{
				SessionId: "test-session",
				ChunkId:   "test-chunk",
				Data:      []byte("test data"),
				Offset:    0,
				IsFinal:   false,
			},
			expectErr: false,
		},
		{
			name: "error: EOF on receive",
			setupMocks: func(mocks *serverStreamerMocks) {
				mocks.stream.On("Recv").Return(nil, io.EOF).Once()
			},
			chunkData:           nil,
			expectErr:           true,
			expectedSentinelErr: io.EOF,
		},
		{
			name: "error: receive error",
			setupMocks: func(mocks *serverStreamerMocks) {
				mocks.stream.On("Recv").Return(nil, errors.New("network error")).Once()
			},
			chunkData:         nil,
			expectErr:         true,
			expectedCustomErr: NewStreamReceiveError("", 0, errors.New("network error")),
		},
		{
			name: "error: session not found",
			setupMocks: func(mocks *serverStreamerMocks) {
				chunk := &proto.ChunkDataStream{
					SessionId: "test-session",
					ChunkId:   "test-chunk",
					Data:      []byte("test data"),
					Offset:    0,
					IsFinal:   false,
				}
				mocks.stream.On("Recv").Return(chunk, nil).Once()
				mocks.sessionManager.On("GetSession", common.StreamingSessionID("test-session")).Return(nil, ErrSessionNotFound).Once()
			},
			chunkData:         nil,
			expectErr:         true,
			expectedCustomErr: NewStreamReceiveError("test-session", 0, ErrSessionNotFound),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverStreamerMocks{
				sessionManager: new(MockStreamingSessionManager),
				stream:         new(testutils.MockBidiStreamServer),
			}
			tc.setupMocks(mocks)

			streamer := &serverStreamer{
				sessionManager: mocks.sessionManager,
			}

			session, err := streamer.HandleFirstChunkFrame(mocks.stream)

			if tc.expectErr {
				assert.Error(t, err)
				if tc.expectedCustomErr != nil {
					assert.ErrorAs(t, err, &tc.expectedCustomErr)
				} else {
					assert.ErrorIs(t, err, tc.expectedSentinelErr)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, session)
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestServerStreamer_ReceiveChunks(t *testing.T) {
	testCases := []struct {
		name                string
		setupMocks          func(*serverStreamerMocks)
		session             *streamingSession
		expectErr           bool
		expectedSentinelErr error
		expectedCustomErr   error
		expectedBuffer      []byte
	}{
		{
			name: "success: multiple chunks received and assembled",
			setupMocks: func(mocks *serverStreamerMocks) {
				chunk1 := &proto.ChunkDataStream{SessionId: "test-session", Data: []byte("aaaaa"), Offset: 0, IsFinal: false}
				chunk2 := &proto.ChunkDataStream{SessionId: "test-session", Data: []byte("bbbbb"), Offset: 5, IsFinal: true}

				mocks.stream.On("Recv").Return(chunk1, nil).Once()
				mocks.stream.On("Recv").Return(chunk2, nil).Once()

				// Expected successful acknowledgments
				ack1 := &common.ChunkDataAck{SessionID: "test-session", Success: true, BytesReceived: 5, ReadyForNext: true}
				ack2 := &common.ChunkDataAck{SessionID: "test-session", Success: true, BytesReceived: 10, ReadyForNext: true}

				mocks.stream.On("Send", ack1.ToProto()).Return(nil).Once()
				mocks.stream.On("Send", ack2.ToProto()).Return(nil).Once()
			},
			session: &streamingSession{
				SessionID: common.StreamingSessionID("test-session"),
				ChunkHeader: common.ChunkHeader{
					ID:       "test-chunk",
					Index:    0,
					Size:     10,
					Checksum: chunk.CalculateChecksum([]byte("aaaaabbbbb")),
				},
				offset:          0,
				runningChecksum: sha256.New(),
				Status:          SessionActive,
			},
			expectErr:      false,
			expectedBuffer: []byte("aaaaabbbbb"),
		},
		{
			name: "error: receive error",
			setupMocks: func(mocks *serverStreamerMocks) {
				mocks.stream.On("Recv").Return(nil, errors.New("network error")).Once()
			},
			session: &streamingSession{
				SessionID: common.StreamingSessionID("test-session"),
				ChunkHeader: common.ChunkHeader{
					ID:       "test-chunk",
					Index:    0,
					Size:     10,
					Checksum: chunk.CalculateChecksum([]byte("aaaaabbbbb")),
				},
				offset:          0,
				runningChecksum: sha256.New(),
				Status:          SessionActive,
			},
			expectErr:         true,
			expectedCustomErr: NewStreamReceiveError("test-session", 0, errors.New("network error")),
		},
		{
			name: "error: session write error",
			setupMocks: func(mocks *serverStreamerMocks) {
				chunk := &proto.ChunkDataStream{SessionId: "test-session", Data: []byte("data"), Offset: 0, IsFinal: false}
				mocks.stream.On("Recv").Return(chunk, nil).Once()
			},
			session: &streamingSession{
				SessionID: common.StreamingSessionID("test-session"),
				ChunkHeader: common.ChunkHeader{
					ID:       "test-chunk",
					Index:    0,
					Size:     10,
					Checksum: chunk.CalculateChecksum([]byte("aaaaabbbbb")),
				},
				offset:          1, // cause data to be out of order
				runningChecksum: sha256.New(),
				Status:          SessionActive,
			},
			expectErr:         true,
			expectedCustomErr: NewChunkFrameWriteError("test-session", "test-chunk", 1, errors.New("data out of order")),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverStreamerMocks{
				sessionManager: new(MockStreamingSessionManager),
				stream:         new(testutils.MockBidiStreamServer),
			}
			tc.setupMocks(mocks)

			// Use the session provided in the test case, which is already initialized
			session := tc.session
			session.startStreamingSession()

			streamer := &serverStreamer{
				sessionManager: mocks.sessionManager,
			}

			data, err := streamer.ReceiveChunkFrames(session, mocks.stream)

			if tc.expectErr {
				assert.Error(t, err)
				if tc.expectedCustomErr != nil {
					assert.ErrorAs(t, err, &tc.expectedCustomErr)
				} else {
					assert.ErrorIs(t, err, tc.expectedSentinelErr)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedBuffer, data)
			}

			checksumBytes := session.runningChecksum.Sum(nil)
			computedChecksum := hex.EncodeToString(checksumBytes)

			if tc.expectErr {
				return
			}

			assert.Equal(t, session.offset, len(tc.expectedBuffer))
			assert.Equal(t, computedChecksum, session.ChunkHeader.Checksum)
			assert.ElementsMatch(t, session.buffer.Bytes(), tc.expectedBuffer)
			assert.Equal(t, session.Status, SessionActive)
			assert.Equal(t, session.ChunkHeader.Size, int64(len(tc.expectedBuffer)))
			assert.Equal(t, session.ChunkHeader.Index, 0)
			assert.Equal(t, session.ChunkHeader.ID, "test-chunk")

			mocks.assertExpectations(t)
		})
	}
}

func TestServerStreamer_sendAck(t *testing.T) {
	testCases := []struct {
		name                string
		setupMocks          func(*serverStreamerMocks)
		chunk               common.ChunkDataStream
		bytesReceived       int
		expectErr           bool
		expectedSentinelErr error
		expectedCustomErr   error
	}{
		{
			name: "success: send acknowledgment",
			setupMocks: func(mocks *serverStreamerMocks) {
				ack := &proto.ChunkDataAck{
					SessionId:     "test-session",
					Success:       true,
					BytesReceived: 10,
					ReadyForNext:  true,
				}
				mocks.stream.On("Send", ack).Return(nil).Once()
			},
			chunk: common.ChunkDataStream{
				SessionID: common.StreamingSessionID("test-session"),
				ChunkID:   "test-chunk",
				Data:      []byte("test data"),
				Offset:    0,
				IsFinal:   false,
			},
			bytesReceived: 10,
			expectErr:     false,
		},
		{
			name: "error: send fails",
			setupMocks: func(mocks *serverStreamerMocks) {
				ack := &proto.ChunkDataAck{
					SessionId:     "test-session",
					Success:       true,
					BytesReceived: 10,
					ReadyForNext:  true,
				}
				mocks.stream.On("Send", ack).Return(errors.New("send failed")).Once()
			},
			chunk: common.ChunkDataStream{
				SessionID: common.StreamingSessionID("test-session"),
				ChunkID:   "test-chunk",
				Data:      []byte("test data"),
				Offset:    0,
				IsFinal:   false,
			},
			bytesReceived:     10,
			expectErr:         true,
			expectedCustomErr: NewStreamSendError("test-session", "test-chunk", 0, errors.New("send failed")),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverStreamerMocks{
				sessionManager: new(MockStreamingSessionManager),
				stream:         new(testutils.MockBidiStreamServer),
			}
			tc.setupMocks(mocks)

			streamer := &serverStreamer{
				sessionManager: mocks.sessionManager,
			}

			err := streamer.sendAck(tc.chunk, tc.bytesReceived, mocks.stream)

			if tc.expectErr {
				assert.Error(t, err)
				if tc.expectedCustomErr != nil {
					assert.ErrorAs(t, err, &tc.expectedCustomErr)
				} else {
					assert.ErrorIs(t, err, tc.expectedSentinelErr)
				}
			} else {
				assert.NoError(t, err)
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestServerStreamer_handleFinalChunk(t *testing.T) {
	testCases := []struct {
		name                string
		session             *streamingSession
		expectErr           bool
		expectedSentinelErr error
		expectedCustomErr   error
	}{
		{
			name: "success: finalize session",
			session: &streamingSession{
				SessionID: common.StreamingSessionID("test-session"),
				ChunkHeader: common.ChunkHeader{
					ID:       "test-chunk",
					Index:    0,
					Size:     1024,
					Checksum: chunk.CalculateChecksum([]byte("test data")),
				},
				Status:          SessionActive,
				runningChecksum: sha256.New(),
			},
			expectErr: false,
		},
		{
			name: "error: finalize session fails",
			session: &streamingSession{
				SessionID: common.StreamingSessionID("test-session"),
				ChunkHeader: common.ChunkHeader{
					ID:       "test-chunk",
					Index:    0,
					Size:     1024,
					Checksum: "invalid-checksum",
				},
				Status:          SessionActive,
				runningChecksum: sha256.New(),
			},
			expectErr:           true,
			expectedSentinelErr: ErrChecksumMismatch,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			streamer := &serverStreamer{}

			// Pretend chunk was written chunks have been written to the session
			tc.session.startStreamingSession()
			tc.session.write(common.ChunkDataStream{
				SessionID: common.StreamingSessionID("test-session"),
				ChunkID:   "test-chunk",
				Data:      []byte("test data"),
			})

			err := streamer.handleFinalChunk(tc.session)

			if tc.expectErr {
				assert.Error(t, err)
				if tc.expectedCustomErr != nil {
					assert.ErrorAs(t, err, &tc.expectedCustomErr)
				} else {
					assert.ErrorIs(t, err, tc.expectedSentinelErr)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestServerStreamer_sendFinalAck(t *testing.T) {
	testCases := []struct {
		name                string
		setupMocks          func(*serverStreamerMocks)
		chunk               common.ChunkDataStream
		bytesReceived       int
		expectErr           bool
		expectedSentinelErr error
		expectedCustomErr   error
	}{
		{
			name: "success: send final acknowledgment",
			setupMocks: func(mocks *serverStreamerMocks) {
				mocks.stream.On("Send", mock.AnythingOfType("*proto.ChunkDataAck")).Return(nil).Once()
			},
			chunk: common.ChunkDataStream{
				SessionID: common.StreamingSessionID("test-session"),
				ChunkID:   "test-chunk",
				Data:      []byte("test data"),
				Offset:    0,
				IsFinal:   true,
			},
			bytesReceived: 10,
			expectErr:     false,
		},
		{
			name: "error: send final ack fails",
			setupMocks: func(mocks *serverStreamerMocks) {
				mocks.stream.On("Send", mock.AnythingOfType("*proto.ChunkDataAck")).Return(errors.New("send failed")).Once()
			},
			chunk: common.ChunkDataStream{
				SessionID: common.StreamingSessionID("test-session"),
				ChunkID:   "test-chunk",
				Data:      []byte("test data"),
				Offset:    0,
				IsFinal:   true,
			},
			bytesReceived:     10,
			expectErr:         true,
			expectedCustomErr: NewStreamSendError("test-session", "test-chunk", 0, errors.New("send failed")),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverStreamerMocks{
				sessionManager: new(MockStreamingSessionManager),
				stream:         new(testutils.MockBidiStreamServer),
			}
			tc.setupMocks(mocks)

			streamer := &serverStreamer{
				sessionManager: mocks.sessionManager,
			}

			err := streamer.SendFinalAck(tc.chunk.SessionID, tc.bytesReceived, mocks.stream)

			if tc.expectErr {
				assert.Error(t, err)
				if tc.expectedCustomErr != nil {
					assert.ErrorAs(t, err, &tc.expectedCustomErr)
				} else {
					assert.ErrorIs(t, err, tc.expectedSentinelErr)
				}
			} else {
				assert.NoError(t, err)
			}

			mocks.assertExpectations(t)
		})
	}
}

func TestServerStreamer_SendFinalReplicasAck(t *testing.T) {
	testCases := []struct {
		name                string
		setupMocks          func(*serverStreamerMocks)
		session             *streamingSession
		replicaNodes        []*common.NodeInfo
		expectErr           bool
		expectedSentinelErr error
		expectedCustomErr   error
	}{
		{
			name: "success: send final replicas acknowledgment",
			setupMocks: func(mocks *serverStreamerMocks) {
				mocks.stream.On("Send", mock.AnythingOfType("*proto.ChunkDataAck")).Return(nil).Once()
			},
			session: &streamingSession{
				SessionID: common.StreamingSessionID("test-session"),
				ChunkHeader: common.ChunkHeader{
					ID:       "test-chunk",
					Index:    0,
					Size:     1024,
					Checksum: "test-checksum",
				},
				Status: SessionActive,
			},
			replicaNodes: []*common.NodeInfo{
				{
					ID:     "node1",
					Host:   "localhost",
					Port:   8080,
					Status: common.NodeHealthy,
				},
			},
			expectErr: false,
		},
		{
			name: "error: send final replicas ack fails",
			setupMocks: func(mocks *serverStreamerMocks) {
				mocks.stream.On("Send", mock.AnythingOfType("*proto.ChunkDataAck")).Return(errors.New("send failed")).Once()
			},
			session: &streamingSession{
				SessionID: "test-session",
				ChunkHeader: common.ChunkHeader{
					ID:       "test-chunk",
					Index:    0,
					Size:     1024,
					Checksum: "test-checksum",
				},
				Status: SessionActive,
			},
			replicaNodes:      []*common.NodeInfo{},
			expectErr:         true,
			expectedCustomErr: NewStreamSendError("test-session", "test-chunk", 0, errors.New("send failed")),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mocks := &serverStreamerMocks{
				sessionManager: new(MockStreamingSessionManager),
				stream:         new(testutils.MockBidiStreamServer),
			}
			tc.setupMocks(mocks)

			streamer := &serverStreamer{
				sessionManager: mocks.sessionManager,
			}

			err := streamer.SendFinalReplicasAck(tc.session, tc.replicaNodes, mocks.stream)

			if tc.expectErr {
				assert.Error(t, err)
				if tc.expectedCustomErr != nil {
					assert.ErrorAs(t, err, &tc.expectedCustomErr)
				} else {
					assert.ErrorIs(t, err, tc.expectedSentinelErr)
				}
			} else {
				assert.NoError(t, err)
			}

			mocks.assertExpectations(t)
		})
	}
}
